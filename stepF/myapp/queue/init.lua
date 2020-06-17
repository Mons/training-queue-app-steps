local queue = {}

local log   = require 'log'
local json  = require 'json'
local fiber = require 'fiber'
local clock = require 'clock'
require 'queue.credentials'
local schema = require 'queue.schema'

local F = schema.fields

local STATUS = {}
STATUS.READY = 'R'
STATUS.TAKEN = 'T'
STATUS.WAITING = 'W'

local old = rawget(_G,'queue')
if old then
    queue.taken = old.taken;
    queue.bysid = old.bysid;
    queue._wait = old._wait
    queue._stats = old._stats

    queue._triggers = old._triggers

    queue._runch = old._runch
    queue._runat = old._runat

    queue._ulid = old._ulid
else
    queue.taken = {};
    queue.bysid = {};
    queue._wait = fiber.channel(0)
    queue._stats = {}

    queue._triggers = {}

    queue._runch = fiber.cond()

    queue._ulid = require 'id.ulid':new()

    while true do
        local t = box.space.queue.index.status:pairs({STATUS.TAKEN}):nth(1)
        if not t then break end
        box.space.queue:update({t.id},{{'=', F.status, STATUS.READY }})
        log.info("Autoreleased %s at start", t.id)
    end

    for k,v in pairs(STATUS) do
        queue._stats[v] = 0LL
    end

    log.info("Perform initial stat counts")
    for _,t in box.space.queue:pairs() do
        queue._stats[ t[F.status] ] = (queue._stats[ t[F.status] ] or 0LL)+1
    end
    log.info("Initial stats: %s", json.encode( queue._stats ))
end

queue._triggers.on_replace = box.space.queue:on_replace(function(old,new)
    if old then
        queue._stats[ old[ F.status ] ] = queue._stats[ old[ F.status ] ] - 1
    end
    if new then
        queue._stats[ new[ F.status ] ] = queue._stats[ new[ F.status ] ] + 1
    end
end, queue._triggers.on_replace)

queue._triggers.on_truncate = box.space._truncate:on_replace(function(old,new)
    if new.id == box.space.queue.id then
        for k,v in pairs(queue._stats) do
            queue._stats[k] = 0LL
        end
    end
end, queue._triggers.on_truncate)

queue._triggers.on_connect = box.session.on_connect(function()
    log.info( "connected %s from %s", box.session.id(), box.session.peer() )
    box.session.storage.peer = box.session.peer()
end,queue._triggers.on_connect)

queue._triggers.on_disconnect = box.session.on_disconnect(function()
    log.info(
        "disconnected %s:%s from %s", box.session.id(),
        box.session.user(), box.session.storage.peer
    )

    box.session.storage.destroyed = true

    local sid = box.session.id()
    local bysid = queue.bysid[ sid ]
    if bysid then
        while next(bysid) do
            for key in pairs(bysid) do
                log.info("Autorelease %s by disconnect", key);
                queue.taken[key] = nil
                bysid[key] = nil
                local t = box.space.queue:get(key)
                if t then
                    if queue._wait:has_readers() then queue._wait:put(true,0) end
                    box.space.queue:update({t.id},{{'=', F.status, STATUS.READY }})
                end
            end
        end
        queue.bysid[ sid ] = nil
    end
end, queue._triggers.on_disconnect)

queue._runat = fiber.create(function(queue, gen, old_fiber)
    fiber.name('queue.runat.'..gen)

    while package.reload.count == gen and old_fiber and old_fiber:status() ~= 'dead' do
        log.info("Waiting for old to die")
        queue._runch:wait(0.1)
    end

    log.info("Started...")
    while package.reload.count == gen do
        local remaining

        local now = clock.realtime()

        for _,t in box.space.queue.index.runat
            :pairs( {0}, { iterator = 'GT' })
        do
            if t.runat > now then
                remaining = t.runat - now
                break
            else
                if t.status == STATUS.WAITING then
                    log.info("Runat: W->R %s",t.id)
                    if queue._wait:has_readers() then queue._wait:put(true,0) end
                    box.space.queue:update({t.id},{
                        {'=', F.status, STATUS.READY },
                        {'=', F.runat, 0 },
                    })
                else
                    log.error("Runat: bad status %s for %s", t.status, t.id)
                    box.space.queue:update({t.id},{{'=', F.runat, 0 }})
                end
            end
        end

        if not remaining or remaining > 1 then remaining = 1 end
        queue._runch:wait(remaining)
    end

    queue._runch:broadcast()
    log.info("Finished")
end, queue, package.reload.count, queue._runat)

queue._runch:broadcast()

function queue.put(data, opts)
    local id = queue._ulid:id()

    local runat = 0
    local status = STATUS.READY

    if opts and opts.delay then
        runat = clock.realtime() + tonumber(opts.delay)
        status = STATUS.WAITING
    else
        if queue._wait:has_readers() then
            queue._wait:put(true,0)
        end
    end

    return box.space.queue
        :insert{ id, status, runat, data }
        :tomap{ names_only=true }
end

function queue.take(timeout)
    if not timeout then timeout = 0 end
    local now = fiber.time()
    local found
    while not found do
        found = box.space.queue.index.status
            :pairs({STATUS.READY},{ iterator = 'EQ' }):nth(1)
        if not found then
            local left = (now + timeout) - fiber.time()
            if left <= 0 then return end

            queue._wait:get(left)
        end
    end

    if box.session.storage.destroyed then return end

    local sid = box.session.id()
    log.info("Register %s by %s", found.id, sid)

    queue.taken[ found.id ] = sid
    queue.bysid[ sid ] = queue.bysid[ sid ] or {}
    queue.bysid[ sid ][ found.id ] = true

    return box.space.queue
        :update( {found.id}, {{'=', F.status, STATUS.TAKEN }})
        :tomap{ names_only=true }
end

local function get_task(key)
    if not key then error("Task id required", 2) end
    local t = box.space.queue:get{key}
    if not t then
        error(string.format( "Task {%s} was not found", key ), 2)
    end
    if not queue.taken[key] then
        error(string.format( "Task %s not taken by anybody", key ), 2)
    end
    if queue.taken[key] ~= box.session.id() then
        error(string.format( "Task %s taken by %d. Not you (%d)",
            key, queue.taken[key], box.session.id() ), 2)
    end
    return t
end

function queue.ack(id)
    local t = assert(box.space.queue:get{id},"Task not exists")
    if t and t.status == STATUS.TAKEN then
        return box.space.queue
            :delete{t.id}
            :tomap{ names_only=true }
    else
        error("Task not taken")
    end
end

function queue.release(id)
    local t = assert(box.space.queue:get{id},"Task not exists")
    if t and t.status == STATUS.TAKEN then
        if queue.wait:has_readers() then queue.wait:put(true,0) end
        return box.space.queue
            :update({t.id},{{'=', F.status, STATUS.READY }})
            :tomap{ names_only=true }
    else
        error("Task not taken")
    end
end

function queue.ack(id)
    local t = get_task(id)
    queue.taken[ t.id ] = nil
    queue.bysid[ box.session.id() ][ t.id ] = nil
    return box.space.queue
        :delete{t.id}:tomap{ names_only=true }
end

function queue.release(id, opts)
    local t = get_task(id)
    queue.taken[ t.id ] = nil
    queue.bysid[ box.session.id() ][ t.id ] = nil

    local runat = 0
    local status = STATUS.READY

    if opts and opts.delay then
        runat = clock.realtime() + tonumber(opts.delay)
        status = STATUS.WAITING
    else
        if queue._wait:has_readers() then queue._wait:put(true,0) end
    end

    return box.space.queue
        :update({t.id},{{'=', F.status, status },{ '=', F.runat, runat }})
        :tomap{ names_only=true }
end

function queue.stats()
    return {
        total   = box.space.queue:len(),
        ready   = queue._stats[ STATUS.READY ],
        waiting = queue._stats[ STATUS.WAITING ],
        taken   = queue._stats[ STATUS.TAKEN ],
    }
end

return queue