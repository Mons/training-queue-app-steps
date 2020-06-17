queue = {}

local log = require 'log'
local fiber = require 'fiber'
local clock = require 'clock'
local ulid = require 'id.ulid':new()

require 'queue.credentials'

queue._wait = fiber.channel(0)

queue.taken = {};
queue.bysid = {};

local STATUS = {}
STATUS.READY = 'R'
STATUS.TAKEN = 'T'
STATUS.WAITING = 'W'

box.schema.create_space('queue',{ if_not_exists = true; })

box.space.queue:format( {
    { name = 'id';     type = 'string' },
    { name = 'status'; type = 'string' },
    { name = 'runat';  type = 'number' },
    { name = 'data';   type = '*'      },
} );

box.space.queue:create_index('primary', {
    parts = { 1, 'string' };
    if_not_exists = true;
})

box.space.queue:create_index('status', {
    parts = { 2, 'string', 1, 'string' };
    if_not_exists = true;
})

box.space.queue:create_index('runat', {
    parts = {3, 'number', 1, 'string'};
    if_not_exists = true;
})

local F = {}
for no,def in pairs(box.space.queue:format()) do
    F[no] = def.name
    F[def.name] = no
end

while true do
    local t = box.space.queue.index.status:pairs({STATUS.TAKEN}):nth(1)
    if not t then break end
    box.space.queue:update({t.id},{{'=', F.status, STATUS.READY }})
    log.info("Autoreleased %s at start", t.id)
end

box.session.on_connect(function()
    log.info( "connected %s from %s", box.session.id(), box.session.peer() )
    box.session.storage.peer = box.session.peer()
end)

box.session.on_auth(function(user, success)
    if success then
        log.info( "auth %s:%s from %s", box.session.id(), user, box.session.peer() )
    else
        log.info( "auth %s failed from %s", user, box.session.storage.peer )
    end
end)

box.session.on_disconnect(function()
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
end)

function queue.put(data, opts)
    local id = ulid:id()

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

function queue.release(id)
    local t = get_task(id)
    if queue._wait:has_readers() then queue._wait:put(true,0) end
    queue.taken[ t.id ] = nil
    queue.bysid[ box.session.id() ][ t.id ] = nil
    return box.space.queue
        :update({t.id},{{'=', F.status, STATUS.READY }})
        :tomap{ names_only=true }
end

return queue