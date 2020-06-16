box.schema.create_space('queue',{ if_not_exists = true; })

box.space.queue:format( {
    { name = 'id';     type = 'number' },
    { name = 'status'; type = 'string' },
    { name = 'data';   type = '*'      },
} );

box.space.queue:create_index('primary', {
   parts = { 1,'number' };
   if_not_exists = true;
})

box.space.queue:create_index('status', {
    parts = { 2, 'string', 1, 'number' };
    if_not_exists = true;
})

local F = {}
for no,def in pairs(box.space.queue:format()) do
    F[no] = def.name
    F[def.name] = no
end

local queue = {}

local fiber = require 'fiber'
queue._wait = fiber.channel()

local STATUS = {}
STATUS.READY = 'R'
STATUS.TAKEN = 'T'

local clock = require 'clock'
function gen_id()
    local new_id
    repeat
        new_id = clock.realtime64()
    until not box.space.queue:get(new_id)
    return new_id
end

function queue.put(...)
    local id = gen_id()

    if queue._wait:has_readers() then
        queue._wait:put(true,0)
    end

    return box.space.queue:insert{ id, STATUS.READY, { ... } }
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
    return box.space.queue:update(
        {found.id},
        {{'=', F.status, STATUS.TAKEN }}
    )
end

function queue.ack(id)
    local t = assert(box.space.queue:get{id},"Task not exists")
    if t and t.status == STATUS.TAKEN then
        return box.space.queue:delete{t.id}
    else
        error("Task not taken")
    end
end

function queue.release(id)
    local t = assert(box.space.queue:get{id},"Task not exists")
    if t and t.status == STATUS.TAKEN then
        if queue._wait:has_readers() then
            queue._wait:put(true,0)
        end
        return box.space.queue:update({t.id},{{'=', F.status, STATUS.READY }})
    else
        error("Task not taken")
    end
end

return queue
