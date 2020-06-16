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

local queue = {}

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
    return box.space.queue:insert{ id, STATUS.READY, { ... } }
end

function queue.take(...)

end

return queue
