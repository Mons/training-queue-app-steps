local t = require('luatest')
local fio = require('fio')
local json = require('json')
local fiber = require('fiber')

local g = t.group('queue')

g.before_all(function()
    g.server = t.Server:new{
        command = './queue_test.lua',
        workdir = 'data/queue_test',
        net_box_port = 3303,
        net_box_credentials = { user = 'queue', password = 'queue' },
    }
    g.server:start()
    t.helpers.retrying({timeout = 10, delay = 0.1}, g.server.connect_net_box, g.server)
    g.server.net_box:call('box.space.queue:truncate', {{"Test message"}})
end)

g.after_all(function()
    g.server:stop()
end)

g.test_queue = function()
    local res

    res = g.server.net_box:call('queue.stats', {})
    t.assert(res, "Stats received")
    t.assert_equals(res.ready, 0, "Ready = 0")
    t.assert_equals(res.taken, 0, "Taken = 0")
    t.assert_equals(res.waiting, 0, "Waiting = 0")
    t.assert_equals(res.total, 0, "Total = 0")
    -- print(json.encode(res))

    res = g.server.net_box:call('queue.put', {{"Test message"}})
    t.assert(res, "Task sent")
    -- print(json.encode(res))

    res = g.server.net_box:call('queue.stats', {})
    t.assert(res, "Stats received")
    t.assert_equals(res.ready, 1, "Ready = 1")
    t.assert_equals(res.taken, 0, "Taken = 0")
    t.assert_equals(res.waiting, 0, "Waiting = 0")
    t.assert_equals(res.total, 1, "Total = 1")
    -- print(json.encode(res))
end
