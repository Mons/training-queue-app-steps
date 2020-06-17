#!/usr/bin/env tarantool

local netbox = require 'net.box'
local conn = netbox.connect('queue:queue@localhost:3301')
local yaml = require 'yaml'

while true do
    local task = conn:call('queue.take',{1})

    if task then
        print("Got task: ",yaml.encode(task))
    else
        print "No more tasks"
    end
end
