#!/usr/bin/env tarantool

if #arg < 1 then
    error("Need arguments",0)
end

local netbox = require 'net.box'
local conn = netbox.connect('queue:queue@localhost:3301')

local yaml = require 'yaml'
local res = conn:call('queue.put',{unpack(arg)})
print(yaml.encode(res))
conn:close()
