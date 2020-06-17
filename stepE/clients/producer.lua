#!/usr/bin/env tarantool

if #arg < 1 then
    error("Need arguments",0)
end

local netbox = require 'net.box'
local yaml = require 'yaml'
local fiber = require 'fiber'
local clock = require 'clock'
local fio = require 'fio'

math.randomseed(tonumber(clock.time64()/1e6))
local peers = { 'queue:queue@localhost:3301','queue:queue@localhost:3302' }
table.sort(peers, function() return math.random(2) == 1 end)

local data = {unpack(arg)}

for _,peer in pairs(peers) do
    local r,e = pcall(function()
        local conn = netbox.connect(peer)

        local res = conn:call('queue.put',{data})

        conn:close()

        print(yaml.encode(res))

        if not res then
            error("Failed to put")
        end
    end)
    if r then
        os.exit()
    else
        print(peer .. ": " .. e)
    end
end
error("Failed to deliver message", 0)
