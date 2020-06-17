#!/usr/bin/env tarantool

local netbox = require 'net.box'
local yaml = require 'yaml'
local fiber = require 'fiber'
local clock = require 'clock'
local fio = require 'fio'

local peers = { 'queue:queue@localhost:3301','queue:queue@localhost:3302' }

local i = 0
local last = clock.time()

for _,peer in pairs(peers) do
    fiber.create(function(peer)
        while true do
            local r,e = pcall(function()
                local conn = netbox.connect(peer, {
                    reconnect_after = 1,
                    wait_connected = true,
                })

                while true do
                    local task = conn:call('queue.take', {1})
                    if task then
                        conn:call('queue.ack', {task.id})
                        i = i + 1
                        if i % 5000 == 0 then
                            print("Processed ",i, "in", clock.time() - last)
                            last = clock.time()
                        end
                    else
                        print("No tasks from",peer)
                    end
                end
            end)

            if not r then print(e) end
        end
    end, peer)
end
