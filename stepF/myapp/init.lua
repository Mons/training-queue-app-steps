#!/usr/bin/env tarantool 
require'strict'.on()

fiber = require('fiber');
clock = require('clock');


local fio = require('fio');
local under_tarantoolctl = fiber.name() == 'tarantoolctl'

local ffi = require 'ffi'
if not pcall(function() return ffi.C.isatty end) then
    ffi.cdef[[ int isatty(int fd); ]]
end
function isatty(fd)
    fd = fd or 0
    return ffi.C.isatty(fd) ~= 0
end

local source = fio.abspath(debug.getinfo(1,"S").source:match('^@(.+)'))
local symlink = fio.readlink(source);
if not symlink then error("Please run by symlink",0) end
local instance_name = source:match("/([^/]+)$"):gsub('%.lua$','')

local data_dir = 'data/'..instance_name
if os.getenv('TARANTOOL_WORKDIR') then
    data_dir = os.getenv('TARANTOOL_WORKDIR')
end

local config = {
    pid_file   = data_dir..".pid",
    wal_dir    = data_dir,
    memtx_dir  = data_dir,
    vinyl_dir  = data_dir,
}

local yaml = require 'yaml'
local config_file = os.getenv('CONFIG') or "etc/"..instance_name..'.yaml'

local f,e = fio.open(config_file)
if not f then error("Failed to open "..config_file..": "..e,0) end
local data = f:read()
f:close()
local ok, cfg_data = pcall(yaml.decode,data)
if not ok then error("Failed to read "..config_file..": "..cfg_data,0) end

for k,v in pairs(cfg_data or {}) do
    config[k] = v
end

if os.getenv('TARANTOOL_LISTEN') then
    config.listen = os.getenv('TARANTOOL_LISTEN')
end

box.cfg(config)

require 'package.reload'

queue = require 'queue'

if not under_tarantoolctl and not fiber.self().storage.console and isatty() then
    require'console'.start()
    os.exit()
end
