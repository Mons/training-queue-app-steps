require'strict'.on()

fiber = require('fiber');
local under_tarantoolctl = fiber.name() == 'tarantoolctl'

local fio = require('fio');
local source = fio.abspath(debug.getinfo(1,"S").source:match('^@(.+)'))
local symlink = fio.readlink(source);
if not symlink then error("Please run by symlink",0) end
local instance_name = source:match("/([^/]+)$"):gsub('%.lua$','')

local data_dir = 'data/'..instance_name
box.cfg{
    pid_file   = data_dir..".pid",
    wal_dir    = data_dir,
    memtx_dir  = data_dir,
    vinyl_dir  = data_dir,
    -- log        = data_dir..".log",
}

if not under_tarantoolctl then
    require'console'.start()
    os.exit()
end
