local boot = require "ltask.bootstrap"

local SERVICE_ROOT <const> = 1
local MESSSAGE_SYSTEM <const> = 0

local config

local function searchpath(name)
	return assert(package.searchpath(name, config.service_path))
end

local function readall(path)
	local f <close> = assert(io.open(path))
	return f:read "a"
end

local function init_config()
	local servicepath = searchpath "service"
	config.service_source = config.service_source or readall(servicepath)
	config.service_chunkname = config.service_chunkname or ("@" .. servicepath)
	config.initfunc = ([=[
local name = ...
package.path = [[${lua_path}]]
package.cpath = [[${lua_cpath}]]
local filename, err = package.searchpath(name, "${service_path}")
if not filename then
	return nil, err
end
return loadfile(filename)
]=]):gsub("%$%{([^}]*)%}", {
	lua_path = config.lua_path or package.path,
	lua_cpath = config.lua_cpath or package.cpath,
	service_path = config.service_path,
})
end

local function new_service(label, id)
	local sid = assert(boot.new_service(label, config.service_source, config.service_chunkname, id))
	assert(sid == id)
	return sid
end

local function bootstrap()
	new_service("root", SERVICE_ROOT)
	boot.init_root(SERVICE_ROOT)
	-- send init message to root service
	local init_msg, sz = boot.pack("init", {
		initfunc = config.initfunc,
		name = "root",
		args = {config}
	})
	-- self bootstrap
	boot.post_message {
		from = SERVICE_ROOT,
		to = SERVICE_ROOT,
		session = 1,	-- 1 for root init
		type = MESSSAGE_SYSTEM,
		message = init_msg,
		size = sz,
	}
end

function print(...)
	local t = table.pack(...)
	local str = {}
	for i = 1, t.n do
		str[#str+1] = tostring(t[i])
	end
	local message = string.format("( ltask.bootstrap ) %s", table.concat(str, "\t"))
	boot.pushlog(boot.pack("info", message))
end

local function start(cfg)
	config = cfg
	init_config()
	boot.init(config)
	boot.init_timer()
	boot.init_socket()
	bootstrap()	-- launch root
	print "ltask Start"
	local ctx = boot.run()
	boot.wait(ctx)
	boot.deinit()
end

return start
