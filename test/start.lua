local boot = require "ltask.bootstrap"
local ltask = require "ltask"

local SERVICE_ROOT <const> = 1
local MESSSAGE_SYSTEM <const> = 0

local config

local function searchpath(name)
	return assert(package.searchpath(name, config.service_path))
end

local function init_config()
	config.lua_path = config.lua_path or package.path
	config.lua_cpath = config.lua_cpath or package.cpath
	config.init_service = config.init_service or ("@" .. searchpath "service")
end

local function new_service(label, id)
	local sid = assert(boot.new_service(label, config.init_service, id))
	assert(sid == id)
	return sid
end

local function bootstrap()
	new_service("root", SERVICE_ROOT)
	boot.init_root(SERVICE_ROOT)
	-- send init message to root service
	local init_msg, sz = ltask.pack("init", {
		lua_path = config.lua_path,
		lua_cpath = config.lua_cpath,
		service_path = config.service_path,
		name = "root",
		args = {config}
	})
	-- self bootstrap
	boot.post_message {
		from = SERVICE_ROOT,
		to = SERVICE_ROOT,
		session = 0,	-- 0 for root init
		type = MESSSAGE_SYSTEM,
		message = init_msg,
		size = sz,
	}
end

local function exclusive_thread(label, id)
	local sid = new_service(label, id)
	boot.new_thread(sid)
end

function print(...)
	boot.pushlog(ltask.pack("info", ...))
end

local function toclose(f)
	return setmetatable({}, {__close=f})
end

-- test exclusive transfer

local function dummy_service(id)
	local dummy = [[
local exclusive = require "ltask.exclusive"

local i = 0
while true do
	local pre = coroutine.yield()
	print ("Dummy Tick", pre, i)
	io.flush()
	exclusive.sleep(1000)
	i = i + 1
end
	]]

	local p = boot.preinit(dummy)
	local label = "Dummy"
	os.execute "sleep 1"

	local sid = assert(boot.new_service_preinit(label, id + 1, p))
	assert(id + 1 == sid)
	boot.new_thread(sid)
end

local function start(cfg)
	config = cfg
	init_config()
	boot.init(config)
	local _ <close> = toclose(boot.deinit)
	boot.init_timer()
	boot.init_socket()

	local id = 0
	for i, label in ipairs(config.exclusive) do
		id = i + 1
		exclusive_thread(label, id)
	end
	-- dummy_service(id)
	bootstrap()	-- launch root
	print "ltask Start"
	boot.run()
end

return start
