local boot = require "ltask.bootstrap"
local ltask = require "ltask"

local SERVICE_ROOT <const> = 1
local SERVICE_TIMER <const> = 2

local MESSSAGE_SYSTEM <const> = 0

local config = boot.init {
--	worker = 1,
}
config.service = "luasrc/service.lua"
config.service_path = "luasrc/?.lua"
config.bootstrap =  { "bootstrap" }

local function bootstrap()
	assert(boot.new_service("@" .. config.service, SERVICE_ROOT))
	boot.init_root(SERVICE_ROOT)
	-- send init message to root service
	local init_msg, sz = ltask.pack("init", "luasrc/root.lua", config)
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

config.exclusive = {}

local function exclusive_thread( name, id )
	local sid = boot.new_service("@luasrc/" .. name .. ".lua", id)
	assert(sid == id)
	boot.new_thread(sid)
	table.insert(config.exclusive, sid)
end

boot.init_timer()
exclusive_thread ("timer", SERVICE_TIMER)

bootstrap()	-- launch root

print "ltask Start"
boot.run()
