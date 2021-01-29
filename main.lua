local boot = require "ltask.bootstrap"
local ltask = require "ltask"

local SERVICE_ROOT <const> = 1
local SERVICE_TIMER <const> = 2

local MESSSAGE_SYSTEM <const> = 0
local MESSAGE_REQUEST <const> = 1
local MESSAGE_RESPONSE <const> = 2
local MESSAGE_ERROR <const> = 3

local config = boot.init {}
config.service = "luasrc/service.lua"
config.service_path = "luasrc/"

config.SERVICE_ROOT = SERVICE_ROOT

config.MESSSAGE_SYSTEM = MESSSAGE_SYSTEM
config.MESSAGE_REQUEST = MESSAGE_REQUEST
config.MESSAGE_RESPONSE = MESSAGE_RESPONSE
config.MESSAGE_ERROR = MESSAGE_ERROR

config.MESSAGE_SCHEDULE_NEW = 0
config.MESSAGE_SCHEDULE_DEL = 1
config.MESSAGE_SCHEDULE_HANG = 2

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

local function exclusive_thread( name, id )
	local sid = boot.new_service("@luasrc/" .. name .. ".lua", id)
	assert(sid == id)
	boot.new_thread(sid)
end

bootstrap()	-- launch root

boot.init_timer()
exclusive_thread ("timer", SERVICE_TIMER)

print "ltask Start"
boot.run()
