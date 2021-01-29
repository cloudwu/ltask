local ltask = require "ltask"
local root = require "ltask.root"

local config = ...

local S = {}

do
	-- root init response to itself
	local function init_receipt(type, msg , sz)
		if type == config.MESSAGE_ERROR then
			print("Root init error:", ltask.unpack(msg, sz))
		end
	end

	ltask.suspend(0, coroutine.create(init_receipt))
end

local function init_service(address, name, ...)
	root.init_service(address, config.service)
	ltask.syscall(address, "init", config.service_path .. name..".lua", ...)
end

-- todo: mange services
function S.spawn(name, ...)
	local address = assert(ltask.post_message(0, 0, config.MESSAGE_SCHEDULE_NEW))
	local ok, err = pcall(init_service, address, name, ...)
	if not ok then
		ltask.post_message(0,address,config.MESSAGE_SCHEDULE_DEL)
		error(err)
	end
	return address
end

function S.kill(address)
	if ltask.post_message(0, address, config.MESSAGE_SCHEDULE_HANG) then
		-- address must not in schedule
		root.close_service(address)
		ltask.post_message(0,address,config.MESSAGE_SCHEDULE_DEL)
		return true
	end
	return false
end

local function boot()
	print "Root init"
	print(os.date("%c", (ltask.now())))
	local addr = S.spawn("user", "Hello")
	print(ltask.call(addr, "ping", "PONG"))
	print(ltask.send(addr, "ping", "SEND"))
	ltask.call(addr, "exit")
	print(ltask.send(addr, "ping", "SEND"))
end

ltask.dispatch(S)

boot()
