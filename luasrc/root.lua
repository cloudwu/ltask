local ltask = require "ltask"
local root = require "ltask.root"

local config = ...

local MESSAGE_ERROR <const> = 3

local MESSAGE_SCHEDULE_NEW <const> = 0
local MESSAGE_SCHEDULE_DEL <const> = 1
local MESSAGE_SCHEDULE_HANG <const> = 2

local S = {}

do
	-- root init response to itself
	local function init_receipt(type, msg , sz)
		if type == MESSAGE_ERROR then
			print("Root init error:", ltask.unpack(msg, sz))
		end
	end

	ltask.suspend(0, coroutine.create(init_receipt))
end

local function searchpath(name)
	return assert(package.searchpath(name, config.service_path))
end

local function init_service(address, name, ...)
	root.init_service(address, "@"..searchpath "service")
	ltask.syscall(address, "init", searchpath(name), ...)
end

-- todo: manage services

local SERVICE_N = 0

function S.spawn(name, ...)
	local address = assert(ltask.post_message(0, 0, MESSAGE_SCHEDULE_NEW))
	local ok, err = pcall(init_service, address, name, ...)
	if not ok then
		ltask.post_message(0,address, MESSAGE_SCHEDULE_DEL)
		error(err)
	end
--	print("SERVICE NEW", address)
	SERVICE_N = SERVICE_N + 1
	return address
end

function S.kill(address)
	if ltask.post_message(0, address, MESSAGE_SCHEDULE_HANG) then
		return true
	end
	return false
end

ltask.signal_handler(function(from)
--	print("SERVICE DELETE", from)
	SERVICE_N = SERVICE_N - 1
	root.close_service(from)
	ltask.post_message(0,from, MESSAGE_SCHEDULE_DEL)

	if SERVICE_N == 0 then
		-- Only root alive
		for id = 2, 1 + #config.exclusive do
			ltask.send(id, "QUIT")
		end
		ltask.quit()
	end
end)

local function boot()
	S.spawn(table.unpack(config.bootstrap))
end

ltask.dispatch(S)

boot()
