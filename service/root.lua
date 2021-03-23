local ltask = require "ltask"
local root = require "ltask.root"

local config = ...

local MESSAGE_ERROR <const> = 3

local MESSAGE_SCHEDULE_NEW <const> = 0
local MESSAGE_SCHEDULE_DEL <const> = 1
local MESSAGE_SCHEDULE_HANG <const> = 2

local S = {}
local MAPPED = {}
local SERVICES = {}
local LOGGER_SERVICE


do
	local function writelog()
		while true do
			local ti, id, msg, sz = ltask.poplog()
			if ti == nil then
				break
			end
			local tsec = ti // 100
			local msec = ti % 100
			local t = table.pack(ltask.unpack_remove(msg, sz))
			local str = {}
			for i = 1, t.n do
				str[#str+1] = tostring(t[i])
			end
			io.write(string.format("[%s.%02d : %08d]\t%s\n", os.date("%c", tsec), msec, id, table.concat(str, "\t")))
		end
	end

	-- root init response to itself
	local function init_receipt(type, session, msg, sz)
		if type == MESSAGE_ERROR then
			ltask.log("Root init error:", ltask.unpack(msg, sz))
			writelog()
			-- todo : quit
		end
	end

	ltask.suspend(0, coroutine.create(init_receipt))
end

local function searchpath(name)
	return assert(package.searchpath(name, config.service_path))
end

local function init_service(address, name, ...)
	root.init_service(address, "@"..searchpath "service")
	ltask.syscall(address, "init", {
		path = config.lua_path,
		cpath = config.lua_cpath,
		filename = searchpath(name),
		args = {...},
	})
end

function S.report_error(addr, session)
	ltask.error(addr, session)
end

function S.spawn(name, ...)
	local address = assert(ltask.post_message(0, 0, MESSAGE_SCHEDULE_NEW))
	SERVICES[address] = true
	local ok, err = pcall(init_service, address, name, ...)
	if not ok then
		ltask.post_message(0,address, MESSAGE_SCHEDULE_DEL)
		SERVICES[address] = nil
		error(err)
	end
	return address
end

function S.kill(address)
	if ltask.post_message(0, address, MESSAGE_SCHEDULE_HANG) then
		return true
	end
	return false
end

function S.register(name)
	local session = ltask.current_session()
	if MAPPED[name] then
		error(("Name `%s` already exists."):format(name))
	end
	MAPPED[name] = session.from
end

function S.query(name)
	return MAPPED[name]
end

local function del_service(from)
	if from == LOGGER_SERVICE then
		LOGGER_SERVICE = nil
	else
		assert(SERVICES[from] ~= nil)
		SERVICES[from] = nil
	end
	local msg = root.close_service(from)
	ltask.post_message(0,from, MESSAGE_SCHEDULE_DEL)
	if msg then
		for i=1, #msg, 2 do
			local addr = msg[i]
			local session = msg[i+1]
			ltask.error(addr,session)
		end
	end
end

local function quit_signal_handler(from)
	assert(from == LOGGER_SERVICE)
	del_service(from)
	ltask.quit()
end

local function signal_handler(from)
	del_service(from)
	if next(SERVICES) == nil then
		ltask.signal_handler(quit_signal_handler)

		local request = ltask.request()
		for id = 2, 1 + #config.exclusive do
			request:add { id, proto = "system", "quit" }
		end
		for req, resp in request:select() do
			if not resp then
				print(string.format("exclusive %d quit error: %s.", req[1], req.error))
			end
		end

		if LOGGER_SERVICE then
			ltask.call(LOGGER_SERVICE, "quit")
		else
			ltask.quit()
		end
	end
end

ltask.signal_handler(signal_handler)

local function boot()
	local request = ltask.request()
	for i, name in ipairs(config.exclusive) do
		local id = i + 1
		MAPPED[name] = id
		request:add { id, proto = "system", "init", {
			path = config.lua_path,
			cpath = config.lua_cpath,
			filename = searchpath(name),
			args = {},
		}}
	end
	for req, resp in request:select() do
		if not resp then
			print(string.format("exclusive %d init error: %s.", req[1], req.error))
		end
	end
	LOGGER_SERVICE = S.spawn(table.unpack(config.logger))
	SERVICES[LOGGER_SERVICE] = nil
	S.spawn(table.unpack(config.bootstrap))
end

ltask.dispatch(S)

boot()
