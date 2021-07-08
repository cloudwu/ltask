local ltask = require "ltask"
local root = require "ltask.root"

local config = ...

local MESSAGE_ERROR <const> = 3

local MESSAGE_SCHEDULE_NEW <const> = 0
local MESSAGE_SCHEDULE_DEL <const> = 1
local MESSAGE_SCHEDULE_HANG <const> = 2

local S = {}

local anonymous_services = {}
local named_services = {}
local labels = {
	[0] = "system",
	[1] = "root",
}

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
		io.write(string.format("[%s.%02d : %-10s]\t%s\n", os.date("%c", tsec), msec, labels[id], table.concat(str, "\t")))
	end
end

do
	-- root init response to itself
	local function init_receipt(type, session, msg, sz)
		if type == MESSAGE_ERROR then
			local errobj = ltask.unpack_remove(msg, sz)
			ltask.log("Root init error:", table.concat(errobj, "\n"))
			writelog()
			ltask.quit()
		end
	end

	ltask.suspend(0, coroutine.create(init_receipt))
end

local tokenmap = {}
local function multi_wait(key)
	local mtoken = tokenmap[key]
	if not mtoken then
		mtoken = {}
		tokenmap[key] = mtoken
	end
	local t = {}
	mtoken[#mtoken+1] = t
	return ltask.wait(t)
end

local function multi_wakeup(key, ...)
	local mtoken = tokenmap[key]
	if mtoken then
		tokenmap[key] = nil
		for _, token in ipairs(mtoken) do
			ltask.wakeup(token, ...)
		end
	end
end

local function multi_interrupt(key, ...)
	local mtoken = tokenmap[key]
	if mtoken then
		tokenmap[key] = nil
		for _, token in ipairs(mtoken) do
			ltask.interrupt(token, ...)
		end
	end
end

local function init_service(address, name, ...)
	root.init_service(address, name, config.init_service)
	ltask.syscall(address, "init", {
		lua_path = config.lua_path,
		lua_cpath = config.lua_cpath,
		service_path = config.service_path,
		name = name,
		args = {...},
	})
end

local function new_service(name, ...)
	local address = assert(ltask.post_message(0, 0, MESSAGE_SCHEDULE_NEW))
	anonymous_services[address] = true
	labels[address] = name
	local ok, err = pcall(init_service, address, name, ...)
	if not ok then
		S.kill(address)
		return nil, err
	end
	return address
end

local function register_service(address, name)
	if named_services[name] then
		error(("Name `%s` already exists."):format(name))
	end
	anonymous_services[address] = nil
	named_services[#named_services+1] = name
	named_services[name] = address
	multi_wakeup("query."..name, address)
end

function S.report_error(addr, session, errobj)
	ltask.error(addr, session, errobj)
end

function S.spawn(name, ...)
	local address, err = new_service(name, ...)
	if not address then
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

function S.label(address)
	return labels[address]
end

function S.register(name)
	local session = ltask.current_session()
	register_service(session.from, name)
end

function S.queryservice(name)
	local address = named_services[name]
	if address then
		return address
	end
	return multi_wait("query."..name)
end

function S.uniqueservice(name, ...)
	local address = named_services[name]
	if address then
		return address
	end
	local key = "unique."..name
	if not tokenmap[key] then
		ltask.fork(function (...)
			local addr, err = new_service(name, ...)
			if not addr then
				multi_interrupt(key, err)
			else
				register_service(addr, name)
				multi_wakeup(key, addr)
			end
		end, ...)
	end
	return multi_wait(key)
end

local function del_service(address)
	labels[address] = nil
	if anonymous_services[address] then
		anonymous_services[address] = nil
	else
		for _, name in ipairs(named_services) do
			if named_services[name] == address then
				break
			end
		end
	end
	local msg = root.close_service(address)
	ltask.post_message(0,address, MESSAGE_SCHEDULE_DEL)
	if msg then
		for i=1, #msg, 2 do
			local addr = msg[i]
			local session = msg[i+1]
			ltask.error(addr, session, "Service has been quit.")
		end
	end
end

local function quit()
	if next(anonymous_services) ~= nil then
		return
	end
	ltask.signal_handler(del_service)
	for i = #named_services, 1, -1 do
		local name = named_services[i]
		local address = named_services[name]
		local ok, err = pcall(ltask.syscall, address, "quit")
		if not ok then
			print(string.format("named service %s(%d) quit error: %s.", name, address, err))
		end
	end
	writelog()
	ltask.quit()
end

local function signal_handler(from)
	del_service(from)
	quit()
end

ltask.signal_handler(signal_handler)

local function boot()
	local request = ltask.request()
	for i, t in ipairs(config.exclusive) do
		local name, args
		if type(t) == "table" then
			name = table.remove(t, 1)
			args = t
		else
			name = t
			args = {}
		end
		local id = i + 1
		labels[id] = name
		register_service(id, name)
		request:add { id, proto = "system", "init", {
			lua_path = config.lua_path,
			lua_cpath = config.lua_cpath,
			service_path = config.service_path,
			name = name,
			args = args,
			exclusive = true,
		}}
	end
	for req, resp in request:select() do
		if not resp then
			print(string.format("exclusive %d init error: %s", req[1], req.error))
			return
		end
	end
	S.uniqueservice(table.unpack(config.logger))
	S.spawn(table.unpack(config.bootstrap))
end

ltask.dispatch(S)

boot()
quit()
