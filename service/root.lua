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
		io.write(string.format("[%s.%02d : %d]\t%s\n", os.date("%c", tsec), msec, id, table.concat(str, "\t")))
	end
end

do
	-- root init response to itself
	local function init_receipt(type, session, msg, sz)
		if type == MESSAGE_ERROR then
			local errobj = ltask.unpack_remove(msg, sz)
			ltask.log.error("Root fatal:", table.concat(errobj, "\n"))
			writelog()
			ltask.quit()
		end
	end

	ltask.suspend(0, coroutine.create(init_receipt))
end

local multi_wait = ltask.multi_wait
local multi_wakeup = ltask.multi_wakeup
local multi_interrupt = ltask.multi_interrupt

local function new_service(name)
	local address = assert(ltask.post_message(0, 0, MESSAGE_SCHEDULE_NEW))
	anonymous_services[address] = true
	local worker_id
	if config.worker_bind then
		worker_id = config.worker_bind[name]
	end
	local ok, err = root.init_service(address, name, config.init_service, worker_id)
	if not ok then
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
	multi_wakeup("unique."..name, address)
end

function S.report_error(addr, session, errobj)
	ltask.error(addr, session, errobj)
end

function S.spawn(name, ...)
	local address = assert(new_service(name))
	ltask.syscall(address, "init", {
		preload = config.preload,
		lua_path = config.lua_path,
		lua_cpath = config.lua_cpath,
		service_path = config.service_path,
		name = name,
		args = {...},
	})
	return address
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
	return multi_wait("unique."..name)
end

local unique = {}

function S.uniqueservice(name, ...)
	local address = named_services[name]
	if address then
		return address
	end
	local key = "unique."..name
	if not unique[name] then
		unique[name] = true
		ltask.fork(function (...)
			local ok, addr = pcall(S.spawn, name, ...)
			if not ok then
				local err = addr
				multi_interrupt(key, err)
				unique[name] = nil
				return
			end
			register_service(addr, name)
			unique[name] = nil
		end, ...)
	end
	return multi_wait(key)
end

local function del_service(address)
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

local function find_exclusive(name)
	for i, label in ipairs(config.exclusive or {}) do
		if label == name then
			return i + 1
		end
	end
end

local function find_preinit(name)
	for i, label in ipairs(config.preinit or {}) do
		if label == name then
			return i + #config.exclusive + 1
		end
	end
end

local function bootstrap()
	local namemap = {}
	local request = ltask.request()
	if config.exclusive then
		for _, label in ipairs(config.exclusive) do
			config.bootstrap[label] = config.bootstrap[label] or {}
		end
	end
	if config.preinit then
		for _, label in ipairs(config.preinit) do
			config.bootstrap[label] = config.bootstrap[label] or {}
		end
	end
	for label, t in pairs(config.bootstrap) do
		local sid = find_exclusive(label)
		if sid then
			namemap[sid] = label
			unique[label] = true
			request:add { sid, proto = "system", "init", {
				preload = config.preload,
				lua_path = config.lua_path,
				lua_cpath = config.lua_cpath,
				service_path = config.service_path,
				name = label,
				args = t.args or {},
			}}
			goto continue
		end
		sid = find_preinit(label)
		if sid then
			namemap[sid] = label
			unique[label] = true
			request:add { sid, proto = "system", "init", {
				lua_path = config.lua_path,
				lua_cpath = config.lua_cpath,
			}}
			goto continue
		end
		sid = assert(new_service(label))
		namemap[sid] = label
		if t.unique ~= false then
			unique[label] = true
		end
		request:add { sid, proto = "system", "init", {
			preload = config.preload,
			lua_path = config.lua_path,
			lua_cpath = config.lua_cpath,
			service_path = config.service_path,
			name = label,
			args = t.args or {},
		}}
		::continue::
	end
	for req, resp in request:select() do
		local sid = req[1]
		local name = namemap[req[1]]
		if unique[name] then
			unique[name] = nil
			if not resp then
				multi_interrupt("unique."..name, req.error)
				print(string.format("service %s(%d) init error: %s", name, sid, req.error))
				return
			end
			register_service(sid, name)
		else
			if not resp then
				print(string.format("service %s(%d) init error: %s", name, sid, req.error))
				return
			end
		end
	end
end

ltask.dispatch(S)

bootstrap()
quit()
