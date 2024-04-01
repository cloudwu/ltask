local ltask = require "ltask"
local root = require "ltask.root"

local config = ...

local MESSAGE_ERROR <const> = 3

local MESSAGE_SCHEDULE_NEW <const> = 0
local MESSAGE_SCHEDULE_DEL <const> = 1

local S = {}

local anonymous_services = {}
local named_services = {}

local root_quit = ltask.quit
ltask.quit = function() end

local function writelog()
	while true do
		local ti, _, msg, sz = ltask.poplog()
		if ti == nil then
			break
		end
		local tsec = ti // 100
		local msec = ti % 100
		local level, message = ltask.unpack_remove(msg, sz)
		io.write(string.format("[%s.%02d][%-5s]%s\n", os.date("%Y-%m-%d %H:%M:%S", tsec), msec, level:upper(), message))
	end
end

do
	-- root init response to itself
	local function init_receipt(type, session, msg, sz)
		if type == MESSAGE_ERROR then
			local errobj = ltask.unpack_remove(msg, sz)
			ltask.log.error("Root fatal:", table.concat(errobj, "\n"))
			writelog()
			root_quit()
		end
	end

	-- The session of root init message must be 1
	ltask.suspend(1, coroutine.create(init_receipt))
end

local multi_wait = ltask.multi_wait
local multi_wakeup = ltask.multi_wakeup
local multi_interrupt = ltask.multi_interrupt

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

local function spawn(t)
	local address = assert(ltask.post_message(0, 0, MESSAGE_SCHEDULE_NEW))
	anonymous_services[address] = true
	assert(root.init_service(address, t.name, config.service_source, config.service_chunkname, t.worker_id))
	ltask.syscall(address, "init", {
		initfunc = t.initfunc or config.initfunc,
		name = t.name,
		args = t.args or {},
	})
	return address
end

local unique = {}

local function spawn_unique(t)
	local address = named_services[t.name]
	if address then
		return address
	end
	local key = "unique."..t.name
	if not unique[t.name] then
		unique[t.name] = true
		ltask.fork(function ()
			local ok, addr = pcall(spawn, t)
			if not ok then
				local err = addr
				multi_interrupt(key, err)
				unique[t.name] = nil
				return
			end
			register_service(addr, t.name)
			unique[t.name] = nil
		end)
	end
	return multi_wait(key)
end

function S.spawn(name, ...)
	return spawn {
		name = name,
		args = {...},
	}
end

function S.queryservice(name)
	local address = named_services[name]
	if address then
		return address
	end
	return multi_wait("unique."..name)
end

function S.uniqueservice(name, ...)
	return spawn_unique {
		name = name,
		args = {...},
	}
end

function S.spawn_service(t)
	if t.unique then
		return spawn_unique(t)
	else
		return spawn(t)
	end
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
		local err = "Service " .. address .. " has been quit."
		for i=1, #msg, 2 do
			local addr = msg[i]
			local session = msg[i+1]
			ltask.rasie_error(addr, session, err)
		end
	end
end

function S.quit_ltask()
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
	root_quit()
end

local function quit()
	if next(anonymous_services) ~= nil then
		return
	end
	ltask.send(ltask.self(), "quit_ltask")
end

local function signal_handler(from)
	del_service(from)
	quit()
end

ltask.signal_handler(signal_handler)

local function bootstrap()
	for _, t in ipairs(config.bootstrap) do
		S.spawn_service(t)
	end
end

ltask.dispatch(S)

bootstrap()
quit()
