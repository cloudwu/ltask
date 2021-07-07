local SERVICE_ROOT <const> = 1

local MESSAGE_SYSTEM <const> = 0
local MESSAGE_REQUEST <const> = 1
local MESSAGE_RESPONSE <const> = 2
local MESSAGE_ERROR <const> = 3
local MESSAGE_SIGNAL <const> = 4

local RECEIPT_DONE <const> = 1
local RECEIPT_ERROR <const> = 2
local RECEIPT_BLOCK <const> = 3

local SELECT_PROTO = {
	system = MESSAGE_SYSTEM,
	request = MESSAGE_REQUEST,
}

local ltask = require "ltask"

local yield_service = coroutine.yield
local yield_session = coroutine.yield
local function continue_session()
	coroutine.yield(true)
end

----- send message ------
local post_message_ ; do
	function post_message_(to, session, type, msg, sz)
		ltask.send_message(to, session, type, msg, sz)
		continue_session()
		local type, msg, sz = ltask.message_receipt()
		if type == RECEIPT_DONE then
			return true
		elseif type == RECEIPT_ERROR then
			ltask.remove(msg, sz)
			return false
		elseif type == RECEIPT_BLOCK then
			-- todo: send again
			ltask.remove(msg, sz)
			return nil
		else
			-- RECEIPT_RESPONSE(4) (msg is session)
			return msg
		end
	end
end

function ltask.post_message(to, ...)
	local r = post_message_(to, ...)
	if r == nil then
		error(string.format("%x is busy", to))
	end
	return r
end

local running_thread

local session_coroutine_suspend_lookup = {}
local session_coroutine_suspend = {}
local session_coroutine_response = {}
local session_coroutine_address = {}
local session_id = 1

local session_waiting = {}
local wakeup_queue = {}
local exclusive_service = false

----- error handling ------

local error_mt = {}
function error_mt:__tostring()
	return table.concat(self, "\n")
end

local traceback; do
	local selfsource <const> = debug.getinfo(1, "S").source
	local function getshortsrc(source)
		local maxlen <const> = 60
		local type = source:byte(1)
		if type == 61 --[['=']] then
			if #source <= maxlen then
				return source:sub(2)
			else
				return source:sub(2, maxlen)
			end
		elseif type == 64 --[['@']] then
			if #source <= maxlen then
				return source:sub(2)
			else
				return '...' .. source:sub(#source - maxlen + 5)
			end
		else
			local nl = source:find '\n'
			local maxlen <const> = maxlen - 15
			if #source < maxlen and nl == nil then
				return ('[string "%s"]'):format(source)
			else
				local n = #source
				if nl ~= nil then
					n = nl - 1
				end
				if n > maxlen then
					n = maxlen
				end
				return ('[string "%s..."]'):format(source:sub(1, n))
			end
		end
	end
	local function findfield(t, f, level)
		if level == 0 or type(t) ~= 'table' then
			return
		end
		for key, value in pairs(t) do
			if type(key) == 'string' and not (level == 2 and key == '_G') then
				if value == f then
					return key
				end
				local res = findfield(value, f, level - 1)
				if res then
					return key .. '.' .. res
				end
			end
		end
	end
	local function pushglobalfuncname(f)
		return findfield(_G, f, 2)
	end
	local function pushfuncname(info)
		local funcname = pushglobalfuncname(info.func)
		if funcname then
			return ("function '%s'"):format(funcname)
		elseif info.namewhat ~= '' then
			return ("%s '%s'"):format(info.namewhat, info.name)
		elseif info.what == 'main' then
			return 'main chunk'
		elseif info.what ~= 'C' then
			return ('function <%s:%d>'):format(getshortsrc(info.source), info.linedefined)
		else
			return '?'
		end
	end
	local function create_traceback(co, level)
		local s = {}
		local depth = level or 0
		while true do
			local info = debug.getinfo(co, depth, "Slntf")
			if not info then
				s[#s] = nil
				break
			end
			if #s > 0 and selfsource == info.source then
				goto continue
			end
			s[#s + 1] = ('\t%s:'):format(getshortsrc(info.source))
			if info.currentline > 0 then
				s[#s + 1] = ('%d:'):format(info.currentline)
			end
			s[#s + 1] = " in "
			s[#s + 1] = pushfuncname(info)
			if info.istailcall then
				s[#s + 1] = '\n\t(...tail calls...)'
			end
			s[#s + 1] = "\n"
			::continue::
			depth = depth + 1
		end
		return table.concat(s)
	end
	local function replacewhere(co, message, level)
		local f, l = message:find ':[-%d]+: '
		if f and l then
			local where_path = message:sub(1, f - 1)
			local where_line = tonumber(message:sub(f + 1, l - 2))
			local where_src = "@"..where_path
			message = message:sub(l + 1)
			local depth = level or 0
			while true do
				local info = debug.getinfo(co, depth, "Sl")
				if not info then
					break
				end
				if info.what ~= 'C' and info.source == where_src and where_line == info.currentline then
					return message, depth
				end
				depth = depth + 1
			end
		end
		return message, level
	end
	function traceback(errobj, co)
		local level
		if type(co) ~= "thread" then
			level = co
			co = running_thread
		end
		if type(errobj) == "string" then
			local message
			message, level = replacewhere(co, errobj, level)
			errobj = {
				message,
				"stack traceback:",
				level = level,
			}
		end
		assert(type(errobj) == "table")
		errobj[#errobj+1] = ("\t( service:%d )"):format(ltask.self())
		errobj[#errobj+1] = create_traceback(co, level or errobj.level)
		setmetatable(errobj, error_mt)
		return errobj
	end
end

local function rethrow_error(level, errobj)
	if type(errobj) == "string" then
		error(errobj, level + 1)
	else
		errobj.level = level + 1
		setmetatable(errobj, error_mt)
		error(errobj)
	end
end

local function report_error(addr, session, errobj)
	ltask.send_message(SERVICE_ROOT, 0, MESSAGE_REQUEST, ltask.pack("report_error", addr, session, errobj))
	continue_session()
	while true do
		local type, msg, sz = ltask.message_receipt()
		if type == RECEIPT_DONE then
			break
		elseif type == RECEIPT_BLOCK then
			-- retry "report_error"
			ltask.sleep(1)
			ltask.send_message(SERVICE_ROOT, 0, MESSAGE_REQUEST, msg, sz)
			continue_session()
		else
			-- error (root quit?)
			ltask.remove(msg, sz)
			break
		end
	end
end

function ltask.error(addr, session, errobj)
	ltask.send_message(addr, session, MESSAGE_ERROR, ltask.pack(errobj))
	if running_thread == nil then
		-- main thread
		yield_service()
	else
		continue_session()
	end
	local type, msg, sz = ltask.message_receipt()
	if type ~= RECEIPT_DONE then
		ltask.remove(msg, sz)
		if type == RECEIPT_BLOCK then
			ltask.timeout(0, function ()
				report_error(addr, session, errobj)
			end)
		end
	end
end

local function resume_session(co, ...)
	running_thread = co
	local ok, errobj = coroutine.resume(co, ...)
	running_thread = nil
	if ok then
		return errobj
	else
		local from = session_coroutine_address[co]
		local session = session_coroutine_response[co]

		-- term session
		session_coroutine_address[co] = nil
		session_coroutine_response[co] = nil

		errobj = traceback(errobj, co)
		if from == nil or from == 0 then
			-- system message
			print(tostring(errobj))
		else
			ltask.error(from, session, errobj)
		end
	end
end

local function wakeup_session(co, ...)
	local cont = resume_session(co, ...)
	while cont do
		yield_service()
		cont = resume_session(co)
	end
end

-- todo: cache thread
local new_thread = coroutine.create

local function new_session(f, from, session)
	local co = new_thread(f)
	session_coroutine_address[co] = from
	session_coroutine_response[co] = session
	return co
end

local SESSION = {}

local function send_response(...)
	local from = session_coroutine_address[running_thread]
	local session = session_coroutine_response[running_thread]
	-- End session
	session_coroutine_address[running_thread] = nil
	session_coroutine_response[running_thread] = nil

	if session and session > 0 then
		ltask.post_message(from, session, MESSAGE_RESPONSE, ltask.pack(...))
	end
end

local function send_error(errobj)
	local from = session_coroutine_address[running_thread]
	local session = session_coroutine_response[running_thread]
	-- End session
	session_coroutine_address[running_thread] = nil
	session_coroutine_response[running_thread] = nil

	if session and session > 0 then
		ltask.error(from, session, errobj)
	end
end

------------- ltask lua api

function ltask.suspend(session, co)
	session_coroutine_suspend_lookup[session] = co
end

function ltask.call(address, ...)
	if not ltask.post_message(address, session_id, MESSAGE_REQUEST, ltask.pack(...)) then
		error(string.format("%x is dead", address))
	end
	session_coroutine_suspend_lookup[session_id] = running_thread
	session_id = session_id + 1
	local type, session, msg, sz = yield_session()
	if type == MESSAGE_RESPONSE then
		return ltask.unpack_remove(msg, sz)
	else
		-- type == MESSAGE_ERROR
		rethrow_error(2, ltask.unpack_remove(msg, sz))
	end
end

local ignore_response ; do
	local function no_response_()
		while true do
			local type, session, msg, sz = yield_session()
			ltask.remove(msg, sz)
		end
	end

	local no_response_handler = coroutine.create(no_response_)
	coroutine.resume(no_response_handler)

	function ignore_response(session_id)
		session_coroutine_suspend_lookup[session_id] = no_response_handler
	end
end

function ltask.send(address, ...)
	local r = ltask.post_message(address, session_id, MESSAGE_REQUEST, ltask.pack(...))
	if r then
		ignore_response(session_id)
		session_id = session_id + 1
	end
	return r
end

function ltask.syscall(address, ...)
	if not ltask.post_message(address, session_id, MESSAGE_SYSTEM, ltask.pack(...)) then
		error(string.format("%x is dead", address))
	end
	session_coroutine_suspend_lookup[session_id] = running_thread
	session_id = session_id + 1
	local type, session,  msg, sz = yield_session()
	if type == MESSAGE_RESPONSE then
		return ltask.unpack_remove(msg, sz)
	else
		-- type == MESSAGE_ERROR
		rethrow_error(2, ltask.unpack_remove(msg, sz))
	end
end

function ltask.sleep(ti)
	session_coroutine_suspend_lookup[session_id] = running_thread
	if ti == 0 then
		ltask.post_message(ltask.self(), session_id, MESSAGE_RESPONSE)
	else
		ltask.timer_add(session_id, ti)
	end
	session_id = session_id + 1
	yield_session()
end

function ltask.timeout(ti, func)
	local co = new_thread(func)
	session_coroutine_suspend_lookup[session_id] = co
	if ti == 0 then
		ltask.post_message(ltask.self(), session_id, MESSAGE_RESPONSE)
	else
		ltask.timer_add(session_id, ti)
	end
	session_id = session_id + 1
end

local function wait_interrupt(errobj)
	rethrow_error(3, errobj)
end

local function wait_response(type, ...)
	if type == MESSAGE_RESPONSE then
		return ...
	else -- type == MESSAGE_ERROR
		wait_interrupt(...)
	end
end

function ltask.wait(token)
	token = token or running_thread
	session_waiting[token] = running_thread
	session_coroutine_suspend_lookup[session_id] = running_thread
	session_id = session_id + 1
	return wait_response(yield_session())
end

function ltask.wakeup(token, ...)
	local co = session_waiting[token]
	if co then
		wakeup_queue[#wakeup_queue+1] = {co, MESSAGE_RESPONSE, ...}
		session_waiting[token] = nil
		return true
	end
end

function ltask.interrupt(token, errobj)
	local co = session_waiting[token]
	if co then
		errobj = traceback(errobj, 4)
		wakeup_queue[#wakeup_queue+1] = {co, MESSAGE_ERROR, errobj}
		session_waiting[token] = nil
		return true
	end
end

function ltask.fork(func, ...)
	local co = new_thread(func)
	wakeup_queue[#wakeup_queue+1] = {co, ...}
end

function ltask.current_session()
	local from = session_coroutine_address[running_thread]
	local session = session_coroutine_response[running_thread]
	return { from = from, session = session }
end

function ltask.no_response()
	session_coroutine_response[running_thread] = nil
end

function ltask.log(...)
	ltask.pushlog(ltask.pack(...))
end

function ltask.spawn(name, ...)
    return ltask.call(SERVICE_ROOT, "spawn", name, ...)
end

function ltask.kill(addr)
    return ltask.call(SERVICE_ROOT, "kill", addr)
end

function ltask.register(name)
    return ltask.call(SERVICE_ROOT, "register", name)
end

function ltask.queryservice(name)
    return ltask.call(SERVICE_ROOT, "queryservice", name)
end

function ltask.uniqueservice(name, ...)
    return ltask.call(SERVICE_ROOT, "uniqueservice", name, ...)
end

do ------ request/select
	local function request_thread(self)
		local co = new_thread(function()
			while true do
				local type, session, msg, sz = yield_session()
				if session == self._timeout then
					self.timeout = true
					self._timeout = nil
				elseif type == MESSAGE_RESPONSE then
					self._resp[session] = { ltask.unpack_remove(msg, sz) }
				else
					self._request = self._request - 1
					local req = self._sessions[session]
					req.error = (ltask.unpack_remove(msg, sz))
					local err = self._error
					if not err then
						err = {}
						self._error = err
					end
					err[#err+1] =req
				end
				ltask.wakeup(self)
			end
		end)
		coroutine.resume(co)
		return co
	end

	local function send_requests(self, timeout)
		local sessions = {}
		self._sessions = sessions
		self._resp = {}
		local request_n = 0
		local err
		local req_thread = request_thread(self)
		for i = 1, #self do
			local req = self[i]
			-- send request
			local address = req[1]
			local proto = req.proto and assert(SELECT_PROTO[req.proto]) or MESSAGE_REQUEST
			local session = session_id
			session_coroutine_suspend_lookup[session] = req_thread
			session_id = session_id + 1

			if not ltask.post_message(address, session, proto, ltask.pack(table.unpack(req, 2))) then
				err = err or {}
				req.error = true	-- address is dead
				err[#err+1] = req
			else
				sessions[session] = req
				request_n = request_n + 1
			end
		end
		if timeout then
			session_coroutine_suspend_lookup[session_id] = req_thread
			ltask.timer_add(session_id, timeout)
			self._timeout = session_id
			session_id = session_id + 1
		end
		self._request = request_n
		self._error = err
	end

	local function ignore_timout(self)
		if self._timeout then
			ignore_response(self._timeout)
			self._timeout = nil
		end
	end

	local function pop_error(self)
		local req = table.remove(self._error)
		if req then
			req.error = tostring(traceback(req.error, 4))
			return req
		end
	end

	local function request_iter(self)
		local timeout_session = self._timeout
		return function()
			if self._error and #self._error > 0 then
				return pop_error(self)
			end

			local session, resp = next(self._resp)
			if session == nil then
				if self._request == 0 then
					return
				end
				if self.timeout then
					return
				end
				ltask.wait(self)
				if self.timeout then
					return
				end
				session, resp = next(self._resp)
				if session == nil then
					return pop_error(self)
				end
			end

			self._request = self._request - 1
			local req = self._sessions[session]
			self._resp[session] = nil
			self._sessions[session] = nil
			return req, resp
		end
	end

	local request_meta = {}	; request_meta.__index = request_meta

	function request_meta:add(obj)
		assert(self._request == nil)	-- select starts
		self[#self+1] = obj
		return self
	end

	request_meta.__call = request_meta.add

	function request_meta:close()
		if self._request > 0 then
			for session, req in pairs(self._sessions) do
				if not self._resp[session] then
					ignore_response(session)
				end
			end
			self._request = 0
		end
		ignore_timout(self)
	end

	request_meta.__close = request_meta.close

	function request_meta:select(timeout)
		send_requests(self, timeout)
		return request_iter(self), nil, nil, self
	end

	function ltask.request(obj)
		local ret = setmetatable({}, request_meta)
		if obj then
			return ret(obj)
		end
		return ret
	end
end

-------------

local quit

function ltask.quit()
	if not exclusive_service then
		ltask.fork(function ()
			for co, addr in pairs(session_coroutine_address) do
				local session = session_coroutine_response[co]
				ltask.error(addr, session, "Service has been quit.")
			end
			quit = true
		end)
	end
end

local service
local sys_service = {}

function ltask.dispatch(handler)
	service = handler
end

function ltask.signal_handler(f)	-- root only
	SESSION[MESSAGE_SIGNAL] = function (type, msg, sz)
		local from = session_coroutine_address[running_thread]
		local session = session_coroutine_response[running_thread]
		f(from, session)
	end
end

local yieldable_require; do
	local require = _G.require
	local loaded = package.loaded
	local loading = {}
	local function findloader(name)
		local msg = ''
		local searchers = package.searchers
		assert(type(searchers) == "table", "'package.searchers' must be a table")
		for _, searcher in ipairs(searchers) do
			local f, extra = searcher(name)
			if type(f) == 'function' then
				return f, extra
			elseif type(f) == 'string' then
				msg = msg .. "\n\t" .. f
			end
		end
		error(("module '%s' not found:%s"):format(name, msg))
	end
	function yieldable_require(name)
		local m = loaded[name]
		if m ~= nil then
			return m
		end
		local _, main = coroutine.running()
		if main then
			return require(name)
		end
		local initfunc, extra = findloader(name)
		if loading[name] then
			error(("Another coroutine is loading `%s`."):format(name))
		end
		loading[name] = true
		local r = initfunc(name, extra)
		loading[name] = nil
		if r == nil then
			r = true
		end
		loaded[name] = r
		return r
	end
end

local function init_exclusive()
	exclusive_service = true
	local exclusive = require "ltask.exclusive"
	local blocked_message
	local retry_blocked_message
	local function post_message(address, session, type, msg, sz)
		if not exclusive.send(address, session, type, msg, sz) then
			if blocked_message then
				local n = #blocked_message
				blocked_message[n+1] = address
				blocked_message[n+2] = session
				blocked_message[n+3] = type
				blocked_message[n+4] = msg
				blocked_message[n+5] = sz
			else
				blocked_message = { address, session, type, msg, sz }
				ltask.fork(retry_blocked_message)
			end
		end
	end
	function retry_blocked_message()
		ltask.sleep(0)
		if not blocked_message then
			return
		end
		local blocked = blocked_message
		blocked_message = nil
		for i = 1, #blocked, 5 do
			local address = blocked[i]
			local session = blocked[i+1]
			local type    = blocked[i+2]
			local msg     = blocked[i+3]
			local sz      = blocked[i+4]
			post_message(address, session, type, msg, sz)
		end
	end
	function ltask.post_message(address, session, type, msg, sz)
		post_message(address, session, type, msg, sz)
		return true
	end
	function ltask.error(address, session, errobj)
		post_message(address, session, MESSAGE_ERROR, ltask.pack(errobj))
	end
end

function sys_service.init(t)
	-- The first system message
	assert(service == nil)
	if t.lua_path then
		package.path = t.lua_path
	end
	if t.lua_cpath then
		package.cpath = t.lua_cpath
	end
	local _require = _G.require
	local filename = assert(package.searchpath(t.name, t.service_path))
	local f = assert(loadfile(filename))
	_G.require = yieldable_require
	if t.exclusive then
		init_exclusive()
	end
	local r = f(table.unpack(t.args))
	_G.require = _require
	if service == nil then
		service = r
	end
	if service == nil then
		ltask.quit()
	end
end

function sys_service.quit()
	if service and service.quit then
		return service.quit()
	end
end

local function system(command, ...)
	local s = sys_service[command]
	if not s then
		send_error("Unknown system message : " .. command)
		return
	end
	send_response(s(...))
end

SESSION[MESSAGE_SYSTEM] = function (type, msg, sz)
	system(ltask.unpack_remove(msg, sz))
end

local function request(command, ...)
	local s = service[command]
	if not s then
		send_error("Unknown request message : " .. command)
		return
	end
	send_response(s(...))
end

SESSION[MESSAGE_REQUEST] = function (type, msg, sz)
	request(ltask.unpack_remove(msg, sz))
end

local function dispatch_wakeup()
	while #wakeup_queue > 0 do
		local s = table.remove(wakeup_queue, 1)
		wakeup_session(table.unpack(s))
	end
end

local SCHEDULE_IDLE <const> = 1
local SCHEDULE_QUIT <const> = 2
local SCHEDULE_SUCCESS <const> = 3

function ltask.schedule_message()
	local from, session, type, msg, sz = ltask.recv_message()
	local f = SESSION[type]
	if f then
		-- new session for this message
		local co = new_session(f, from, session)
		wakeup_session(co, type, msg, sz)
	elseif session then
		local co = session_coroutine_suspend_lookup[session]
		if co == nil then
			print("Unknown response session : ", session)
			ltask.remove(msg, sz)
			ltask.quit()
		else
			session_coroutine_suspend_lookup[session] = nil
			wakeup_session(co, type, session, msg, sz)
		end
	else
		return SCHEDULE_IDLE
	end
	dispatch_wakeup()
	if quit then
		return SCHEDULE_QUIT
	end
	return SCHEDULE_SUCCESS
end

print = ltask.log

while true do
	local s = ltask.schedule_message()
	if s == SCHEDULE_QUIT then
		break
	end
	yield_service()
end
