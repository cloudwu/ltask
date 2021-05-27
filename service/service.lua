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


local function dprint(...)
--	print("DEBUG", ...)
end

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
	local ok, msg = coroutine.resume(co, ...)
	running_thread = nil
	if ok then
		return msg
	else
		local from = session_coroutine_address[co]
		local session = session_coroutine_response[co]

		-- term session
		session_coroutine_address[co] = nil
		session_coroutine_response[co] = nil

		if from == nil or from == 0 then
			-- system message
			print(debug.traceback(co, msg))
		else
			print(debug.traceback(co, msg))
			ltask.error(from, session, msg)
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
		error(ltask.unpack_remove(msg, sz))
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
	local r = post_message_(address, session_id, MESSAGE_REQUEST, ltask.pack(...))
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
		error(ltask.unpack_remove(msg, sz))
	end
end

function ltask.sleep(ti)
	session_coroutine_suspend_lookup[session_id] = running_thread
	ltask.timer_add(session_id, ti)
	session_id = session_id + 1
	yield_session()
end

function ltask.timeout(ti, func)
	local co = new_thread(func)
	session_coroutine_suspend_lookup[session_id] = co
	ltask.timer_add(session_id, ti)
	session_id = session_id + 1
end

local function wait_interrupt(errobj)
	error(errobj)
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
		wakeup_queue[#wakeup_queue+1] = {co, MESSAGE_ERROR, errobj}
		session_waiting[token] = nil
		return true
	end
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
					req.error = (ltask.unpack_remove(msg, sz))	-- todo : multiple error objects
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

	local function request_iter(self)
		local timeout_session = self._timeout
		return function()
			if self._error and #self._error > 0 then
				return table.remove(self._error)
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
					return table.remove(self._error)
				end
			end

			self._request = self._request - 1
			local req = self._sessions[session]
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
	ltask.timeout(0, function ()
		for co, addr in pairs(session_coroutine_address) do
			local session = session_coroutine_response[co]
			ltask.error(addr, session, "Service has been quit.")
		end
		quit = true
	end)
end

local service

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

local function system(command, t)
	if service == nil then
		-- The first system message
		assert(command == "init")
		if t.path then
			package.path = t.path
		end
		if t.cpath then
			package.cpath = t.cpath
		end
		local _require = _G.require
		local f = assert(loadfile(t.filename))
		_G.require = require "ltask.require"
		if t.exclusive then
			require "ltask.init_exclusive"
		end
		local r = f(table.unpack(t.args))
		_G.require = _require
		if service == nil then
			service = r
		end
	else
		assert(command == "quit")
		if service.quit then
			return service.quit()
		end
	end
end

SESSION[MESSAGE_SYSTEM] = function (type, msg, sz)
	send_response(system(ltask.unpack_remove(msg, sz)))
	if service == nil then
		quit = true
	end
end

local function request(command, ...)
	return service[command](...)
end

SESSION[MESSAGE_REQUEST] = function (type, msg, sz)
	send_response(request(ltask.unpack_remove(msg, sz)))
end

print = ltask.log

while true do
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
			break
		else
			session_coroutine_suspend_lookup[session] = nil
			wakeup_session(co, type, session, msg, sz)
		end
	elseif ltask.exclusive_idle then
		ltask.exclusive_idle()
	end

	while #wakeup_queue > 0 do
		local s = table.remove(wakeup_queue, 1)
		wakeup_session(table.unpack(s))
	end

	if quit then
		break
	end
	-- todo : inner thread (fork)
	yield_service()
end
