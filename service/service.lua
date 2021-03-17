local SERVICE_ROOT <const> = 1

local MESSAGE_SYSTEM <const> = 0
local MESSAGE_REQUEST <const> = 1
local MESSAGE_RESPONSE <const> = 2
local MESSAGE_ERROR <const> = 3
local MESSAGE_SIGNAL <const> = 4

local RECEIPT_DONE <const> = 1
local RECEIPT_ERROR <const> = 2
local RECEIPT_BLOCK <const> = 3

local function dprint(...)
--	print("DEBUG", ...)
end

local ltask = require "ltask"

----- send message ------
local post_message_ ; do
	function post_message_(to, session, type, msg, sz)
		ltask.send_message(to, session, type, msg, sz)
		coroutine.yield(true) -- tell schedule continue
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

local function post_message(to, ...)
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

local yield_service = coroutine.yield

local function resume_session(co, ...)
	running_thread = co
	local ok, msg = coroutine.resume(co, ...)
	if ok then
		return msg
	else
		local from = session_coroutine_address[co]
		local session = session_coroutine_response[co]

		if from == 0 then
			-- system message
			print(debug.traceback(co, msg))
		else
			print(debug.traceback(co, msg))
			ltask.send_message(from, session, MESSAGE_ERROR, ltask.pack(msg))
			yield_service()
			local type, msg, sz = ltask.message_receipt()
			if type == RECEIPT_ERROR then
				ltask.remove(msg, sz)
			elseif type == RECEIPT_BLOCK then
				-- todo: report again
				ltask.remove(msg, sz)
				print(from, "is busy")
			end
		end
	end
end

-- todo: cache thread
local new_thread = coroutine.create

local function new_session(f, type, from, session, msg, sz)
	local co = new_thread(f)
	session_coroutine_address[co] = from
	session_coroutine_response[co] = session
	return resume_session(co, type, msg, sz)
end

local SESSION = {}

local function send_response(...)
	local from = session_coroutine_address[running_thread]
	local session = session_coroutine_response[running_thread]
	if session then
		if not post_message(from, session, MESSAGE_RESPONSE, ltask.pack(...)) then
			print(string.format("Response to absent %x", from))
		end
	end
end

------------- ltask lua api

function ltask.suspend(session, co)
	session_coroutine_suspend_lookup[session] = co
end

function ltask.call(address, ...)
	if not post_message(address, session_id, MESSAGE_REQUEST, ltask.pack(...)) then
		error(string.format("%x is dead", address))
	end
	session_coroutine_suspend_lookup[session_id] = running_thread
	session_id = session_id + 1
	local type, session, msg, sz = coroutine.yield()
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
			local type, session, msg, sz = coroutine.yield()
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
	if not post_message(address, session_id, MESSAGE_SYSTEM, ltask.pack(...)) then
		error(string.format("%x is dead", address))
	end
	session_coroutine_suspend_lookup[session_id] = running_thread
	session_id = session_id + 1
	local type, session,  msg, sz = coroutine.yield()
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
	coroutine.yield()
end

function ltask.timeout(ti, func)
	local co = new_thread(func)
	session_coroutine_suspend_lookup[session_id] = co
	ltask.timer_add(session_id, ti)
	session_id = session_id + 1
end

ltask.post_message = post_message

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
	local function send_requests(self, timeout)
		local sessions = {}
		self._sessions = sessions
		local request_n = 0
		local err
		for i = 1, #self do
			local req = self[i]
			-- send request
			local address = req[1]
			local session = session_id
			session_coroutine_suspend_lookup[session] = running_thread
			session_id = session_id + 1

			if not post_message(address, session, MESSAGE_REQUEST, ltask.pack(table.unpack(req, 2))) then
				err = err or {}
				req.error = true	-- address is dead
				err[#err+1] = req
			else
				sessions[session] = req
				request_n = request_n + 1
			end
		end
		if timeout then
			session_coroutine_suspend_lookup[session_id] = running_thread
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
			if self._error then
				-- invalid address
				local e = table.remove(self._error)
				if e then
					return e
				end
				self._error = nil
			end
			if self._request == 0 then
				-- done, finish
				ignore_timout(self)
				return
			end

			local type, session, msg, sz = coroutine.yield()
			if session == timeout_session then
				-- timeout, finish
				self._timeout = nil
				return
			end
			self._request = self._request - 1
			local req = self._sessions[session]
			if type == MESSAGE_RESPONSE then
				self._sessions[session] = nil
				return req, { ltask.unpack_remove(msg, sz) }
			else -- type == MESSAGE_ERROR
				req.error = (ltask.unpack_remove(msg, sz))	-- todo : multiple error objects
				return req
			end
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
				ignore_response(session)
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
	quit = true
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
		_G.require = require "ltask.require"
		local f = assert(loadfile(t.filename))
		local r = f(table.unpack(t.args))
		_G.require = _require
		if service == nil then
			service = r
		end
	else
		-- todo : other system command
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

while true do
	local from, session, type, msg, sz = ltask.recv_message()
	local f = SESSION[type]
	local cont
	if f then
		-- new session for this message
		cont = new_session(f, type, from, session, msg, sz)
	else
		local co = session_coroutine_suspend_lookup[session]
		if co == nil then
			print("Unknown response session : ", session)
			ltask.remove(msg, sz)
			break
		else
			session_coroutine_suspend_lookup[session] = nil
			cont = resume_session(co, type, session, msg, sz)
		end
	end

	while cont do
		yield_service()
		cont = resume_session(running_thread)
	end

	if quit then
		break
	end
	-- todo : inner thread (fork)
	yield_service()
end
