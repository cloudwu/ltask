local SERVICE_ROOT <const> = 1
local SERVICE_TIMER <const> = 2

local MESSAGE_SYSTEM <const> = 0
local MESSAGE_REQUEST <const> = 1
local MESSAGE_RESPONSE <const> = 2
local MESSAGE_ERROR <const> = 3

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
	local type, msg, sz = coroutine.yield()
	if type == MESSAGE_RESPONSE then
		return ltask.unpack_remove(msg, sz)
	else
		-- type == MESSAGE_ERROR
		error(ltask.unpack_remove(msg, sz))
	end
end

local no_response_handler = coroutine.create(function(type, msg, sz)
	while true do
		ltask.remove(msg, sz)
		type, msg, sz = coroutine.yield()
	end
end)

function ltask.send(address, ...)
	local r = post_message_(address, session_id, MESSAGE_REQUEST, ltask.pack(...))
	if r then
		session_coroutine_suspend_lookup[session_id] = no_response_handler
		session_id = session_id + 1
		return r
	else
		return r
	end
end

function ltask.syscall(address, ...)
	if not post_message(address, session_id, MESSAGE_SYSTEM, ltask.pack(...)) then
		error(string.format("%x is dead", address))
	end
	session_coroutine_suspend_lookup[session_id] = running_thread
	session_id = session_id + 1
	local type, msg, sz = coroutine.yield()
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

-------------

local quit

function ltask.quit()
	ltask.send(SERVICE_ROOT, "quit")
	quit = true
end

local service

function ltask.dispatch(handler)
	service = handler
end

local function system(command, filename, ...)
	if service == nil then
		-- The first system message
		assert(command == "init")
		local f = assert(loadfile(filename))
		local r = f(...)
		if service == nil then
			service = r
		end
	else
		-- todo : other system command
	end
end

SESSION[MESSAGE_SYSTEM] = function (type, msg, sz)
	send_response(system(ltask.unpack_remove(msg, sz)))
end

local function request(command, ...)
	return service[command](...)
end

SESSION[MESSAGE_REQUEST] = function (type, msg, sz)
	send_response(request(ltask.unpack_remove(msg, sz)))
end

while not quit do
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
 			cont = resume_session(co, type, msg, sz)
		end
	end

	while cont do
		yield_service()
		cont = resume_session(running_thread)
	end
	-- todo : inner thread (fork)
	yield_service()
end
