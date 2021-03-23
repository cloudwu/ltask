local ltask = require "ltask"
local exclusive = require "ltask.exclusive"

local MESSAGE_SYSTEM <const> = 0
local MESSAGE_REQUEST <const> = 1
local MESSAGE_RESPONSE <const> = 2
local MESSAGE_ERROR <const> = 3
local MESSAGE_SIGNAL <const> = 4

local quit
local blocked_message

local function send_message(address, session, type, msg, sz)
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
		end
	end
end

local function retry_blocked_message()
	ltask.sleep(1)	-- sleep 1/1000s
	coroutine.yield()
	local blocked = blocked_message
	blocked_message = nil
	for i = 1, #blocked, 5 do
		local address = blocked[i]
		local session = blocked[i+1]
		local type    = blocked[i+2]
		local msg     = blocked[i+3]
		local sz      = blocked[i+4]
		send_message(address, session, type, msg, sz)
	end
end

function exclusive.send_request(address, session, ...)
	send_message(address, session, MESSAGE_REQUEST, ltask.pack(...))
end

function exclusive.send_response(address, session, ...)
	send_message(address, session, MESSAGE_RESPONSE, ltask.pack(...))
end

function exclusive.send_error(address, session, ...)
	send_message(address, session, MESSAGE_ERROR, ltask.pack(...))
end

function ltask.log(...)
	ltask.pushlog(ltask.pack(...))
end


local service = {}

function service.dispatch()
end

function service.update()
	coroutine.yield()
	exclusive.sleep(1)
end

function service.quit()
end

local SESSION = {}

local SYSTEM = {}

function SYSTEM.init(from, session, t)
	if t.path then
		package.path = t.path
	end
	if t.cpath then
		package.cpath = t.cpath
	end
	local f, err = loadfile(t.filename)
	if not f then
		exclusive.send_error(from, session, err)
		return
	end
	local ok, res = xpcall(f, debug.traceback, table.unpack(t.args))
	if not ok then
		exclusive.send_error(from, session, res)
		return
	end
	service = res
	exclusive.send_response(from, session)
end

function SYSTEM.quit(from, session)
	quit = {from, session}
end

local function system(from, session, command, ...)
	SYSTEM[command](from, session, ...)
end

SESSION[MESSAGE_SYSTEM] = function (from, session, msg, sz)
	system(from, session, ltask.unpack_remove(msg, sz))
end

print = ltask.log

while not quit do
	while true do
		local from, session, type, msg, sz = ltask.recv_message()
		if not from then
			break
		end
		local f = SESSION[type]
		if f then
			f(from, session, msg, sz)
		else
			service.dispatch(from, session, type, ltask.unpack_remove(msg, sz))
		end
	end
	if blocked_message then
		retry_blocked_message()
	end
	service.update()
end
service.quit()
exclusive.send_response(table.unpack(quit))
