local ltask = require "ltask"
local exclusive = require "ltask.exclusive"

local MESSAGE_ERROR <const> = 3

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
