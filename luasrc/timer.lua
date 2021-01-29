local ltask = require "ltask"
local exclusive = require "ltask.exclusive"

local SERVICE_ROOT <const> = 1
local MESSAGE_RESPONSE <const> = 2
local quit

-- todo: query timer internal information
local function dispatch_messages()
	local from, session, type, msg, sz = ltask.recv_message()
	if from then
		if from == SERVICE_ROOT then
			local command = ltask.unpack_remove(msg, sz)
			if command == "QUIT" then
				quit = true
			end
		else
			print("Timer message : ", from, ltask.unpack_remove(msg, sz))
		end
		return true
	end
end

local blocked_message

local function retry_blocked_message()
	local blocked = blocked_message
	blocked_message = nil
	for i = 1, #blocked, 2 do
		local session = blocked[i]
		local address = blocked[i+1]
		if not exclusive.send(address, session, MESSAGE_RESPONSE) then
			-- blocked
			print("Timer message to " .. address .. " is blocked")
			if blocked_message then
				local n = #blocked_message
				blocked_message[n+1] = session
				blocked_message[n+2] = address
			else
				blocked_message = { session, address }
			end
		end
	end
end

local function send_blocked_message(blocked)
	local retry = 1
	for i = 1, #blocked, 2 do
		local session = blocked[i]
		local address = blocked[i+1]
		if not exclusive.send(address, session, MESSAGE_RESPONSE) then
			-- blocked
			if retry >= 10 then
				if blocked_message then
					local n = #blocked_message
					blocked_message[n+1] = session
					blocked_message[n+2] = address
				else
					blocked_message = { session, address }
				end
			else
				ltask.sleep(1)
				coroutine.yield()
				retry = retry + 1
			end
		end
	end
end

print "Timer start"
while not quit do
	while dispatch_messages() do end
	local blocked = exclusive.timer_update()
	coroutine.yield()
	if blocked then
		ltask.sleep(1)	-- sleep 1/1000s
		coroutine.yield()
		if blocked_message then
			retry_blocked_message()
		end
		send_blocked_message(blocked)
	else
		ltask.sleep(1)
	end
end
print "Timer quit"
