local ltask = require "ltask"
local exclusive = require "ltask.exclusive"

local MESSAGE_RESPONSE <const> = 2

-- todo: query timer internal information
local function dispatch_messages()
	local from, session, type, msg, sz = ltask.recv_message()
	if from then
		print("Timer message : ", ltask.unpack_remove(msg, sz))
		return true
	end
end

print "Timer start"
while true do
	while dispatch_messages() do end
	local blocked = exclusive.timer_update()
	coroutine.yield()
	local sleeping = 2500 -- sleep 2.5 ms
	if blocked then
		print "Too many timer message"
		for i = 1, #blocked, 2 do
			local session = blocked[i]
			local address = blocked[i+1]
			if not exclusive.send(address, session, MESSAGE_RESPONSE) then
				-- blocked
				coroutine.yield()
				if sleeping > 100 then
					sleeping = sleeping - 100
					ltask.usleep(100)	-- sleep 0.1 ms
				end
			end
		end
	end

	ltask.usleep(sleeping)
end
