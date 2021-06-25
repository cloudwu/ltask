local ltask = require "ltask"
local exclusive = require "ltask.exclusive"

local MESSAGE_RESPONSE <const> = 2

local function send_blocked_message(blocked)
	for i = 1, #blocked, 2 do
		local session = blocked[i]
		local address = blocked[i+1]
		ltask.post_message(address, session, MESSAGE_RESPONSE)
	end
end

ltask.fork(function ()
	while true do
		local blocked = exclusive.timer_update()
		exclusive.sleep(1)	-- sleep 1/1000s
		if blocked then
			send_blocked_message(blocked)
		end
		ltask.sleep(0)
	end
end)

return {}
