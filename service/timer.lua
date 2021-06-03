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

exclusive.idle_func(function()
	local blocked = exclusive.timer_update()
	coroutine.yield()
	if blocked then
		exclusive.sleep(1)	-- sleep 1/1000s
		coroutine.yield()
		send_blocked_message(blocked)
	else
		exclusive.sleep(1)
	end
end)

local S = {}

function S.quit()
end

return S
