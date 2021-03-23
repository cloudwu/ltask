local ltask = require "ltask"
local exclusive = require "ltask.exclusive"

local function send_blocked_message(blocked)
	for i = 1, #blocked, 2 do
		local session = blocked[i]
		local address = blocked[i+1]
		exclusive.send_response(address, session)
	end
end

local S = {}

function S.update()
	local blocked = exclusive.timer_update()
	coroutine.yield()
	if blocked then
		exclusive.sleep(1)	-- sleep 1/1000s
		coroutine.yield()
		send_blocked_message(blocked)
	else
		exclusive.sleep(1)
	end
end

print "Timer start"
function S.quit()
	print "Timer quit"
end

return S
