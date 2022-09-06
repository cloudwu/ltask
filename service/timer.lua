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

local last = ltask.walltime()
local blocked = {}

function ltask.on_idle()
	local now = ltask.walltime()
	local delta = now - last
	if delta > 0 then
		for _ = 1, delta * 10 do
			exclusive.timer_update(blocked)
		end
		last = now
	end
	exclusive.sleep(10)
	if #blocked > 0 then
		send_blocked_message(blocked)
		blocked = {}
	end
end

return {}
