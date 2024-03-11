local ltask = require "ltask"

local last = ltask.walltime()

local messages = {}
local timer = {}

function timer.exit()
	ltask.quit()
end

ltask.fork(function()
	while true do
		local now = ltask.walltime()
		local delta = now - last
		if delta > 0 then
			for _ = 1, delta * 10 do
				ltask.timer_update(messages)
				for i = 1, #messages do
					ltask.send_message_handle(messages[i])
					coroutine.yield(true)
					local type, msg, sz = ltask.message_receipt()
					if type == RECEIPT_DONE then
					elseif type == RECEIPT_ERROR then
						ltask.remove(msg, sz)
					elseif type == RECEIPT_BLOCK then
						-- todo: send again
						ltask.remove(msg, sz)
					end
				end
			end
			last = now
		end
		ltask.timer_sleep(10)
		ltask.sleep(0)
	end
end)

return timer
