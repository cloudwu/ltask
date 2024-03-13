local ltask = require "ltask"

local MESSAGE_RESPONSE <const> = 2

local messages = {}
local timer = {}

function timer.quit()
	ltask.quit()
end

local send_message = ltask.send_message
local coroutine_yield = coroutine.yield
local message_receipt = ltask.message_receipt

local blocked

local mcount = 0

local function send_all_messages()
	for i = 1, #messages do
		local v = messages[i]
		local session = v >> 32
		local addr = v & 0xffffffff
		local blocked_queue = blocked and blocked[addr]
		if blocked_queue then
			blocked_queue[#blocked_queue+1] = session
		else
			send_message(addr, session, MESSAGE_RESPONSE)
			mcount = mcount + 1
			coroutine_yield(true)
			if message_receipt() == RECEIPT_BLOCK then
				blocked = blocked or {}
				blocked[addr] = { session }
			end
		end
	end
end

local function send_blocked_queue(addr, queue)
	local n = #queue
	for i = 1, n do
		send_message(addr, queue[i], MESSAGE_RESPONSE)
		coroutine_yield(true)
		if message_receipt() == RECEIPT_BLOCK then
			table.move(queue, i, n, 1)
			return true
		end
	end
end

local function send_blocked()
	local b
	for addr, queue in pairs(blocked) do
		if send_blocked_queue(addr, queue) then
			b = true
		else
			blocked[addr] = nil
		end
	end
	if not b then
		blocked = nil
	end
end

ltask.fork(function()
	while true do
		ltask.timer_update(messages)
		send_all_messages()
		ltask.timer_sleep(10)
		if blocked then
			send_blocked()
		else
			ltask.sleep(0)
		end
	end
end)

return timer
