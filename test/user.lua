local ltask = require "ltask"
local coroutine = require "test.coroutine"	-- Use coroutine wrap instead of strandard coroutine api

local S = setmetatable({}, { __gc = function() print "User exit" end } )

print ("User init :", ...)
local worker = ltask.worker_id()
print (string.format("User %d in worker %d", ltask.self(), worker))
ltask.worker_bind(worker)	-- bind to current worker thread

local function coroutine_test()
	coroutine.yield "Coroutine yield"
	return "Coroutine end"
end

local co = coroutine.create(coroutine_test)
while true do
	local ok, ret = coroutine.resume(co)
	if ok then
		print(ret)
	else
		break
	end
end

function S.wait(ti)
	if ti < 10 then
		error("Error : " .. ti)
	end
	ltask.sleep(ti)
	return ti
end

function S.req(ti)
	print ("Wait Req", ti)
	ltask.sleep(ti)
	print ("Wait resp", ti)
	return ti
end

function S.ping(...)
	ltask.timeout(10, function() print(1) end)
	ltask.timeout(20, function() print(2) end)
	ltask.timeout(30, function() print(3) end)
	local t = ltask.counter()
	ltask.sleep(40) -- sleep 0.4 sec
	print("TIME:", ltask.counter() - t)
	return "PING", ...
end

local task_queue = {}

local function add_task(f, ...)
	local tail = task_queue.t
	if tail then
		task_queue[tail] = table.pack( f, ... )
		task_queue.t = tail + 1
	else
		-- empty
		task_queue.h = 1
		task_queue.t = 1
		f(...)
		while task_queue.h < task_queue.t do
			local h = task_queue.h
			local task = task_queue[h]
			task_queue[h] = nil
			task_queue.h = h + 1
			task[1](table.unpack(task, 2, task.n))			
		end
		task_queue.h = nil
		task_queue.t = nil
	end
end

local QUEUE = setmetatable({}, {
	__newindex = function(_, name, f)
		S[name] = function(...)
			add_task(f, ...)
		end
	end })

function QUEUE.func1(v)
	print("func1 begin", v)
	ltask.sleep(1)
	print("func1 end", v)
end

function QUEUE.func2()
	print("func2")
end

function S.exit()
	ltask.quit()
end

return S
