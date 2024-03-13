local ltask = require "ltask"

local arg = ...

print "Bootstrap Begin"
print(os.date("%c", (ltask.now())))
local addr = ltask.spawn("user", "Hello")

print("Spawn user", addr)

-- test async

local async = ltask.async()

async:request( addr, "req", 30 )
async:request( addr, "req", 10 )

print("Waiting request 1")
async:wait()
print("Get request 1")

async:request( addr, "req", 5 )
async:request( addr, "req", 20 )

print("Waiting request 2")
async:wait()
print("Get request 2")

--- test request

local req = ltask.request
	{ addr, "wait", 30, id = 1 }
	{ addr, "wait", 20, id = 2 }
	{ addr, "wait", 10, id = 3 }
	{ addr, "wait", 5, id = 4 }

for req, resp in req:select(25) do
	if resp then
		print("REQ", req.id, resp[1])
	else
		print("ERR", req.id, req.error)
	end
end

local se =ltask.queryservice "sockevent"
print("PING sockevent", se)
print(ltask.call(se , "ping", "PONG"))
print(ltask.call(addr, "ping", "PONG"))
ltask.send(addr, "exit")
print(ltask.send(addr, "ping", "SEND"))

local function run_task(what, ti)
	if ti > 0 then
		ltask.sleep(ti)
	end
	return what
end

local task = {
	{ run_task, "a", 0 },
	{ run_task, "b", 0 },
	{ run_task, "c", 0 },
	{ run_task, "d", 0 },
	{ run_task, "e", 0 },
	{ run_task, "f", 0 },
	{ run_task, "g", 0 },
	{ run_task, "h", 10 },
	{ run_task, "i", 20 },
}

for req, resp in ltask.parallel(task) do
	print(resp)
end

print "Bootstrap End"
