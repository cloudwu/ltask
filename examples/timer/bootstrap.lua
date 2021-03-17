local ltask = require "ltask"
local manager = require "ltask.manager"

local arg = ...

local logger = manager.spawn("logger")

local function print(...)
	ltask.log(...)
end

print "Bootstrap Begin"
print(os.date("%c", (ltask.now())))

local addr = manager.spawn("user", "Hello")

print("Spawn user", addr)

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

print(ltask.call(addr, "ping", "PONG"))
print(ltask.send(addr, "ping", "SEND"))
ltask.send(addr, "exit")
print(ltask.send(addr, "ping", "SEND"))

print "Bootstrap End"

ltask.send(logger, "exit")