local ltask = require "ltask"

local waitfunc, fd = ltask.eventinit()
print("Event fd =", fd)

ltask.idle_handler(function()
	print("Idle")
	waitfunc()
end)

local S = {}

function S.ping(...)
	print "PING SOCK"
	return ...
end

return S