local ltask = require "ltask"
local exclusive = require "ltask.exclusive"

local waitfunc, fd = exclusive.eventinit()
print("Event fd =", fd)
ltask.on_idle = waitfunc

local S = {}

function S.ping(...)
	print "PING SOCK"
	return ...
end

return S