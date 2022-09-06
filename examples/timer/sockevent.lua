local ltask = require "ltask"
local exclusive = require "ltask.exclusive"

local waitfunc, fd = exclusive.eventinit()
print("Event fd =", fd)
ltask.on_idle = waitfunc

local S = {}

function S.ping(...)
	io.write("PING SOCK\n")
	return ...
end

return S