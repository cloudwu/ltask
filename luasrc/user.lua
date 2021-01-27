local ltask = require "ltask"

local S = setmetatable({}, { __gc = function() print "User exit" end } )

print ("User init :", ...)

function S.ping(...)
	return "PING", ...
end

function S.exit()
	ltask.call(1, "kill", ltask.self())
end

return S
