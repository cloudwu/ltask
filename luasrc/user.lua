local ltask = require "ltask"

local S = setmetatable({}, { __gc = function() print "User exit" end } )

print ("User init :", ...)

function S.ping(...)
	ltask.timeout(0, function() print(1) end)
	ltask.timeout(0.1, function() print(2) end)
	ltask.timeout(0.2, function() print(3) end)
	ltask.sleep(0.3) -- sleep 0.3 sec
	return "PING", ...
end

function S.exit()
	ltask.call(1, "kill", ltask.self())
end

return S
