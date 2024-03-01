local ltask = require "ltask"

local S = setmetatable({}, { __gc = function() print "User exit" end } )

print ("User init :", ...)

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
	local wall = ltask.walltime()
	local t = ltask.counter()
	ltask.sleep(40) -- sleep 0.4 sec
	print("TIME:", ltask.counter() - t)
	print("WALL TIME:", ltask.walltime() - wall)
	return "PING", ...
end

function S.exit()
	ltask.quit()
end

return S
