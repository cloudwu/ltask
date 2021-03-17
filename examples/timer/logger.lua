local ltask = require "ltask"

local S = setmetatable({}, { __gc = function() print "Logger exit" end } )

local function writelog()
	while true do
		local ti, id, msg, sz = ltask.poplog()
		if ti == nil then
			break
		end
		local tsec = ti // 100
		local msec = ti % 100
		print(string.format("[%s.%02d : %08d]", os.date("%c", tsec), msec, id), ltask.unpack_remove(msg, sz))
	end
end

local function loop()
	writelog()
	ltask.timeout(100, loop)
end

loop()

function S.exit()
	writelog()
	ltask.quit()
end

return S
