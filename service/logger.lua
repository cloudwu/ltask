local ltask = require "ltask"
local SERVICE_ROOT <const> = 1

local S = {}
local lables = {}

local function querylabel(id)
	local label = lables[id]
	if not label then
		label = ltask.call(SERVICE_ROOT, "label", id)
		lables[id] = label
	end
	return label
end

local function writelog()
	local flush
	while true do
		local ti, id, msg, sz = ltask.poplog()
		if ti == nil then
			if flush then
				io.flush()
			end
			break
		end
		local tsec = ti // 100
		local msec = ti % 100
		local t = table.pack(ltask.unpack_remove(msg, sz))
		local str = {}
		for i = 1, t.n do
			str[#str+1] = tostring(t[i])
		end
		io.write(string.format("[%s.%02d : %-10s]\t%s\n", os.date("%c", tsec), msec, querylabel(id), table.concat(str, "\t")))
		flush = true
	end
end

ltask.fork(function()
	while true do
		writelog()
		ltask.sleep(100)
	end
end)

function S.quit()
	writelog()
end

return S
