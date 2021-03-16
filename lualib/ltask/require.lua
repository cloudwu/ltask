local require = _G.require
local loaded = package.loaded
local loading = {}

return function (name)
	local m = loaded[name]
	if m ~= nil then
		return m
	end

	local _, main = coroutine.running()
	if main then
		return require(name)
	end

	local filename = package.searchpath(name, package.path)
	if not filename then
		return require(name)
	end

	local modfunc = loadfile(filename)
	if not modfunc then
		return require(name)
	end

	if loading[name] then
		error(("Another coroutine is loading `%s`."):format(name))
	end

	loading[name] = true
	local ok, r = xpcall(modfunc, debug.traceback, name, filename)
	loading[name] = nil

	if ok then
		if r == nil then
			r = true
		end
		loaded[name] = r
		return r
	else
		error(r)
	end
end
