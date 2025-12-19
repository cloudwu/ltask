ltask: Yet another lua task library
============================

ltask is inspired by skynet (https://github.com/cloudwu/skynet) , but it's a library rather than a framework.

It implement an n:m scheduler , so that you can run M lua VMs on N OS threads.

Each lua service (an independent lua VM) works in request/response mode, they use message channels to inter-communicate.

`root` is a special service that can spawn new services. For example, 

```lua
-- user
local ltask = require "ltask"

local S = {}

print "User Start"

function S.ping(...)
	ltask.timeout(10, function() print(1) end)
	ltask.timeout(20, function() print(2) end)
	ltask.timeout(30, function() print(3) end)
	ltask.sleep(40) -- sleep 0.4 sec
	-- response
	return "PING", ...
end

return S
```

```lua
-- root
local function boot()
	print "Root Start"
	print(os.date("%c", (ltask.now())))
	local addr = S.spawn("user", "Hello")	-- spawn a new service `user`
	print(ltask.call(addr, "ping", "PONG"))	-- request "ping" message
end

boot()
```

Test
====
```
lua test.lua
```
