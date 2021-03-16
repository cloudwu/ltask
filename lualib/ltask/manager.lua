local ltask = require "ltask"

local SERVICE_ROOT <const> = 1

local m = {}

function m.spawn(name, ...)
    return ltask.call(SERVICE_ROOT, "spawn", name, ...)
end

function m.kill(addr)
    return ltask.call(SERVICE_ROOT, "kill", addr)
end

function m.register(name)
    return ltask.call(SERVICE_ROOT, "register", name)
end

function m.query(name)
    return ltask.call(SERVICE_ROOT, "query", name)
end

return m
