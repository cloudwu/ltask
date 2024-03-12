local start = require "test.start"
start {
    service_path = "service/?.lua;test/?.lua",
    lua_path = "lualib/?.lua",
    bootstrap = {
        ["logger"] = {},
        ["bootstrap"] = { unique = false },
    },
    exclusive = {
        "timer",
        "sockevent",
    },
    debuglog = "=", -- stdout
}
