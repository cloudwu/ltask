local start = require "test.start"
start {
    service_path = "service/?.lua;test/?.lua",
    lua_path = "lualib/?.lua",
    bootstrap = {
        {
            name = "timer",
            unique = true,
        },
        {
            name = "logger",
            unique = true,
        },
        {
            name = "sockevent",
            unique = true,
        },
        {
            name = "bootstrap",
        },
    },
    debuglog = "=", -- stdout
}
