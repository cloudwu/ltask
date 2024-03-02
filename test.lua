local start = require "test.start"
start {
    service_path = "service/?.lua;test/?.lua",
    lua_path = "lualib/?.lua",
    bootstrap = { "bootstrap" },
    logger = { "logger" },
    exclusive = {
        "timer",
        "sockevent",
    },
    debuglog = "=", -- stdout
}
