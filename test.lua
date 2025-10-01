local start = require "test.start"
start {
    core = {
        debuglog = "=", -- stdout
        worker = 3, -- avoid stuck when running ci on GHA macOS
    },
    service_path = "service/?.lua;test/?.lua",
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
}
