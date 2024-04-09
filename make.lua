local lm = require "luamake"

lm:lua_dll "ltask" {
    c = "c11",
    sources = "src/*.c",
    defines = {
        --"DEBUGLOG",
        lm.mode=="debug" and "DEBUGTHREADNAME",
    },
    windows = {
        defines = {
            "_WIN32_WINNT=0x0601"
        },
        links = {
            "ws2_32",
            "winmm",
        }
    },
    msvc = {
        flags = {
            "/experimental:c11atomics"
        },
        ldflags = {
            "-export:luaopen_ltask_bootstrap",
        },
    },
    gcc = {
        links = "pthread",
        visibility = "default",
        defines = "_XOPEN_SOURCE=600",
    }
}
