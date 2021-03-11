local luadir = "./3rd/lua/"

workspace "ltask"
    configurations { "Debug", "Release" }
    flags{"NoPCH","RelativeLinks"}
    location "./"
    architecture "x64"

    filter "configurations:Debug"
        defines { "DEBUG" }
        symbols "On"

    filter "configurations:Release"
        defines { "NDEBUG" }
        optimize "On"
        -- symbols "On" -- add debug symbols

    filter {"system:windows"}
        characterset "MBCS"
        systemversion "latest"
        warnings "Extra"
        -- staticruntime "on" -- static link vc runtime

    filter { "system:linux" }
        warnings "High"

    filter { "system:macosx" }
        warnings "High"

project "lua54"
    location "build/projects/%{prj.name}"
    objdir "build/obj/%{prj.name}/%{cfg.buildcfg}"
    targetdir "build/bin/%{cfg.buildcfg}"
    kind "SharedLib"
    language "C"
    includedirs {luadir}
    files { luadir.."**.h", luadir.."**.c"}
    removefiles(luadir.."luac.c")
    removefiles(luadir.."lua.c")
    filter { "system:windows" }
        defines {"LUA_BUILD_AS_DLL"}
    filter { "system:linux" }
        defines {"LUA_USE_LINUX"}
    filter { "system:macosx" }
        defines {"LUA_USE_MACOSX"}
    filter{"configurations:*"}
        postbuildcommands{"{COPY} %{cfg.buildtarget.abspath} %{wks.location}"}

project "lua"
    location "build/projects/%{prj.name}"
    objdir "build/obj/%{prj.name}/%{cfg.buildcfg}"
    targetdir "build/bin/%{cfg.buildcfg}"
    kind "ConsoleApp"
    language "C"
    includedirs {luadir}
    files { luadir.."lua.c"}
    links{"lua54"}
    filter { "system:windows" }
        defines {"LUA_BUILD_AS_DLL"}
    filter { "system:linux" }
        defines {"LUA_USE_LINUX"}
        links{"dl","pthread", "m"}
        linkoptions {"-Wl,-rpath,./"}
    filter { "system:macosx" }
        defines {"LUA_USE_MACOSX"}
        linkoptions {"-Wl,-rpath,./"}
    filter{"configurations:*"}
        postbuildcommands{"{COPY} %{cfg.buildtarget.abspath} %{wks.location}"}


project "ltask"
    location "build/projects/%{prj.name}"
    objdir "build/obj/%{prj.name}/%{cfg.buildcfg}"
    targetdir "build/bin/%{cfg.buildcfg}"
    kind "SharedLib"
    language "C"
    targetprefix ""
    includedirs {luadir, "src"}
    files {"./src/**.h", "./src/**.c" }
    links{"lua54"}
    filter { "system:windows" }
        defines {"LUA_BUILD_AS_DLL"}
        links{"winmm"}
    filter { "system:macosx" }
        defines {"LUA_USE_MACOSX"}
        links{"pthread", "m"}
        linkoptions {"-Wl,-rpath,./"}
    filter{"configurations:*"}
        postbuildcommands{"{COPY} %{cfg.buildtarget.abspath} %{wks.location}"}
