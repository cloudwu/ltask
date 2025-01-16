#!/bin/bash

make clean

if [[ "$OSTYPE" == "darwin"* ]]; then
    # The version of Clang that comes pre-installed with MacOS is outdated.
    brew install llvm
    LLVM_SDK_PATH=$(brew --prefix llvm)
    if [ -z "$LLVM_SDK_PATH" ]; then
        echo "Error: please run command: brew install llvm."
        exit 1
    fi

    make LUAINC="-I/usr/local/include/" CC="$LLVM_SDK_PATH/bin/clang" CFLAGS="-fsanitize=address -g -Wall"
    export ASAN_OPTIONS=detect_leaks=1:fast_unwind_on_malloc=false
    ASAN_LIB_ABS_PATH=$("$LLVM_SDK_PATH"/bin/clang -print-file-name=libclang_rt.asan_osx_dynamic.dylib)
    "$LLVM_SDK_PATH"/bin/clang -dynamiclib .github/asan_assets/dlclose.c -o ./.github/asan_assets/libdlclose.dylib -install_name libdlclose.dylib
    # export DYLD_PRINT_LIBRARIES=1 X=1
    export DYLD_INSERT_LIBRARIES="./.github/asan_assets/libdlclose.dylib:$ASAN_LIB_ABS_PATH"
    
    lua test.lua
else
    make LUAINC="-I/usr/local/include/" CFLAGS="-fsanitize=address -g -Wall"
    export ASAN_OPTIONS=fast_unwind_on_malloc=false
    ASAN_LIB_ABS_PATH=$(gcc -print-file-name=libasan.so)
    gcc -Wl,-undefined,dynamic_lookup --shared .github/asan_assets/dlclose.c -o .github/asan_assets/libdlclose.so
    export LD_PRELOAD="$ASAN_LIB_ABS_PATH:./.github/asan_assets/libdlclose.so"

    lua test.lua
fi