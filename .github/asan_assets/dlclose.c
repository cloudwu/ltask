// This file is used to resolve an issue where the dynamic library is closed prematurely, causing asan to report that it cannot retrieve symbols. <unknown module> 
// NOTICE: Do not use in a production environment
#include <stdio.h>

#if defined(__APPLE__)
#include <dlfcn.h>

// from: https://github.com/apple-oss-distributions/dyld/blob/c8a445f88f9fc1713db34674e79b00e30723e79d/include/mach-o/dyld-interposing.h#L43
#define DYLD_INTERPOSE(_replacement,_replacee) \
   __attribute__((used)) static struct{ const void* replacement; const void* replacee; } _interpose_##_replacee \
            __attribute__ ((section ("__DATA,__interpose,interposing"))) = { (const void*)(unsigned long)&_replacement, (const void*)(unsigned long)&_replacee };

int mydlclose(void * __handle) {
  (void)__handle; // [[maybe_unused]] is not in C standard yet
  return 0;
}

// dyld-interposing
DYLD_INTERPOSE(mydlclose, dlclose);
#else
// This function prevents unloading of shared libraries, thus the sanitizers【BUG】
// can always find the address that belonged to a shared library.
// See https://github.com/google/sanitizers/issues/89#issuecomment-484435084
// test pass: https://github.com/KatanaGraph/katana/pull/402/files#diff-46402e2a4674871c253dd33ff206252619d7d8890d477f76100f8eecf9071d3a
int dlclose(void* ptr) {
  (void)ptr; // [[maybe_unused]] is not in C standard yet
  return 0;
}
#endif

