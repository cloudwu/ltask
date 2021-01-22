#include "sysinfo.h"

#if defined(_MSC_VER) || defined(__MINGW32__) || defined(__MINGW64__)

#include <windows.h>

int
sysinfo_ncores() {
	SYSTEM_INFO info;
	GetSystemInfo(&info);
	return (int)info.dwNumberOfProcessors;
}

#else

#include <sys/sysinfo.h>

int
sysinfo_ncores() {
	return get_nprocs();
}

#endif
