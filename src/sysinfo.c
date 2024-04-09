#include "sysinfo.h"

#if defined(_WIN32)

#include <windows.h>

int
sysinfo_ncores() {
	// see https://devblogs.microsoft.com/oldnewthing/20200824-00/?p=104116
	return GetActiveProcessorCount(ALL_PROCESSOR_GROUPS);
}

#elif defined(__APPLE__)

#include "unistd.h"

int
sysinfo_ncores() {
	return sysconf(_SC_NPROCESSORS_ONLN);
}

#else

#include <sys/sysinfo.h>

int
sysinfo_ncores() {
	return get_nprocs();
}

#endif