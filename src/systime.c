#if defined(__MINGW32__) || defined(__MINGW64__)
#define _WIN32_WINNT 0x0600
#endif

#if defined(_WIN32)
#include <Windows.h>
#endif

#include <time.h>

#if defined(__APPLE__)
#include <AvailabilityMacros.h>
#include <sys/time.h>
#include <mach/task.h>
#include <mach/mach.h>
#endif

#include "systime.h"

uint64_t
systime_wall() {
	uint64_t t;
#if defined(_WIN32)
	FILETIME f;
	GetSystemTimeAsFileTime(&f);
	t = ((uint64_t)f.dwHighDateTime << 32) | f.dwLowDateTime;
	t = t / (uint64_t)100000 - (uint64_t)1164447360000;
#elif !defined(__APPLE__) || defined(AVAILABLE_MAC_OS_X_VERSION_10_12_AND_LATER)
	struct timespec ti;
	clock_gettime(CLOCK_REALTIME, &ti);
	t = ti.tv_sec * 100 + (ti.tv_nsec / 10000000);
#else
	struct timeval tv;
	gettimeofday(&tv, NULL);
	t = tv.tv_sec * 100 + tv.tv_usec / 10000;
#endif
	return t;
}

uint64_t
systime_mono() {
	uint64_t t;
#if defined(_WIN32)
	t = GetTickCount64() / 10;
#elif !defined(__APPLE__) || defined(AVAILABLE_MAC_OS_X_VERSION_10_12_AND_LATER)
	struct timespec ti;
	clock_gettime(CLOCK_MONOTONIC, &ti);
	t = (uint64_t)ti.tv_sec * 100;
	t += ti.tv_nsec / 10000000;
#else
	struct timeval tv;
	gettimeofday(&tv, NULL);
	t = (uint64_t)tv.tv_sec * 100;
	t += tv.tv_usec / 10000;
#endif
	return t;
}
