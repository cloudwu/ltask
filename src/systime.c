#include <time.h>

#if defined(__APPLE__)
#include <AvailabilityMacros.h>
#include <sys/time.h>
#include <mach/task.h>
#include <mach/mach.h>
#endif

#if defined(_WIN32)
#include <Windows.h>
#endif

#include "systime.h"

#if defined(_WIN32)
void
set_highest_timer_resolution() {
	HMODULE ntdll = GetModuleHandleW(L"NTDLL.dll");
	if (ntdll) {
		FARPROC NtSetTimerResolution = GetProcAddress(ntdll, "NtSetTimerResolution");
		if (NtSetTimerResolution) {
			unsigned long timer_current_res = ULONG_MAX;
			((long(NTAPI*)(unsigned long, BOOLEAN, unsigned long*))NtSetTimerResolution)(1, TRUE, &timer_current_res);
		}
	}
}
#endif

uint64_t
systime_wall() {
	uint64_t t;
#if defined(_WIN32)
	FILETIME f;
	GetSystemTimeAsFileTime(&f);
	t = ((uint64_t)f.dwHighDateTime << 32) | f.dwLowDateTime;
	t = t / 100000i64 - 1164447360000i64;
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
