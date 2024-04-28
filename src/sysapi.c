#include "sysapi.h"

#ifndef _WIN32

#include <assert.h>
#include <errno.h>
#include <time.h>

void
sys_init() {
}

void
sys_sleep(unsigned int msec) {
	struct timespec timeout;
	int rc;
	timeout.tv_sec  = msec / 1000;
	timeout.tv_nsec = (msec % 1000) * 1000 * 1000;
	do
		rc = nanosleep(&timeout, &timeout);
	while (rc == -1 && errno == EINTR);
	assert(rc == 0);
}

#else

#include <windows.h>

#ifndef CREATE_WAITABLE_TIMER_HIGH_RESOLUTION
#    define CREATE_WAITABLE_TIMER_HIGH_RESOLUTION 0x2
#endif

NTSTATUS NTAPI NtSetTimerResolution(ULONG RequestedResolution, BOOLEAN Set, PULONG ActualResolution);

static int support_hrtimer = 0;

void
sys_init() {
	HANDLE timer = CreateWaitableTimerExW(NULL, NULL, CREATE_WAITABLE_TIMER_HIGH_RESOLUTION, TIMER_ALL_ACCESS);
	if (timer == NULL) {
		support_hrtimer = 0;
	} else {
		CloseHandle(timer);
		// supported in Windows 10 1803+
		support_hrtimer = 1;
	}
}

static void hrtimer_start() {
	if (!support_hrtimer) {
		ULONG actual_res = 0;
		NtSetTimerResolution(10000, TRUE, &actual_res);
	}
}

static void hrtimer_end() {
	if (!support_hrtimer) {
		ULONG actual_res = 0;
		NtSetTimerResolution(10000, FALSE, &actual_res);
	}
}

void
sys_sleep(unsigned int msec) {
	HANDLE timer = CreateWaitableTimerExW(NULL, NULL, support_hrtimer ? CREATE_WAITABLE_TIMER_HIGH_RESOLUTION : 0, TIMER_ALL_ACCESS);
	if (!timer) {
		return;
	}
	hrtimer_start();
	LARGE_INTEGER time;
	time.QuadPart = -((long long)msec * 10000);
	if (SetWaitableTimer(timer, &time, 0, NULL, NULL, 0)) {
		WaitForSingleObject(timer, INFINITE);
	}
	CloseHandle(timer);
	hrtimer_end();
}

#endif
