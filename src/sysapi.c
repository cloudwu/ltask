#include "sysapi.h"

#ifndef _WIN32

#include <unistd.h>

void
sys_init() {
}

void
sys_sleep(unsigned int csec) {
	usleep(usec * 1000);
}

#else

#include <windows.h>

void
sys_init() {
	timeBeginPeriod(1);
}

void
sys_sleep(unsigned int csec) {
	Sleep(csec);
}

#endif

