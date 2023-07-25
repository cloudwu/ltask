#include "threadsig.h"
#include <signal.h>
#include <stddef.h>
#include <assert.h>
#include <pthread.h>

static pthread_key_t key_handler;
static pthread_key_t key_ud;

#if defined(_MSC_VER) || defined(__MINGW32__) || defined(__MINGW64__)

void
sig_init() {
	// todo : Windows don't support signal
}

#else

static void
signal_handler(int sig) {
	sig_handler f = pthread_getspecific(key_handler);
	void *ud = pthread_getspecific(key_ud);
	if (f) {
		f(sig, ud);
	}
}

void
sig_init() {
	pthread_key_create(&key_handler, NULL);
    pthread_key_create(&key_ud, NULL);
	struct sigaction sa;
	sa.sa_handler = signal_handler;
	sigfillset(&sa.sa_mask);
	sigaction(SIGSEGV, &sa, NULL);
	sigaction(SIGFPE, &sa, NULL);
	sigaction(SIGABRT, &sa, NULL);
}

#endif

void
sig_register(sig_handler handler, void *ud) {
	pthread_setspecific(key_handler, (const void *)handler);
	pthread_setspecific(key_ud, ud);
}

const char *
sig_name(int sig) {
	switch(sig) {
	case SIGSEGV: return "SIGSEGV";
	case SIGFPE: return "SIGFPE";
	case SIGABRT: return "SIGABRT";
	default: return "Unknown Signal";
	};
}