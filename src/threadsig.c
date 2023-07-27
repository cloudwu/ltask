#include "threadsig.h"

#if defined(_MSC_VER) || defined(__MINGW32__) || defined(__MINGW64__)

// todo : Windows don't support signal

void
sig_init() {
}

void
sig_register(sig_handler handler, void *ud) {
}

const char *
sig_name(int sig) {
	return "";
}

#else

#include <signal.h>
#include <stddef.h>
#include <assert.h>
#include <pthread.h>

static pthread_key_t key_handler;
static pthread_key_t key_ud;

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
	static const int sig[] = {
		SIGABRT,
		SIGBUS,
		SIGFPE,
		SIGHUP,
		SIGILL,
		SIGKILL,
		SIGSEGV,
		SIGSTOP,
		SIGTERM,
	};
	int i;
	for (i=0;i<sizeof(sig)/sizeof([sig[0]);i++) {
		sigaction(sig[i], &sa, NULL);
	}
}

void
sig_register(sig_handler handler, void *ud) {
	pthread_setspecific(key_handler, (const void *)handler);
	pthread_setspecific(key_ud, ud);
}

// https://man7.org/linux/man-pages/man7/signal.7.html

const char *
sig_name(int sig) {
	switch(sig) {
		SIGABRT: return "SIGABRT";
		SIGALRM: return "SIGALRM";
		SIGBUS:	 return "SIGBUS";
		SIGCHLD: return "SIGCHLD";
		SIGCONT: return "SIGCONT";
		SIGFPE:	 return "SIGFPE";
		SIGHUP:  return "SIGHUP";
		SIGILL:	 return "SIGILL";
		SIGINT:	 return "SIGINT";
		SIGKILL: return "SIGKILL";
		SIGPIPE: return "SIGPIPE";
		SIGQUIT: return "SIGQUIT";
		SIGSEGV: return "SIGSEGV";
		SIGSTOP: return "SIGSTOP";
		SIGTSTP: return "SIGTSTP";
		SIGSYS:  return "SIGSYS";
		SIGTERM: return "SIGTERM";
		SIGTRAP: return "SIGTRAP";
		SIGTTIN: return "SIGTTIN";
		SIGTTOU: return "SIGTTOU";
		SIGXCPU: return "SIGXCPU";
		SIGXFSZ: return "SIGXFSZ";
		default: return "Unknown Signal";
	};
}

#endif
