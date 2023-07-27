#include "threadsig.h"

#if defined(_MSC_VER) || defined(__MINGW32__) || defined(__MINGW64__)

// todo : Windows don't support signal

void
sig_init() {
}

void
sig_register(sig_handler handler, void *ud) {
}

void
sig_register_default(sig_handler handler, void *ud) {
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

static sig_handler g_sig_handler = NULL;
static void * g_sig_ud = NULL;

static void
signal_handler(int sig) {
	sig_handler f = pthread_getspecific(key_handler);
	void *ud = pthread_getspecific(key_ud);
	if (f) {
		f(sig, ud);
	} else if (g_sig_handler) {
		g_sig_handler(sig, g_sig_ud);
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
	for (i=0;i<sizeof(sig)/sizeof(sig[0]);i++) {
		sigaction(sig[i], &sa, NULL);
	}
}

void
sig_register(sig_handler handler, void *ud) {
	pthread_setspecific(key_handler, (const void *)handler);
	pthread_setspecific(key_ud, ud);
}

void
sig_register_default(sig_handler handler, void *ud) {
	g_sig_handler = handler;
	g_sig_ud = ud;
}

// https://man7.org/linux/man-pages/man7/signal.7.html

const char *
sig_name(int sig) {
	switch(sig) {
		case SIGABRT: return "SIGABRT";
		case SIGALRM: return "SIGALRM";
		case SIGBUS:  return "SIGBUS";
		case SIGCHLD: return "SIGCHLD";
		case SIGCONT: return "SIGCONT";
		case SIGFPE:  return "SIGFPE";
		case SIGHUP:  return "SIGHUP";
		case SIGILL:  return "SIGILL";
		case SIGINT:  return "SIGINT";
		case SIGKILL: return "SIGKILL";
		case SIGPIPE: return "SIGPIPE";
		case SIGQUIT: return "SIGQUIT";
		case SIGSEGV: return "SIGSEGV";
		case SIGSTOP: return "SIGSTOP";
		case SIGTSTP: return "SIGTSTP";
		case SIGSYS:  return "SIGSYS";
		case SIGTERM: return "SIGTERM";
		case SIGTRAP: return "SIGTRAP";
		case SIGTTIN: return "SIGTTIN";
		case SIGTTOU: return "SIGTTOU";
		case SIGXCPU: return "SIGXCPU";
		case SIGXFSZ: return "SIGXFSZ";
		default: return "Unknown Signal";
	};
}

#endif
