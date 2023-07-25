#include "threadsig.h"
#include <signal.h>
#include <stddef.h>
#include <assert.h>

#if defined(_MSC_VER) || defined(__MINGW32__) || defined(__MINGW64__)

void
sig_register(int id, sig_handler handler, void *ud) {
	// todo : Windows don't support signal
}

#else

#define MAX_THREAD 32

struct thread_handler {
	sig_handler f;
	void *ud;
};

struct thread_handler g_handler[MAX_THREAD];

#define SIGHANDLE(n) void sig_handler_##n(int sig) { g_handler[n].f(sig, g_handler[n].ud); }

SIGHANDLE(0)
SIGHANDLE(1)
SIGHANDLE(2)
SIGHANDLE(3)
SIGHANDLE(4)
SIGHANDLE(5)
SIGHANDLE(6)
SIGHANDLE(7)
SIGHANDLE(8)
SIGHANDLE(9)
SIGHANDLE(10)
SIGHANDLE(11)
SIGHANDLE(12)
SIGHANDLE(13)
SIGHANDLE(14)
SIGHANDLE(15)
SIGHANDLE(16)
SIGHANDLE(17)
SIGHANDLE(18)
SIGHANDLE(19)
SIGHANDLE(20)
SIGHANDLE(21)
SIGHANDLE(22)
SIGHANDLE(23)
SIGHANDLE(24)
SIGHANDLE(25)
SIGHANDLE(26)
SIGHANDLE(27)
SIGHANDLE(28)
SIGHANDLE(29)
SIGHANDLE(30)
SIGHANDLE(31)

void (*g_handler_func[MAX_THREAD])(int) = {
	sig_handler_0,
	sig_handler_1,
	sig_handler_2,
	sig_handler_3,
	sig_handler_4,
	sig_handler_5,
	sig_handler_6,
	sig_handler_7,
	sig_handler_8,
	sig_handler_9,
	sig_handler_10,
	sig_handler_11,
	sig_handler_12,
	sig_handler_13,
	sig_handler_14,
	sig_handler_15,
	sig_handler_16,
	sig_handler_17,
	sig_handler_18,
	sig_handler_19,
	sig_handler_20,
	sig_handler_21,
	sig_handler_22,
	sig_handler_23,
	sig_handler_24,
	sig_handler_25,
	sig_handler_26,
	sig_handler_27,
	sig_handler_28,
	sig_handler_29,
	sig_handler_30,
	sig_handler_31,
};


void
sig_register(int id, sig_handler handler, void *ud) {
	assert(id >=0 && id < MAX_THREAD);
    struct sigaction sa;
    sa.sa_handler = g_handler_func[id];
	g_handler[id].f = handler;
	g_handler[id].ud = ud;
    sigfillset(&sa.sa_mask);
    sigaction(SIGSEGV, &sa, NULL);
}

#endif