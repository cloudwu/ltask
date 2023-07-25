#ifndef thread_sig_h
#define thread_sig_h

typedef void (*sig_handler)(int sig, void *args);

void sig_register(int id, sig_handler handler, void *ud);

#endif
