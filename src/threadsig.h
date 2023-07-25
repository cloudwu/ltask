#ifndef thread_sig_h
#define thread_sig_h

typedef void (*sig_handler)(int sig, void *args);

void sig_init();
void sig_register(sig_handler handler, void *ud);
const char * sig_name(int sig);

#endif
