#ifndef ltask_timer_h
#define ltask_timer_h

#include <stdint.h>

struct timer;

typedef void (*timer_execute_func)(void *ud,void *arg);

struct timer * timer_init();
void timer_destroy(struct timer *T);
uint64_t timer_now(struct timer *TI);
uint32_t timer_starttime(struct timer *TI);
void timer_update(struct timer *TI, timer_execute_func func, void *ud);
void timer_add(struct timer *T,void *arg,size_t sz,int time);

#endif