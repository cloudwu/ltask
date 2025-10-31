#ifndef ltask_semaphore_h
#define ltask_semaphore_h

#include "cond.h"

struct sem {
	struct cond c;
};

static inline void
sem_init(struct sem *s) {
	cond_create(&s->c);
}

static inline void
sem_deinit(struct sem *s) {
	cond_release(&s->c);
}

static inline int
sem_wait(struct sem *s, int inf) {
	// ignore inf, always wait inf
	cond_wait_begin(&s->c);
	cond_wait(&s->c);
	cond_wait_end(&s->c);
	// fail would return -1 (inf == 0)
	return 0;
}

static inline void
sem_post(struct sem *s) {
	cond_trigger_begin(&s->c);
	cond_trigger_end(&s->c, 1);
}

#endif
