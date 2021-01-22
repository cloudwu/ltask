#ifndef ltask_worker_h
#define ltask_worker_h

#include "atomic.h"
#include "thread.h"
#include "service.h"

struct worker_thread {
	service_id running;
	atomic_int service_ready;
	atomic_int service_done;

	struct thread_event trigger;
};

struct ltask_config;

void worker_init(struct worker_thread *worker, struct ltask_config *config);
void worker_destory(struct worker_thread *worker);
void worker_thread_func(void *);

static inline int
worker_is_ready(struct worker_thread *worker) {
	return atomic_int_load(&worker->service_ready) == 0;
}

#endif
