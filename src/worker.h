#ifndef ltask_worker_h
#define ltask_worker_h

#include "atomic.h"
#include "thread.h"
#include "service.h"
#include "debuglog.h"
#include "cond.h"

struct ltask;

struct worker_thread {
	struct ltask *task;
#ifdef DEBUGLOG
	struct debug_logger *logger;
#endif
	int worker_id;
	service_id running;
	atomic_int service_ready;
	atomic_int service_done;
	int term_signal;
	int sleeping;
	int wakeup;
	struct cond trigger;
};

static inline void
worker_init(struct worker_thread *worker, struct ltask *task, int worker_id) {
	worker->task = task;
#ifdef DEBUGLOG
	worker->logger = dlog_new("WORKER", worker_id);
#endif
	worker->worker_id = worker_id;
	atomic_int_init(&worker->service_ready, 0);
	atomic_int_init(&worker->service_done, 0);
	cond_create(&worker->trigger);
	worker->running.id = 0;
	worker->term_signal = 0;
	worker->sleeping = 0;
	worker->wakeup = 0;
}

static inline void
worker_sleep(struct worker_thread *w) {
	if  (w->term_signal)
		return;
	cond_wait_begin(&w->trigger);
	if (w->wakeup) {
		w->wakeup = 0;
	} else {
		w->sleeping = 1;
		cond_wait(&w->trigger);
		w->sleeping = 0;
	}
	cond_wait_end(&w->trigger);
}

static inline int
worker_wakeup(struct worker_thread *w) {
	int sleeping;
	cond_trigger_begin(&w->trigger);
	sleeping = w->sleeping;
	if (sleeping)
		w->wakeup = 1;
	cond_trigger_end(&w->trigger, sleeping);
	return sleeping;
}

static inline void
worker_quit(struct worker_thread *w) {
	cond_trigger_begin(&w->trigger);
	w->sleeping = 0;
	cond_trigger_end(&w->trigger, 0);
}

static inline void
worker_destory(struct worker_thread *worker) {
	cond_release(&worker->trigger);
}

// Calling by Scheduler, may produce service_ready. 0 : succ
static inline int
worker_assign_job(struct worker_thread *worker, service_id id) {
	if (atomic_int_load(&worker->service_ready) == 0) {
		// only one producer (Woker) except itself (worker_steal_job), so don't need use CAS to set
		atomic_int_store(&worker->service_ready, id.id);
		return 0;	
	} else {
		// Already has a job
		return 1;
	}
}

// Calling by Scheduler (steal job) or Worker, may consume service_ready
static inline service_id
worker_get_job(struct worker_thread *worker) {
	service_id id = { 0 };
	int job = atomic_int_load(&worker->service_ready);
	if (job) {
		if (atomic_int_cas(&worker->service_ready, job, 0)) {
			id.id = job;
		}
	}
	return id;
}

static inline int
worker_has_job(struct worker_thread *worker) {
	return atomic_int_load(&worker->service_ready) != 0;
}

// Calling by Scheduler, may consume service_done
static inline service_id
worker_done_job(struct worker_thread *worker) {
	int done = atomic_int_load(&worker->service_done);
	if (done) {
		// only one consumer (Scheduler) , so don't need use CAS to set
		atomic_int_store(&worker->service_done, 0);
	}
	service_id r = { done };
	return r;
}

// Calling by Worker, may produce service_done. 0 : succ
static inline int
worker_complete_job(struct worker_thread *worker) {
	if (atomic_int_cas(&worker->service_done, 0, worker->running.id)) {
		worker->running.id = 0;
		return 0;
	}
	return 1;
}

#endif
