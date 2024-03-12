#ifndef ltask_worker_h
#define ltask_worker_h

#include <stdint.h>

#include "atomic.h"
#include "thread.h"
#include "service.h"
#include "debuglog.h"
#include "cond.h"
#include "systime.h"

struct ltask;

#define BINDING_SERVICE_QUEUE 16

struct binding_service {
	int head;
	int tail;
	service_id q[BINDING_SERVICE_QUEUE];
};

struct worker_thread {
	struct ltask *task;
#ifdef DEBUGLOG
	struct debug_logger *logger;
#endif
	int worker_id;
	service_id running;
	service_id binding;
	atomic_int service_ready;
	atomic_int service_done;
	int term_signal;
	int sleeping;
	int wakeup;
	struct cond trigger;
	struct binding_service binding_queue;
	uint64_t schedule_time;
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
	worker->binding.id = 0;
	worker->term_signal = 0;
	worker->sleeping = 0;
	worker->wakeup = 0;
	worker->binding_queue.head = 0;
	worker->binding_queue.tail = 0;
}

static inline int
worker_has_job(struct worker_thread *worker) {
	return atomic_int_load(&worker->service_ready) != 0;
}

static inline void
worker_sleep(struct worker_thread *w) {
	if (w->term_signal)
		return;
	cond_wait_begin(&w->trigger);
	if (worker_has_job(w)) {
		w->wakeup = 0;
	} else {
		if (w->wakeup) {
			w->wakeup = 0;
		} else {
			w->sleeping = 1;
			cond_wait(&w->trigger);
			w->sleeping = 0;
		}
	}
	cond_wait_end(&w->trigger);
}

static inline int
worker_wakeup(struct worker_thread *w) {
	int sleeping;
	cond_trigger_begin(&w->trigger);
	sleeping = w->sleeping;
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

// Calling by Scheduler. 0 : succ
static inline int
worker_binding_job(struct worker_thread *worker, service_id id) {
	struct binding_service * q = &(worker->binding_queue);
	if (q->tail - q->head >= BINDING_SERVICE_QUEUE)	// queue full
		return 1;
	q->q[q->tail % BINDING_SERVICE_QUEUE] = id;
	++q->tail;
	assert(q->tail > 0);
	return 0;
}

// Calling by Scheduler, may produce service_ready. 0 : succ
static inline service_id
worker_assign_job(struct worker_thread *worker, service_id id) {
	if (atomic_int_load(&worker->service_ready) == 0) {
		// try binding queue itself
		struct binding_service * q = &(worker->binding_queue);
		if (q->tail != q->head) {
			id = q->q[q->head % BINDING_SERVICE_QUEUE];
			++q->head;
			if (q->head == q->tail)
				q->head = q->tail = 0;
		}
		// only one producer (Woker) except itself (worker_steal_job), so don't need use CAS to set
		atomic_int_store(&worker->service_ready, id.id);
		return id;
	} else {
		// Already has a job
		service_id ret = { 0 };
		return ret;
	}
}

// Calling by Worker, may consume service_ready
static inline service_id
worker_get_job(struct worker_thread *worker) {
	service_id id = { 0 };
	for (;;) {
		int job = atomic_int_load(&worker->service_ready);
		if (job) {
			if (atomic_int_cas(&worker->service_ready, job, 0)) {
				id.id = job;
				break;
			}
		} else {
			break;
		}
	}
	return id;
}

// Calling by Scheduler, may consume service_ready
static inline service_id
worker_steal_job(struct worker_thread *worker, struct service_pool *p) {
	service_id id = { 0 };
	if (worker->binding_queue.head != worker->binding_queue.tail) {
		// binding job
		return id;
	}
	int job = atomic_int_load(&worker->service_ready);
	if (job) {
		service_id t = { job };
		int worker_id = service_binding_get(p, t);
		if (worker_id == worker->worker_id) {
			// binding job, can't steal
			return id;
		}
		if (atomic_int_cas(&worker->service_ready, job, 0)) {
			id = t;
		}
	}
	return id;
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
