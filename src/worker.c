#include "worker.h"
#include "queue.h"
#include "config.h"

void
worker_init(struct worker_thread *worker, struct ltask_config *config) {
	atomic_int_init(&worker->service_ready, 0);
	atomic_int_init(&worker->service_done, 0);
	thread_event_create(&worker->trigger);
	worker->running.id = 0;
}

void
worker_destory(struct worker_thread *worker) {
	thread_event_release(&worker->trigger);
}

void
worker_thread_func(void *ud) {

}
