#define LUA_LIB

#include "sockevent.h"

#include <lua.h>
#include <lauxlib.h>
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>

#include "atomic.h"
#include "queue.h"
#include "thread.h"
#include "config.h"
#include "worker.h"
#include "service.h"
#include "message.h"
#include "lua-seri.h"
#include "timer.h"
#include "sysapi.h"
#include "debuglog.h"
#include "logqueue.h"
#include "systime.h"
#include "threadsig.h"

LUAMOD_API int luaopen_ltask(lua_State *L);
LUAMOD_API int luaopen_ltask_bootstrap(lua_State *L);
LUAMOD_API int luaopen_ltask_exclusive(lua_State *L);
LUAMOD_API int luaopen_ltask_root(lua_State *L);

#define THREAD_NONE -1
#define THREAD_WORKER(n) (MAX_EXCLUSIVE + (n))
#define THREAD_EXCLUSIVE(n) (n)

#ifndef DEBUGLOG

#define debug_printf(logger, fmt, ...)

#else

#define debug_printf dlog_write

#endif

struct exclusive_thread {
	struct debug_logger * logger;
	struct ltask *task;
	struct queue *sending;
	int thread_id;
	int term_signal;
	service_id service;
	struct sockevent event;
};

struct ltask {
	const struct ltask_config *config;
	struct worker_thread *workers;
	struct exclusive_thread exclusives[MAX_EXCLUSIVE];
	struct service_pool *services;
	struct queue *schedule;
	struct timer *timer;
#ifdef DEBUGLOG
	struct debug_logger *logger;
#endif
	struct logqueue *lqueue;
	atomic_int schedule_owner;
	atomic_int active_worker;
	atomic_int thread_count;
	FILE *logfile;
};

struct service_ud {
	struct ltask *task;
	service_id id;
};

static inline struct exclusive_thread *
get_exclusive_thread(struct ltask *task, int thread_id) {
	if (thread_id < 0 || thread_id >= MAX_EXCLUSIVE)
		return NULL;
	return &task->exclusives[thread_id];
}

static int
get_worker_id(struct ltask *task, service_id id) {
	int total_worker = task->config->worker;
	int i;
	for (i=0;i<total_worker;i++) {
		struct worker_thread * w = &task->workers[i];
		if (w->running.id == id.id)
			return i;
	}
	return -1;
}

static inline void
schedule_back(struct ltask *task, service_id id) {
	int r = queue_push_int(task->schedule, (int)id.id);
	// Must succ because task->schedule is large enough.
	(void)r;
	assert(r == 0);
}

static void
dispatch_schedule_message(struct ltask *task, service_id id, struct message *msg) {
	struct service_pool *P = task->services;
	if (id.id != SERVICE_ID_ROOT) {
		// only root can send schedule message
		service_write_receipt(P, id, MESSAGE_RECEIPT_ERROR, msg);
		return;
	}

	service_id sid = { msg->session };
	switch (msg->type) {
	case MESSAGE_SCHEDULE_NEW:
		msg->to = service_new(P, sid.id);
		debug_printf(task->logger, "New service %x", msg->to.id);
		if (msg->to.id == 0) {
			service_write_receipt(P, id, MESSAGE_RECEIPT_ERROR, msg);
		} else {
			service_write_receipt(P, id, MESSAGE_RECEIPT_RESPONCE, msg);
		}
		break;
	case MESSAGE_SCHEDULE_DEL:
		debug_printf(task->logger, "Delete service %x", sid.id);
		service_delete(P, sid);
		message_delete(msg);
		service_write_receipt(P, id, MESSAGE_RECEIPT_DONE, NULL);
		break;
	default:
		service_write_receipt(P, id, MESSAGE_RECEIPT_ERROR, msg);
		break;
	}
}

static void
check_message_to(struct ltask *task, service_id to) {
	struct service_pool *P = task->services;
	int status = service_status_get(P, to);
	if (status == SERVICE_STATUS_IDLE) {
		debug_printf(task->logger, "Service %x is in schedule", to.id);
		service_status_set(P, to, SERVICE_STATUS_SCHEDULE);
		schedule_back(task, to);
	} else if (status == SERVICE_STATUS_EXCLUSIVE) {
		debug_printf(task->logger, "Message to exclusive service %d", to.id);
		int ethread = service_thread_id(task->services, to);
		struct exclusive_thread *thr = get_exclusive_thread(task, ethread);
		assert(thr);
		sockevent_trigger(&thr->event);
	}
}

static void
dispatch_out_message(struct ltask *task, service_id id, struct message *msg) {
	debug_printf(task->logger, "Message from %d to %d type=%d", id.id, msg->to.id, msg->type);
	struct service_pool *P = task->services;
	if (msg->to.id == SERVICE_ID_SYSTEM) {
		dispatch_schedule_message(task, id, msg);
	} else {
		switch (service_push_message(P, msg->to, msg)) {
		case 0 :
			// succ
			service_write_receipt(P, id, MESSAGE_RECEIPT_DONE, NULL);
			break;
		case 1 :
			service_write_receipt(P, id, MESSAGE_RECEIPT_BLOCK, msg);
			break;
		default :	// (Dead) -1
			service_write_receipt(P, id, MESSAGE_RECEIPT_ERROR, msg);
			break;
		}
		check_message_to(task, msg->to);
	}
}

// See https://github.com/cloudwu/ltask/issues/21
#define SENDING_BLOCKED 128

struct sending_blocked {
	int n;
	service_id id[SENDING_BLOCKED];
};

static inline int
sending_blocked_check(struct sending_blocked *S, service_id id) {
	if (S->n == 0)
		return 0;	// no blocked service
	if (S->n > SENDING_BLOCKED)
		return 1;	// all blocked
	int i;
	for (i=0;i<S->n;i++) {
		if (id.id == S->id[i].id) {
			return 1;
		}
	}
	return 0;
}

static inline void
sending_blocked_mark(struct sending_blocked *S, service_id id) {
	int index = S->n++;
	if (index == SENDING_BLOCKED)
		return;	// full, block all subsequent messages.
	S->id[index] = id;
}

static void
dispatch_exclusive_sending(struct exclusive_thread *e, struct queue *sending) {
	struct ltask *task = e->task;
	struct service_pool *P = task->services;
	int len = queue_length(sending);
	int i;
	struct sending_blocked S;
	S.n = 0;
	for (i=0;i<len;i++) {
		struct message *msg = (struct message *)queue_pop_ptr(sending);
		if (!sending_blocked_check(&S, msg->to)) {
			// msg->to is not blocked before
			switch (service_push_message(P, msg->to, msg)) {
			case 0 :	// succ
				check_message_to(task, msg->to);
				break;
			case 1 :	// block, push back
				queue_push_ptr(sending, msg);
				// The messages to msg->to is blocked, mark it.
				sending_blocked_mark(&S, msg->to);
				break;
			default :	// dead, delete message
				// todo : report somewhere or release object in message
				message_delete(msg);
				break;
			}
		} else {
			queue_push_ptr(sending, msg);
		}
	}
}

static int
collect_done_job(struct ltask *task, service_id done_job[]) {
	int done_job_n = 0;
	int i;
	const int worker_n = task->config->worker;
	for (i=0;i<worker_n;i++) {
		service_id job = worker_done_job(&task->workers[i]);
		if (job.id) {
			debug_printf(task->logger, "Service %x is done", job.id);
			done_job[done_job_n++] = job;
		}
	}
	return done_job_n;
}

static void
dispath_out_messages(struct ltask *task, const service_id done_job[], int done_job_n) {
	struct service_pool *P = task->services;
	int i;

	for (i=0;i<done_job_n;i++) {
		service_id id = done_job[i];
		int status = service_status_get(P, id);
		if (status == SERVICE_STATUS_DEAD) {
			struct message *msg = service_message_out(P, id);
			assert(msg && msg->to.id == SERVICE_ID_ROOT && msg->type == MESSAGE_SIGNAL);
			switch (service_push_message(P, msg->to, msg)) {
			case 0 :
				// succ
				debug_printf(task->logger, "Signal %x dead to root", id.id);
				check_message_to(task, msg->to);
				break;
			case 1 :
				debug_printf(task->logger, "Root service is blocked, Service %x tries to signal it later", id.id);
				schedule_back(task, id);
				break;
			default:
				debug_printf(task->logger, "Root service is missing");
				service_delete(P, id);
				break;
			}
		} else {
			struct message *msg = service_message_out(P, id);
			if (msg) {
				dispatch_out_message(task, id, msg);
			}
			assert(status == SERVICE_STATUS_DONE);
			if (!service_has_message(P, id)) {
				debug_printf(task->logger, "Service %x is idle", id.id);
				service_status_set(P, id, SERVICE_STATUS_IDLE);
			} else {
				debug_printf(task->logger, "Service %x back to schedule", id.id);
				service_status_set(P, id, SERVICE_STATUS_SCHEDULE);
				schedule_back(task, id);
			}
		}
	}
}

static int
count_freeslot(struct ltask *task) {
	int i;
	int free_slot = 0;
	const int worker_n = task->config->worker;
	for (i=0;i<worker_n;i++) {
		struct worker_thread * w = &task->workers[i];
		if (atomic_int_load(&w->service_ready) == 0) {
			struct binding_service * q = &(w->binding_queue);
			if (q->tail == q->head) {
				++free_slot;
			} else {
				service_id id = q->q[q->head % BINDING_SERVICE_QUEUE];
				++q->head;
				if (q->head == q->tail)
					q->head = q->tail = 0;
				atomic_int_store(&w->service_ready, id.id);
				worker_wakeup(w);
				debug_printf(task->logger, "Assign queue %x to worker %d", assign.id, i);
			}
		}
	}
	return free_slot;
}

static int
prepare_task(struct ltask *task, service_id prepare[], int free_slot) {
	int prepare_n = 0;
	int i;
	for (i=0;i<free_slot;i++) {
		int job = queue_pop_int(task->schedule);
		if (job == 0)	// no more job
			break;
		service_id id = { job };
		int worker = service_binding_get(task->services, id);
		if (worker < 0) {
			// no binding worker
			prepare[prepare_n++] = id;
		} else {
			struct worker_thread * w = &task->workers[worker];
			if (worker_binding_job(w, id)) {
				// worker queue is full
				queue_push_int(task->schedule, job);
			} else {
				id = worker_assign_job(w, id);
				if (id.id != 0) {
					worker_wakeup(w);
					debug_printf(task->logger, "Assign bind %x to worker %d", assign.id, worker);
					--free_slot;
				}
			}
		}
	}
	return prepare_n;
}

static int
assign_prepare_task(struct ltask *task, const service_id prepare[], int prepare_n) {
	int i;
	int assign_job = 0;
	int worker_id = 0;
	const int worker_n = task->config->worker;
	(void)worker_n;
	for (i=0;i<prepare_n;i++) {
		service_id id = prepare[i];
		for (;;) {
			assert(worker_id < worker_n);
			struct worker_thread * w = &task->workers[worker_id++];
			service_id assign = worker_assign_job(w, id);
			if (assign.id != 0) {
				worker_wakeup(w);
				debug_printf(task->logger, "Assign %x to worker %d", assign.id, worker_id-1);
				if (assign.id == id.id) {
					++assign_job;
					break;	
				}
			}
		}
	}
	return assign_job;
}

static void
wakeup_sleeping_workers(struct ltask *task, int jobs) {
	if (jobs == 0)
		return;
	int total_worker = task->config->worker;
	int active_worker = atomic_int_load(&task->active_worker);
	int sleeping_worker = total_worker - active_worker;
	if (sleeping_worker > 0) {
		int wakeup = jobs > sleeping_worker ? sleeping_worker : jobs;
		int i;
		for (i=0;i<total_worker && wakeup > 0;i++) {
			struct worker_thread * w = &task->workers[i];
			if (w->binding.id == 0) {
				wakeup -= worker_wakeup(w);
			}
		}
	}
}

static void
schedule_dispatch(struct ltask *task) {
	// Step 1 : Collect service_done
	service_id done_job[MAX_WORKER];
	int done_job_n = collect_done_job(task, done_job);

	// Step 2: Dispatch out message by service_done
	dispath_out_messages(task, done_job, done_job_n);

	// Step 3: Assign queue task
	int free_slot = count_freeslot(task);

	// Step 4: Assign task to workers

	service_id prepare[MAX_WORKER];
	int prepare_n = prepare_task(task, prepare, free_slot);

	// Step 5

	int assign_job = assign_prepare_task(task, prepare, prepare_n);

	// Step 6
	wakeup_sleeping_workers(task, assign_job);
}

// 0 succ
static int
acquire_scheduler(struct worker_thread * worker) {
	if (atomic_int_load(&worker->task->schedule_owner) == THREAD_NONE) {
		if (atomic_int_cas(&worker->task->schedule_owner, THREAD_NONE, THREAD_WORKER(worker->worker_id))) {
			debug_printf(worker->logger, "Acquire schedule");
			return 0;
		}
	}
	return 1;
}

static void
release_scheduler(struct worker_thread * worker) {
	assert(atomic_int_load(&worker->task->schedule_owner) == THREAD_WORKER(worker->worker_id));
	atomic_int_store(&worker->task->schedule_owner, THREAD_NONE);
	debug_printf(worker->logger, "Release schedule");
}

static void
acquire_scheduler_exclusive(struct exclusive_thread * exclusive) {
	while (!atomic_int_cas(&exclusive->task->schedule_owner, THREAD_NONE, THREAD_EXCLUSIVE(exclusive->thread_id))) {}
	debug_printf(exclusive->logger, "Acquire schedule");
}

static void
release_scheduler_exclusive(struct exclusive_thread * exclusive) {
	assert(atomic_int_load(&exclusive->task->schedule_owner) == THREAD_EXCLUSIVE(exclusive->thread_id));
	atomic_int_store(&exclusive->task->schedule_owner, THREAD_NONE);
	debug_printf(exclusive->logger, "Release schedule");
}

static service_id
steal_job(struct worker_thread * worker) {
	int i;
	for (i=0;i<worker->task->config->worker;i++) {
		service_id job = worker_steal_job(&worker->task->workers[i], worker->task->services);
		if (job.id)
			return job;
	}
	service_id fail = { 0 };
	return fail;
}

// 1 : no job
static int
schedule_dispatch_worker(struct worker_thread *worker) {
	schedule_dispatch(worker->task);
	if (!worker_has_job(worker) && worker->binding.id == 0) {
		service_id job = steal_job(worker);
		if (job.id) {
			debug_printf(worker->logger, "Steal service %x", job.id);
			atomic_int_store(&worker->service_ready, job.id);
		} else {
			return 1;
		}
	}
	return 0;
}

static void
wakeup_all_workers(struct ltask *task) {
	int i;
	for (i=0;i<task->config->worker;i++) {
		worker_wakeup(&task->workers[i]);
	}
}

static void
quit_all_workers(struct ltask *task) {
	int i;
	for (i=0;i<task->config->worker;i++) {
		task->workers[i].term_signal = 1;
	}
}

static void
quit_all_exclusives(struct ltask *task) {
	int i;
	for (i=0;i<MAX_EXCLUSIVE;i++) {
		task->exclusives[i].term_signal = 1;
		sockevent_trigger(&task->exclusives[i].event);
	}
}

static void
crash_log(struct ltask * t, service_id id, int sig) {
	if (t->config->crashlog[0] == 0)
		return;
	char backtrace[4096];
	int n = service_backtrace(t->services, id, backtrace, sizeof(backtrace));
	if (n == 0)
		return;
#if defined(_MSC_VER)
	FILE *f = fopen(t->config->crashlog, "wb");
	if (f == NULL) {
		return;
	}
	const char *signame = sig_name(sig);
	fwrite(signame, 1, strlen(signame), f);
	fwrite("\n", 1, 1, f);
	fwrite(backtrace, 1, n, f);
	fclose(f);
#else
	int fd = open(t->config->crashlog, O_WRONLY | O_CREAT , 0660);
	if (fd < 0) {
		return;
	}
	const char *signame = sig_name(sig);
	write(fd, signame, strlen(signame));
	write(fd, "\n", 1);
	write(fd, backtrace, n);
	close(fd);
#endif
}

static void
crash_log_exclusive(int sig, void *ud) {
	struct exclusive_thread *e = (struct exclusive_thread *)ud;
	crash_log(e->task, e->service, sig);
	exit(1);
}

static void
crash_log_worker(int sig, void *ud) {
	struct worker_thread *w = (struct worker_thread *)ud;
	crash_log(w->task, w->running, sig);
	exit(1);
}

static void
crash_log_default(int sig, void *ud) {
	const char * filename = (const char *)ud;
#if defined(_MSC_VER)
	FILE *f = fopen(filename, "wb");
	if (f == NULL) {
		return;
	}
	const char *signame = sig_name(sig);
	fwrite(signame, 1, strlen(signame), f);
	fclose(f);
#else
	int fd = open(filename, O_WRONLY | O_CREAT , 0660);
	if (fd < 0) {
		return;
	}
	const char *signame = sig_name(sig);
	write(fd, signame, strlen(signame));
	close(fd);
#endif
}

static void
thread_worker(void *ud) {
	struct worker_thread * w = (struct worker_thread *)ud;
	struct service_pool * P = w->task->services;
	worker_timelog_init(w);
	atomic_int_inc(&w->task->active_worker);
	thread_setnamef("ltask!worker-%02d", w->worker_id);

	int thread_id = THREAD_WORKER(w->worker_id);

	sig_register(crash_log_worker, w);

	for (;;) {
		if (w->term_signal) {
			// quit
			break;
		}
		service_id id = worker_get_job(w);
		if (id.id) {
			w->running = id;
			int status = service_status_get(P, id);
			if (status != SERVICE_STATUS_DEAD) {
				debug_printf(w->logger, "Run service %x", id.id);
				assert(status == SERVICE_STATUS_SCHEDULE);
				service_status_set(P, id, SERVICE_STATUS_RUNNING);
				worker_timelog(w, id.id);
				if (service_resume(P, id, thread_id)) {
					worker_timelog(w, id.id);
					debug_printf(w->logger, "Service %x quit", id.id);
					service_status_set(P, id, SERVICE_STATUS_DEAD);
					if (id.id == SERVICE_ID_ROOT) {
						debug_printf(w->logger, "Root quit");
						// root quit, wakeup others
						quit_all_workers(w->task);
						quit_all_exclusives(w->task);
						wakeup_all_workers(w->task);
						break;
					} else {
						service_send_signal(P, id);
					}
				} else {
					worker_timelog(w, id.id);
					service_status_set(P, id, SERVICE_STATUS_DONE);
				}
			} else {
				debug_printf(w->logger, "Service %x is dead", id.id);
			}

			while (worker_complete_job(w)) {
				// Can't complete (running -> done)
				if (!acquire_scheduler(w)) {
					if (worker_complete_job(w)) {
						// Do it self
						schedule_dispatch(w->task);
						while (worker_complete_job(w)) {}	// CAS may fail spuriously
					}
					if (service_binding_get(P, id) == w->worker_id) {
						w->binding = id;
					} else {
						w->binding.id = 0;
					}
					schedule_dispatch_worker(w);
					release_scheduler(w);
					break;
				}
			}
		} else {
			// No job, try to acquire scheduler to find a job
			int nojob = 1;
			if (!acquire_scheduler(w)) {
				nojob = schedule_dispatch_worker(w);
				release_scheduler(w);
			}
			if (nojob) {
				// go to sleep
				atomic_int_dec(&w->task->active_worker);
				debug_printf(w->logger, "Sleeping (%d)", atomic_int_load(&w->task->active_worker));
				worker_timelog(w, -1);
				worker_sleep(w);
				worker_timelog(w, -1);
				atomic_int_inc(&w->task->active_worker);
				debug_printf(w->logger, "Wakeup");
			}
		}
	}
	worker_quit(w);
	atomic_int_dec(&w->task->thread_count);
	debug_printf(w->logger, "Quit");
}

static void
exclusive_message(struct exclusive_thread *e) {
	struct service_pool * P = e->task->services;
	int queue_len = queue_length(e->sending);
	service_id id = e->service;
	struct message *message_out = service_message_out(P, id);
	if (message_out || queue_len > 0) {
		acquire_scheduler_exclusive(e);
		if (message_out) {
			dispatch_out_message(e->task, id, message_out);
		}
		dispatch_exclusive_sending(e, e->sending);
		schedule_dispatch(e->task);
		release_scheduler_exclusive(e);
	}
}

static void
thread_exclusive(void *ud) {
	struct exclusive_thread *e = (struct exclusive_thread *)ud;
	struct service_pool * P = e->task->services;
	service_id id = e->service;
	int thread_id = THREAD_EXCLUSIVE(e->thread_id);
	thread_setnamef("ltask!%s", service_getlabel(P, id));
	sig_register(crash_log_exclusive, e);

	while (!e->term_signal) {
		if (service_resume(P, id, thread_id)) {
			// Resume error : quit
			break;
		}
		exclusive_message(e);
	}
	debug_printf(e->logger, "Quit");
	atomic_int_dec(&e->task->thread_count);
	sockevent_close(&e->event);
}

static int
lexclusive_scheduling_(lua_State *L) {
	struct exclusive_thread *e = (struct exclusive_thread *)lua_touserdata(L, lua_upvalueindex(1));
	exclusive_message(e);
	return 0;
}

static int
ltask_init(lua_State *L) {
	if (lua_getfield(L, LUA_REGISTRYINDEX, "LTASK_CONFIG") != LUA_TNIL) {
		return luaL_error(L, "Already init");
	}
	lua_pop(L, 1);
	struct ltask_config * config = (struct ltask_config *)lua_newuserdatauv(L, sizeof(*config), 0);
	lua_setfield(L, LUA_REGISTRYINDEX, "LTASK_CONFIG");

	config_load(L, 1, config);

	if (config->crashlog[0]) {
		static char filename[sizeof(config->crashlog)];
		memcpy(filename, config->crashlog, sizeof(config->crashlog));
		sig_register_default(crash_log_default, filename);
	}

	struct ltask *task = (struct ltask *)lua_newuserdatauv(L, sizeof(*task), 0);
	lua_setfield(L, LUA_REGISTRYINDEX, "LTASK_GLOBAL");

	task->lqueue = logqueue_new();
#ifdef DEBUGLOG
	task->logger = dlog_new("SCHEDULE", -1);
#endif
	task->config = config;
	task->workers = (struct worker_thread *)lua_newuserdatauv(L, config->worker * sizeof(struct worker_thread), 0);
	lua_setfield(L, LUA_REGISTRYINDEX, "LTASK_WORKERS");
	task->services = service_create(config);
	task->schedule = queue_new_int(config->max_service);
	task->timer = NULL;

#ifdef DEBUGLOG
	if (lua_getfield(L, 1, "debuglog") == LUA_TSTRING) {
		const char *logfile = lua_tostring(L, -1);
		if (logfile[0] == '=') {
			task->logfile = stdout;
		} else {
			task->logfile = fopen(logfile, "w");
		}
	} else {
		task->logfile = NULL;
	}
	lua_pop(L, 1);
#else
	task->logfile = NULL;
#endif

	int i;
	for (i=0;i<config->worker;i++) {
		worker_init(&task->workers[i], task, i);
	}

	atomic_int_init(&task->schedule_owner, THREAD_NONE);
	atomic_int_init(&task->active_worker, 0);
	atomic_int_init(&task->thread_count, 0);

	for (i=0;i<MAX_EXCLUSIVE;i++) {
		task->exclusives[i].task = NULL;
		task->exclusives[i].sending = NULL;
		sockevent_init(&task->exclusives[i].event);
	}

	return 1;
}

static void *
get_ptr(lua_State *L, const char *key) {
	if (lua_getfield(L, LUA_REGISTRYINDEX, key) == LUA_TNIL) {
		luaL_error(L, "%s is absense", key);
		return NULL;
	}
	void * v = lua_touserdata(L, -1);
	if (v == NULL) {
		luaL_error(L, "Invalid %s", key);
		return NULL;
	}
	lua_pop(L, 1);
	return v;
}

static int
exclusive_count(struct ltask *task) {
	int i;
	for (i=0;i<MAX_EXCLUSIVE;i++) {
		if (task->exclusives[i].task == NULL) {
			return i;
		}
	}
	return i;
}

static const char *
get_error_message(lua_State *L) {
	switch (lua_type(L, -1)) {
	case LUA_TLIGHTUSERDATA:
		return (const char *)lua_touserdata(L, -1);
	case LUA_TSTRING:
		return lua_tostring(L, -1);
	default:
		return "Invalid error message";
	}
}

static int
ltask_exclusive(lua_State *L) {
	struct ltask *task = (struct ltask *)get_ptr(L, "LTASK_GLOBAL");
	int ecount = exclusive_count(task);
	if (ecount >= MAX_EXCLUSIVE) {
		return luaL_error(L, "Too many exclusive thread");
	}
	struct exclusive_thread *e = &task->exclusives[ecount];
	e->service.id = luaL_checkinteger(L, 1);
	if (service_status_get(task->services, e->service) != SERVICE_STATUS_IDLE) {
		return luaL_error(L, "Service is uninitialized");
	}
	if (service_requiref(task->services, e->service, "ltask.exclusive", luaopen_ltask_exclusive, L)) {
		return luaL_error(L, "require ltask.exclusive fail : %s", get_error_message(L));
	}
	service_status_set(task->services, e->service, SERVICE_STATUS_EXCLUSIVE);
	e->task = task;
#ifdef DEBUGLOG
	e->logger = dlog_new("EXCLUSIVE", ecount);
#endif
	debug_printf(e->logger, "New service %x", e->service.id);
	e->thread_id = ecount;
	e->term_signal = 0;
	e->sending = queue_new_ptr(task->config->queue_sending);
	service_bind_thread(task->services, e->service, e->thread_id);
	if (ecount+1 < MAX_EXCLUSIVE) {
		e[1].task = NULL;
	}
	return 0;
}

static void
exclusive_release(struct exclusive_thread *ethread) {
	if (ethread->sending) {
		for (;;) {
			struct message *m = queue_pop_ptr(ethread->sending);
			if (m) {
				message_delete(m);
			} else {
				break;
			}
		}
		queue_delete(ethread->sending);
	}
}

static void
close_logger(struct ltask *t) {
	FILE *f = t->logfile;
	dlog_close(f);
	if (f)
		fclose(f);
}

static void
thread_logger(void *ud) {
	struct ltask *t = (struct ltask *)ud;
	FILE *f = t->logfile;
	thread_setname("ltask!logger");
	while (atomic_int_load(&t->thread_count) > 0) {
		dlog_writefile(f);
		sys_sleep(100);	// sleep 0.1s
	}
}

static int
ltask_run(lua_State *L) {
#ifdef DEBUGLOG
	int logthread = 1;
#else
	int logthread = 0;
#endif
	struct ltask *task = (struct ltask *)get_ptr(L, "LTASK_GLOBAL");
	
	int ecount = exclusive_count(task);
	int threads_count = task->config->worker + ecount + logthread;

	struct thread * t = (struct thread *)lua_newuserdatauv(L, threads_count * sizeof(struct thread), 0);
	int i;
	for (i=0;i<ecount;i++) {
		t[i].func = thread_exclusive;
		t[i].ud = (void *)&task->exclusives[i];
	}
	for (i=0;i<task->config->worker;i++) {
		t[ecount + i].func = thread_worker;
		t[ecount + i].ud = (void *)&task->workers[i];
	}
	atomic_int_store(&task->thread_count, threads_count-logthread);
	if (logthread) {
		t[threads_count-1].func = thread_logger;
		t[threads_count-1].ud = (void *)task;
	}
	sig_init();
	thread_join(t, threads_count);
	if (!logthread) {
		close_logger(task);
	}
	logqueue_delete(task->lqueue);
	return 0;
}

static int
ltask_deinit(lua_State *L) {
	struct ltask *task = (struct ltask *)get_ptr(L, "LTASK_GLOBAL");

	int i;
	for (i=0;i<task->config->worker;i++) {
		worker_destory(&task->workers[i]);
	}
	int ecount = exclusive_count(task);
	for (i=0;i<ecount;i++) {
		exclusive_release(&task->exclusives[i]);
	}

	timer_destroy(task->timer);

	service_destory(task->services);
	queue_delete(task->schedule);

	lua_pushnil(L);
	lua_setfield(L, LUA_REGISTRYINDEX, "LTASK_GLOBAL");
	return 0;
}

struct preload_thread {
	lua_State *L;
	void *thread;
	struct service *service;
	atomic_int term;
};

static void
newservice(lua_State *L, struct ltask *task, service_id id, const char *label, const char *filename_source, struct preload_thread *preinit, int worker_id) {
	struct service_ud ud;
	ud.task = task;
	ud.id = id;
	struct service_pool *S = task->services;
	struct service *preS = NULL;

	if (preinit) {
		atomic_int_store(&preinit->term, 1);
		thread_wait(preinit->thread);
		preS = preinit->service;
		free(preinit);
	}

	if (service_init(S, id, (void *)&ud, sizeof(ud), L, preS) || service_requiref(S, id, "ltask", luaopen_ltask, L)) {
		service_delete(S, id);
		luaL_error(L, "New service fail : %s", get_error_message(L));
		return;
	}
	service_binding_set(S, id, worker_id);
	if (service_setlabel(task->services, id, label)) {
		service_delete(S, id);
		luaL_error(L, "set label fail");
		return;
	}
	if (filename_source) {
		const char * err = NULL;
		if (filename_source[0] == '@') {
			err = service_loadfile(S, id, filename_source+1);
		} else {
			err = service_loadstring(S, id, filename_source);
		}
		if (err) {
			lua_pushstring(L, err);
			service_delete(S, id);
			lua_error(L);
		}
	}
}

static void
preinit_thread(void *args) {
	struct preload_thread * t = (struct preload_thread *)args;
	lua_State *L = t->L;
	while (!atomic_int_load(&t->term)) {
		if (L) {
			int result = 0;
			lua_pushboolean(L, 1);
			int r = lua_resume(L, NULL, 1, &result);
			if (r != LUA_YIELD) {
				if (r != LUA_OK) {
					if (!lua_checkstack(L, LUA_MINSTACK)) {
						lua_writestringerror("%s\n", lua_tostring(L, -1));
						lua_pop(L, 1);
					} else {
						lua_pushfstring(L, "Preinit error: %s", lua_tostring(L, -1));
						luaL_traceback(L, L, lua_tostring(L, -1), 0);
						lua_writestringerror("%s\n", lua_tostring(L, -1));
						lua_pop(L, 2);
					}
				}
				L = NULL;
			} else {
				lua_pop(L, result);
			}
		} else {
			sys_sleep(1);
		}
	}
}

static int
ltask_preinit(lua_State *L) {
	struct preload_thread * p = (struct preload_thread *)malloc(sizeof(*p));
	p->L = NULL;
	p->thread = NULL;
	p->service = NULL;
	atomic_int_init(&p->term, 0);
	const char * source = luaL_checkstring(L, 1);
	p->service = service_preinit((void *)L, source);
	p->L = service_preinit_L(p->service);

	struct thread th;
	th.func = preinit_thread;
	th.ud = (void *)p;

	p->thread = thread_run(th);

	lua_pushlightuserdata(L, (void *)p);

	return 1;
}

static int
ltask_newservice(lua_State *L) {
	struct ltask *task = (struct ltask *)get_ptr(L, "LTASK_GLOBAL");
	const char *label = luaL_checkstring(L, 1);
	const char *filename_source = luaL_checkstring(L, 2);
	unsigned int sid = luaL_optinteger(L, 3, 0);
	int worker_id = luaL_optinteger(L, 4, -1);

	service_id id = service_new(task->services, sid);
	newservice(L, task, id, label, filename_source, NULL, worker_id);
	lua_pushinteger(L, id.id);
	return 1;
}

static int
ltask_newservice_preinit(lua_State *L) {
	struct ltask *task = (struct ltask *)get_ptr(L, "LTASK_GLOBAL");
	const char *label = luaL_checkstring(L, 1);
	unsigned int sid = luaL_checkinteger(L, 2);
	luaL_checktype(L, 3, LUA_TLIGHTUSERDATA);
	struct preload_thread *preload = (struct preload_thread *)lua_touserdata(L, 3);
	int worker_id = luaL_optinteger(L, 4, -1);

	service_id id = service_new(task->services, sid);
	newservice(L, task, id, label, NULL, preload, worker_id);
	lua_pushinteger(L, id.id);
	return 1;
}

static lua_Integer
checkfield(lua_State *L, int index, const char *key) {
	if (lua_getfield(L, index, key) != LUA_TNUMBER) {
		return luaL_error(L, ".%s should be an integer", key);
	}
	lua_Integer v = lua_tointeger(L, -1);
	lua_pop(L, 1);
	return v;
}

static int
lpost_message(lua_State *L) {
	luaL_checktype(L, 1, LUA_TTABLE);
	struct message msg;
	msg.from.id = checkfield(L, 1, "from");
	msg.to.id = checkfield(L, 1, "to");
	msg.session = (session_t)checkfield(L, 1, "session");
	msg.type = checkfield(L, 1, "type");
	int t = lua_getfield(L, 1, "message");
	if (t == LUA_TNIL) {
		msg.msg = NULL;
		msg.sz = 0;
	} else {
		if (t != LUA_TLIGHTUSERDATA) {
			return luaL_error(L, ".message should be a pointer");
		}
		msg.msg = lua_touserdata(L, -1);
		lua_pop(L, 1);
		msg.sz = checkfield(L, 1, "size");
	}
	struct ltask *task = (struct ltask *)get_ptr(L, "LTASK_GLOBAL");
	struct message * m = message_new(&msg);
	if (service_push_message(task->services, msg.to, m)) {
		message_delete(m);
		return luaL_error(L, "push message failed");
	}
	check_message_to(task, msg.to);
	return 0;
}

static int
ltask_init_timer(lua_State *L) {
	struct ltask *task = (struct ltask *)get_ptr(L, "LTASK_GLOBAL");
	if (task->timer)
		return luaL_error(L, "Timer can init only once");
	task->timer = timer_init();

	return 0;
}

static int
ltask_init_root(lua_State *L) {
	struct ltask *task = (struct ltask *)get_ptr(L, "LTASK_GLOBAL");
	service_id id = { luaL_checkinteger(L, 1) };
	if (id.id != SERVICE_ID_ROOT) {
		return luaL_error(L, "Id should be ROOT(1)");
	}
	if (service_requiref(task->services, id, "ltask.root", luaopen_ltask_root, L)) {
		return luaL_error(L, "require ltask.root fail : %s", get_error_message(L));
	}
	return 0;
}

static int
pushlog(struct ltask* task, service_id id, void *data, uint32_t sz) {
	struct logmessage msg;
	msg.id = id;
	msg.msg = data;
	msg.sz = sz;
	struct timer *TI = task->timer;
	struct logqueue *q = task->lqueue;
	if (TI == NULL) {
		msg.timestamp = 0;
	} else {
		msg.timestamp = timer_now(TI);
	}
	return logqueue_push(q, &msg);
}

static int
ltask_boot_pushlog(lua_State *L) {
	luaL_checktype(L, 1, LUA_TLIGHTUSERDATA);
	struct ltask *task = (struct ltask *)get_ptr(L, "LTASK_GLOBAL");
	service_id system_id = { SERVICE_ID_SYSTEM };
	void* data = lua_touserdata(L, 1);
	uint32_t sz = (uint32_t)luaL_checkinteger(L, 2);
	if (pushlog(task, system_id, data, sz)) {
		return luaL_error(L, "log error");
	}
	return 0;
}

static int
ltask_init_socket(lua_State *L) {
	sockevent_initsocket();
	return 0;
}

LUAMOD_API int
luaopen_ltask_bootstrap(lua_State *L) {
	static atomic_int init = 0;
	if (atomic_int_inc(&init) != 1) {
		return luaL_error(L, "ltask.bootstrap can only require once");
	}
	luaL_checkversion(L);
	luaL_Reg l[] = {
		{ "init", ltask_init },
		{ "deinit", ltask_deinit },
		{ "run", ltask_run },
		{ "new_thread", ltask_exclusive },
		{ "post_message", lpost_message },
		{ "new_service", ltask_newservice },
		{ "init_timer", ltask_init_timer },
		{ "init_root", ltask_init_root },
		{ "init_socket", ltask_init_socket },
		{ "pushlog", ltask_boot_pushlog },
		{ "preinit", ltask_preinit },
		{ "new_service_preinit", ltask_newservice_preinit },
		{ NULL, NULL },
	};
	
	luaL_newlib(L, l);
	return 1;
}

static inline const struct service_ud *
getS(lua_State *L) {
	if (lua_getfield(L, LUA_REGISTRYINDEX, LTASK_KEY) != LUA_TSTRING) {
		luaL_error(L, "No service id, the VM is not inited by ltask");
	}
	const struct service_ud * ud = (const struct service_ud *)luaL_checkstring(L, -1);
	lua_pop(L, 1);
	return ud;
}

static inline const struct service_ud *
getSinit(lua_State *L) {
	if (lua_getfield(L, LUA_REGISTRYINDEX, LTASK_KEY) != LUA_TSTRING) {
		lua_pop(L, 1);
		return NULL;
	}
	const struct service_ud * ud = (const struct service_ud *)luaL_checkstring(L, -1);
	lua_pop(L, 1);
	return ud;
}

static inline const struct service_ud *
getSup(lua_State *L) {
	const struct service_ud * ud = (const struct service_ud *)lua_touserdata(L, lua_upvalueindex(1));
	return ud;
}

static inline const struct service_ud *
getSdelay(lua_State *L) {
	const struct service_ud *S = getSup(L);
	if (S == NULL) {
		S = getS(L);
		lua_pushlightuserdata(L, (void *)S);
		lua_replace(L, lua_upvalueindex(1));
	}
	return S;
}

// Timer

struct timer_event {
	session_t session;
	service_id id;
};

static int
ltask_timer_add(lua_State *L) {
	const struct service_ud *S = getS(L);
	struct timer *t = S->task->timer;
	if (t == NULL)
		return luaL_error(L, "Init timer before bootstrap");

	struct timer_event ev;
	ev.session = luaL_checkinteger(L, 1);
	ev.id = S->id;
	lua_Integer ti = luaL_checkinteger(L, 2);
	if (ti < 0 || ti != (int)ti)
		return luaL_error(L, "Invalid timer %d", ti);

	timer_add(t, &ev, sizeof(ev), ti);
	return 0;
}

struct timer_execute {
	lua_State *L;
	service_id from;
	struct queue *sending;
	int blocked;
};

static void
execute_timer(void *ud, void *arg) {
	struct timer_execute *te = (struct timer_execute *)ud;
	struct timer_event *event = (arg);

	if (te->blocked > 0) {
		lua_State *L = te->L;
		lua_pushinteger(L, event->session);
		lua_rawseti(L, -2, te->blocked*2+1);
		lua_pushinteger(L, event->id.id);
		lua_rawseti(L, -2, te->blocked*2+2);
		++te->blocked;
	} else {
		struct message m;
		m.from = te->from;
		m.to = event->id;
		m.session = event->session;
		m.type = MESSAGE_RESPONSE;
		m.msg = NULL;
		m.sz = 0;
		struct message *msg = message_new(&m);
		if (queue_push_ptr(te->sending, (void *)msg)) {
			// too many messages, blocked
			message_delete(msg);
			lua_State *L = te->L;
			if (lua_istable(L, 1)) {
				int n = lua_rawlen(L, 1);
				te->blocked = n/2 + 1;
			} else {
				lua_newtable(L);
				te->blocked = 1;
			}
			lua_pushinteger(L, event->session);
			lua_rawseti(L, -2, te->blocked * 2 - 1);
			lua_pushinteger(L, event->id.id);
			lua_rawseti(L, -2 , te->blocked * 2 - 0);
		}
	}
}

static int
lexclusive_timer_update(lua_State *L) {
	const struct service_ud *S = getS(L);
	struct timer *t = S->task->timer;
	if (t == NULL)
		return luaL_error(L, "Init timer before bootstrap");
	if (lua_gettop(L) > 1) {
		lua_settop(L, 1);
		luaL_checktype(L, 1, LUA_TTABLE);
	}

	int ethread = service_thread_id(S->task->services, S->id);
	struct exclusive_thread *thr = get_exclusive_thread(S->task, ethread);
	if (thr == NULL)
		return luaL_error(L, "Not in exclusive service");
	struct timer_execute te;
	te.L = L;
	te.sending = thr->sending;
	te.blocked = 0;
	te.from = S->id;

	timer_update(t, execute_timer, &te);

	if (te.blocked > 0) {
		return 1;
	}

	return 0;
}

static struct message *
gen_send_message(lua_State *L, service_id id) {
	struct message m;
	m.from = id;
	m.to.id = luaL_checkinteger(L, 1);
	m.session = (session_t)luaL_checkinteger(L, 2);
	m.type = luaL_checkinteger(L, 3);
	if (lua_isnoneornil(L, 4)) {
		m.msg = NULL;
		m.sz = 0;
	} else {
		luaL_checktype(L, 4, LUA_TLIGHTUSERDATA);
		m.msg = lua_touserdata(L, 4);
		m.sz = (size_t)luaL_checkinteger(L, 5);
	}

	return message_new(&m);
}

/*
	integer to
	integer session 
	integer type
	pointer message
	integer sz
 */
static inline int
lsend_message_(lua_State *L, const struct service_ud *S) {
	struct message *msg = gen_send_message(L, S->id);
	if (!lua_isyieldable(L)) {
		message_delete(msg);
		return luaL_error(L, "Can't send message in none-yieldable context");
	}
	if (service_send_message(S->task->services, S->id, msg)) {
		// error
		message_delete(msg);
		return luaL_error(L, "Can't send message");
	}
	return 0;
}

static inline int
lsend_message(lua_State *L) {
	return lsend_message_(L, getSup(L));
}

static inline int
lsend_message_delay(lua_State *L) {
	return lsend_message_(L, getSdelay(L));
}

static void
acquire_scheduler_by_id(struct ltask * task, int thread_id) {
	if (thread_id < MAX_EXCLUSIVE) {
		struct exclusive_thread * e = &task->exclusives[thread_id];
		acquire_scheduler_exclusive(e);
	} else {
		thread_id -= MAX_EXCLUSIVE;
		assert(thread_id < task->config->worker);
		struct worker_thread *w = &task->workers[thread_id];
		while (acquire_scheduler(w)) {}
	}
}

static void
release_scheduler_by_id(struct ltask * task, int thread_id) {
	if (thread_id < MAX_EXCLUSIVE) {
		struct exclusive_thread * e = &task->exclusives[thread_id];
		release_scheduler_exclusive(e);
	} else {
		thread_id -= MAX_EXCLUSIVE;
		assert(thread_id < task->config->worker);
		struct worker_thread *w = &task->workers[thread_id];
		release_scheduler(w);
	}
}

static inline int
lsend_message_direct_(lua_State *L, const struct service_ud *S) {
	struct message *msg = gen_send_message(L, S->id);
	struct ltask * task = S->task;

	int thread = service_thread_id(task->services, S->id);
	if (thread < 0)
		return luaL_error(L, "Invalid thread id %d", thread);
	acquire_scheduler_by_id(task, thread);

	int r = service_push_message(task->services, msg->to, msg);
	if (r) {
		release_scheduler_by_id(task, thread);
		message_delete(msg);
		if (r > 0) {
			r = MESSAGE_RECEIPT_BLOCK;
		} else {
			r = MESSAGE_RECEIPT_ERROR;
		}
		lua_pushinteger(L, r);
		return 1;
	}
	check_message_to(task, msg->to);
	release_scheduler_by_id(task, thread);
	lua_pushinteger(L, MESSAGE_RECEIPT_DONE);
	return 1;
}

static inline int
lsend_message_direct(lua_State *L) {
	return lsend_message_direct_(L, getSup(L));
}

static inline int
lsend_message_direct_delay(lua_State *L) {
	return lsend_message_direct_(L, getSdelay(L));
}

static inline int
lexclusive_send_message_(lua_State *L, const struct service_ud *S) {
	int ethread = service_thread_id(S->task->services, S->id);
	struct exclusive_thread *thr = get_exclusive_thread(S->task, ethread);
	if (thr == NULL)
		return luaL_error(L, "%d is not in exclusive thread", S->id.id);
	struct message *msg = gen_send_message(L, S->id);
	if (queue_push_ptr(thr->sending, msg)) {
		// sending queue is full
		msg->msg = NULL;
		msg->sz = 0;
		message_delete(msg);
		return 0;
	}
	lua_pushboolean(L, 1);
	return 1;
}

static int
lexclusive_send_message(lua_State *L) {
	return lexclusive_send_message_(L, getSup(L));
}

static int
lexclusive_send_message_delay(lua_State *L) {
	return lexclusive_send_message_(L, getSdelay(L));
}

static inline int
lrecv_message_(lua_State *L, const struct service_ud *S) {
	struct message *m = service_pop_message(S->task->services, S->id);
	if (m == NULL)
		return 0;
	int r = 3;
	lua_pushinteger(L, m->from.id);
	lua_pushinteger(L, m->session);
	lua_pushinteger(L, m->type);
	if (m->msg) {
		lua_pushlightuserdata(L, m->msg);
		lua_pushinteger(L, m->sz);
		r += 2;
		m->msg = NULL;
		m->sz = 0;
	}
	message_delete(m);

	return r;
}

static int
lrecv_message(lua_State *L) {
	return lrecv_message_(L, getSup(L));
}

static int
lrecv_message_delay(lua_State *L) {
	return lrecv_message_(L, getSdelay(L));
}

static inline int
lmessage_receipt_(lua_State *L, const struct service_ud *S) {
	int receipt;
	struct message *m = service_read_receipt(S->task->services, S->id, &receipt);
	if (receipt == MESSAGE_RECEIPT_NONE) {
		return luaL_error(L, "No receipt");
	}
	lua_pushinteger(L, receipt);
	if (m == NULL)
		return 1;
	if (receipt == MESSAGE_RECEIPT_RESPONCE) {
		// Only for schedule message NEW
		lua_pushinteger(L, m->to.id);
		message_delete(m);
		return 2;
	}
	if (m->msg) {
		lua_pushlightuserdata(L, m->msg);
		lua_pushinteger(L, m->sz);
		m->msg = NULL;
		m->sz = 0;
		message_delete(m);
		return 3;
	} else {
		message_delete(m);
		return 1;
	}
}

static int
lmessage_receipt(lua_State *L) {
	return lmessage_receipt_(L, getSup(L));
}

static int
lmessage_receipt_delay(lua_State *L) {
	return lmessage_receipt_(L, getSdelay(L));
}

static int
luaseri_remove(lua_State *L) {
	if (lua_isnoneornil(L, 1))
		return 0;
	luaL_checktype(L, 1, LUA_TLIGHTUSERDATA);
	void * data = lua_touserdata(L, 1);
	size_t sz = luaL_checkinteger(L, 2);
	(void)sz;
	free(data);
	return 0;
}

static int
lexclusive_sleep(lua_State *L) {
	lua_Integer csec = luaL_optinteger(L, 1, 0);
	sys_sleep(csec);
	return 0;
}

static int
lself(lua_State *L) {
	const struct service_ud *S = getS(L);
	lua_pushinteger(L, S->id.id);

	return 1;
}

static int
lworker_id(lua_State *L) {
	const struct service_ud *S = getS(L);
	int worker = get_worker_id(S->task, S->id);
	if (worker >= 0) {
		lua_pushinteger(L, worker);
		return 1;
	}
	return 0;
}

static int
lworker_bind(lua_State *L) {
	const struct service_ud *S = getS(L);
	if (lua_isnoneornil(L, 1)) {
		// unbind
		service_binding_set(S->task->services, S->id, -1);
		return 0;
	}
	int	worker = luaL_checkinteger(L, 1);
	if (worker < 0 || worker >= S->task->config->worker) {
		return luaL_error(L, "Invalid worker id %d", worker);
	}
	service_binding_set(S->task->services, S->id, worker);
	return 0;
}

static int
ltask_now(lua_State *L) {
	const struct service_ud *S = getS(L);
	struct timer *TI = S->task->timer;
	if (TI == NULL) {
		return luaL_error(L, "Init timer before bootstrap");
	}
	uint32_t start = timer_starttime(TI);
	uint64_t now = timer_now(TI);
	lua_pushinteger(L, start + now / 100);
	lua_pushinteger(L, (uint64_t)start * 100 + now);
	return 2;
}

static int
ltask_counter(lua_State *L) {
	uint64_t freq = lua_tointeger(L, lua_upvalueindex(1));
	uint64_t ti = systime_counter();
	lua_pushnumber(L, (double)ti / freq);
	return 1;
}

static int
ltask_walltime(lua_State *L) {
	uint64_t ti = systime_wall();
	lua_pushinteger(L, ti);
	return 1;
}

static int
ltask_pushlog(lua_State *L) {
	luaL_checktype(L, 1, LUA_TLIGHTUSERDATA);
	void* data = lua_touserdata(L, 1);
	uint32_t sz = (uint32_t)luaL_checkinteger(L, 2);
	const struct service_ud *S = getS(L);
	if (pushlog(S->task, S->id, data, sz)) {
		return luaL_error(L, "log error");
	}
	return 0;
}

static int
ltask_poplog(lua_State *L) {
	struct logmessage msg;
	const struct service_ud *S = getS(L);
	struct logqueue *q = S->task->lqueue;
	struct timer *TI = S->task->timer;
	uint64_t start = 0;
	if (TI) {
		start = (uint64_t)timer_starttime(TI) * 100;
	}
	if (logqueue_pop(q, &msg))
		return 0;
	lua_pushinteger(L, msg.timestamp + start);
	lua_pushinteger(L, msg.id.id);
	lua_pushlightuserdata(L, msg.msg);
	lua_pushinteger(L, msg.sz);
	return 4;
}

static int
ltask_get_pushlog(lua_State *L) {
	const struct service_ud *S = getS(L);
	lua_pushlightuserdata(L, pushlog);
	lua_pushlightuserdata(L, S->task);
	return 2;
}

static int
ltask_memlimit(lua_State *L) {
	const struct service_ud *S = getS(L);
	size_t limit = luaL_checkinteger(L, 1);
	size_t last_limit = service_memlimit(S->task->services, S->id, limit);
	lua_pushinteger(L, last_limit);
	return 1;
}

static int
ltask_memcount(lua_State *L) {
	const struct service_ud *S = getS(L);
	static int type[] = {
		LUA_TSTRING,
		LUA_TTABLE,
		LUA_TFUNCTION,
		LUA_TUSERDATA,
		LUA_TTHREAD,
	};
	const int ntype = sizeof(type)/sizeof(type[0]);
	if (!lua_istable(L, 1)) {
		lua_settop(L, 0);
		lua_createtable(L, 0, ntype);
	}
	int i;
	for (i=0;i<ntype;i++) {
		size_t c = service_memcount(S->task->services, S->id, type[i]);
		lua_pushinteger(L, c);
		lua_setfield(L, 1, lua_typename(L, type[i]));
	}
	return 1;
}

static int
ltask_label(lua_State *L) {
	const struct service_ud *S = getS(L);
	lua_pushstring(L, service_getlabel(S->task->services, S->id));
	return 1;
}

static int
ltask_touch_service(lua_State *L) {
	int id = luaL_checkinteger(L, 1);
	service_id to = { id };
	const struct service_ud *S = getS(L);
	int ethread = service_thread_id(S->task->services, to);
	struct exclusive_thread *thr = get_exclusive_thread(S->task, ethread);
	if (thr == NULL)
		return luaL_error(L, "%d is not an exclusive service", id);
	sockevent_trigger(&thr->event);
	return 0;
}

static int
lbacktrace(lua_State *L) {
	const struct service_ud *S = getS(L);
	char buf[4096];
	int n = service_backtrace(S->task->services, S->id, buf, sizeof(buf));
	lua_pushlstring(L, buf, n);
	return 1;
}

static int
ltask_cpucost(lua_State *L) {
	const struct service_ud *S = getS(L);
	uint64_t cpucost = service_cpucost(S->task->services, S->id);
	uint64_t freq = lua_tointeger(L, lua_upvalueindex(1));
	lua_pushnumber(L, (double)cpucost / freq);
	return 1;
}

static int
ltask_isexclusive(lua_State *L) {
	const struct service_ud *S = getS(L);
	service_id sid;
	if (lua_isnoneornil(L, 1)) {
		sid = S->id;
	} else {
		int id = luaL_checkinteger(L, 1);
		sid.id = id;
	}
	int status = service_status_get(S->task->services, sid);
	lua_pushboolean(L, status == SERVICE_STATUS_EXCLUSIVE);
	return 1;
}


#ifdef DEBUGLOG

static int
ltask_debuglog(lua_State *L) {
	const struct service_ud *S = getS(L);
	struct debug_logger *dl = NULL;
	int i;
	for (i=0;i<S->task->config->worker;i++) {
		if (S->task->workers[i].running.id == S->id.id) {
			dl = S->task->workers[i].logger;
			break;
		}
	}
	if (dl == NULL) {
		for (i=0;i<MAX_EXCLUSIVE;i++) {
			if (S->task->exclusives[i].task == NULL)
				break;
			if (S->task->exclusives[i].service.id == S->id.id) {
				dl= S->task->exclusives[i].logger;
				break;
			}
		}
		if (dl == NULL) {
			return luaL_error(L, "Can't find thread");
		}
	}
	dlog_write(dl, "%s", luaL_checkstring(L, 1));
	return 0;
}

#else

static int
ltask_debuglog(lua_State *L) {
	return 0;
}

#endif

LUAMOD_API int
luaopen_ltask(lua_State *L) {
	luaL_checkversion(L);
	luaL_Reg l[] = {
		{ "pack", luaseri_pack },
		{ "unpack", luaseri_unpack },
		{ "remove", luaseri_remove },
		{ "unpack_remove", luaseri_unpack_remove },
		{ "send_message", NULL },
		{ "send_message_direct", NULL },
		{ "recv_message", NULL },
		{ "message_receipt", NULL },
		{ "touch_service", ltask_touch_service },
		{ "self", lself },
		{ "worker_id", lworker_id },
		{ "worker_bind", lworker_bind },
		{ "timer_add", ltask_timer_add },
		{ "now", ltask_now },
		{ "walltime", ltask_walltime },
		{ "pushlog", ltask_pushlog },
		{ "poplog", ltask_poplog },
		{ "get_pushlog", ltask_get_pushlog },
		{ "mem_limit", ltask_memlimit },
		{ "mem_count", ltask_memcount },
		{ "label", ltask_label },
		{ "backtrace", lbacktrace },
		{ "counter", NULL },
		{ "cpucost", NULL },
		{ "is_exclusive", ltask_isexclusive },
		{ "debuglog", ltask_debuglog },
		{ NULL, NULL },
	};

	luaL_newlib(L, l);

	const struct service_ud *S = getSinit(L);
	if (S) {
		luaL_Reg l2[] = {
			{ "send_message", lsend_message },
			{ "send_message_direct", lsend_message_direct },
			{ "recv_message", lrecv_message },
			{ "message_receipt", lmessage_receipt },
			{ NULL, NULL },
		};

		lua_pushlightuserdata(L, (void *)S);
		luaL_setfuncs(L, l2, 1);
	} else {
		luaL_Reg l2[] = {
			{ "send_message", lsend_message_delay },
			{ "send_message_direct", lsend_message_direct_delay },
			{ "recv_message", lrecv_message_delay },
			{ "message_receipt", lmessage_receipt_delay },
			{ NULL, NULL },
		};

		lua_pushlightuserdata(L, NULL);
		luaL_setfuncs(L, l2, 1);
	}

	uint64_t f = systime_frequency();
	lua_pushinteger(L, f);
	lua_pushcclosure(L, ltask_counter, 1);
	lua_setfield(L, -2, "counter");

	lua_pushinteger(L, f);
	lua_pushcclosure(L, ltask_cpucost, 1);
	lua_setfield(L, -2, "cpucost");

	sys_init();
	return 1;
}

static int
lexclusive_eventwait_(lua_State *L) {
	struct exclusive_thread *e = (struct exclusive_thread *)lua_touserdata(L, lua_upvalueindex(1));
	int r = sockevent_wait(&e->event);
	lua_pushboolean(L, r > 0);
	return 1;
}

static struct exclusive_thread *
exclusive_ud(lua_State *L) {
	const struct service_ud *S = getS(L);
	int ethread = service_thread_id(S->task->services, S->id);
	struct exclusive_thread *thr = get_exclusive_thread(S->task, ethread);
	if (thr == NULL)
		luaL_error(L, "Not in exclusive service");
	lua_pushlightuserdata(L, (void *)thr);
	return thr;
}

static int
lexclusive_scheduling(lua_State *L) {
	exclusive_ud(L);
	lua_pushcclosure(L, lexclusive_scheduling_, 1);
	return 1;
}

static int
lexclusive_eventinit(lua_State *L) {
	struct exclusive_thread *thr = exclusive_ud(L);
	lua_pushcclosure(L, lexclusive_eventwait_, 1);

	if (sockevent_open(&thr->event) != 0) {
		return luaL_error(L, "Create sockevent fail");
	}
	lua_pushlightuserdata(L, (void *)(intptr_t)sockevent_fd(&thr->event));

	return 2;
}

static int
lexclusive_eventreset(lua_State *L) {
	struct exclusive_thread *thr = exclusive_ud(L);

	sockevent_close(&thr->event);

	if (sockevent_open(&thr->event) != 0) {
		return luaL_error(L, "Reset sockevent fail");
	}
	lua_pushlightuserdata(L, (void *)(intptr_t)sockevent_fd(&thr->event));

	return 1;
}

LUAMOD_API int
luaopen_ltask_exclusive(lua_State *L) {
	luaL_checkversion(L);
	luaL_Reg l[] = {
		{ "send", NULL },
		{ "timer_update", lexclusive_timer_update },
		{ "sleep", lexclusive_sleep },
		{ "scheduling", lexclusive_scheduling },
		{ "eventinit", lexclusive_eventinit },
		{ "eventreset", lexclusive_eventreset },
		{ NULL, NULL },
	};

	luaL_newlib(L, l);

	const struct service_ud *S = getSinit(L);
	if (S) {
		lua_pushlightuserdata(L, (void *)S);
		lua_pushcclosure(L, lexclusive_send_message, 1);
	} else {
		lua_pushlightuserdata(L, NULL);
		lua_pushcclosure(L, lexclusive_send_message_delay, 1);
	}
	lua_setfield(L, -2, "send");

	return 1;
}

static int
ltask_initservice(lua_State *L) {
	const struct service_ud *S = getS(L);
	unsigned int sid = luaL_checkinteger(L, 1);
	const char *label = luaL_checkstring(L, 2);
	const char *filename_source = luaL_checkstring(L, 3);
	int worker_id = luaL_optinteger(L, 4, -1);

	service_id id = { sid };
	newservice(L, S->task, id, label, filename_source, NULL, worker_id);

	return 0;
}

static int
close_service_messages(lua_State *L, struct service_pool *P, service_id id) {
	int report_error = 0;
	int index;
	for (;;) {
		struct message * m = service_pop_message(P, id);
		if (m) {
			// todo : response message (error)
			if (m->type == MESSAGE_REQUEST || m->type == MESSAGE_SYSTEM) {
				if (!report_error) {
					lua_newtable(L);
					report_error = 1;
					index = 1;
				}
				lua_pushinteger(L, m->from.id);
				lua_rawseti(L, -2, index++);
				lua_pushinteger(L, m->session);
				lua_rawseti(L, -2, index++);
			}
			message_delete(m);
		} else {
			break;
		}
	}
	return report_error;
}

static int
ltask_closeservice(lua_State *L) {
       const struct service_ud *S = getS(L);
       unsigned int sid = luaL_checkinteger(L, 1);
       service_id id = { sid };
       if (service_status_get(S->task->services, id) != SERVICE_STATUS_DEAD) {
               return luaL_error(L, "Hang %d before close it", sid);
       }
	   int ret = close_service_messages(L, S->task->services, id);
	   service_delete(S->task->services, id);
       return ret;
}

LUAMOD_API int
luaopen_ltask_root(lua_State *L) {
	static atomic_int init = 0;
	if (atomic_int_inc(&init) != 1) {
		return luaL_error(L, "ltask.root can only require once");
	}
	luaL_checkversion(L);
	luaL_Reg l[] = {
		{ "init_service", ltask_initservice },
		{ "close_service", ltask_closeservice },
		{ NULL, NULL },
	};
	
	luaL_newlib(L, l);
	return 1;
}
