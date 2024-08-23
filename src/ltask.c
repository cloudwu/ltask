#define LUA_LIB

//#define DEBUGLOG
//#define TIMELOG

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
LUAMOD_API int luaopen_ltask_root(lua_State *L);

#define THREAD_NONE -1
#define THREAD_WORKER(n) (n)

#ifndef DEBUGLOG

#define debug_printf(logger, fmt, ...)

#else

#define debug_printf dlog_write

#endif

struct ltask {
	const struct ltask_config *config;
	struct worker_thread *workers;
	atomic_int event_init[MAX_SOCKEVENT];
	struct sockevent event[MAX_SOCKEVENT];
	struct service_pool *services;
	struct queue *schedule;
	struct timer *timer;
#ifdef DEBUGLOG
	struct debug_logger *logger;
#endif
	struct logqueue *lqueue;
	struct queue *external_message;
	struct message *external_last_message;
	atomic_int schedule_owner;
	atomic_int active_worker;
	atomic_int thread_count;
	int blocked_service;		// binding service may block
	FILE *logfile;
};

struct service_ud {
	struct ltask *task;
	service_id id;
};

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
	} else {
		int sockid = service_sockevent_get(task->services, to);
		if (sockid >= 0) {
			debug_printf(task->logger, "Trigger sockevent of service %d", to.id);
			sockevent_trigger(&task->event[sockid]);
		}
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

static int
collect_done_job(struct ltask *task, service_id done_job[]) {
	int done_job_n = 0;
	int i;
	const int worker_n = task->config->worker;
	for (i=0;i<worker_n;i++) {
		struct worker_thread * w = &task->workers[i];
		service_id job = worker_done_job(w);
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
				int sockid = service_sockevent_get(P, id);
				if (sockid >= 0) {
					debug_printf(task->logger, "Service %x back to schedule for sockevent", id.id);
					struct message msg;
					msg.from.id = SERVICE_ID_SYSTEM;
					msg.to = id;
					msg.session = 0;
					msg.type = MESSAGE_IDLE;
					msg.msg = NULL;
					msg.sz = 0;
					service_push_message(P, id, message_new(&msg));
					service_status_set(P, id, SERVICE_STATUS_SCHEDULE);
					schedule_back(task, id);
				} else {
					debug_printf(task->logger, "Service %x is idle", id.id);
					service_status_set(P, id, SERVICE_STATUS_IDLE);
				}
			} else {
				debug_printf(task->logger, "Service %x back to schedule", id.id);
				service_status_set(P, id, SERVICE_STATUS_SCHEDULE);
				schedule_back(task, id);
			}
		}
	}
}

static inline void
kick_running(struct worker_thread * w, service_id id) {
	w->task->blocked_service = 1;
	w->waiting = id;	// will kick running later
}

static int
count_freeslot(struct ltask *task) {
	int i;
	int free_slot = 0;
	const int worker_n = task->config->worker;
	for (i=0;i<worker_n;i++) {
		struct worker_thread * w = &task->workers[i];
		if (w->service_ready == 0) {
			struct binding_service * q = &(w->binding_queue);
			if (q->tail == q->head) {
				if (!worker_has_job(w)) {
					++free_slot;
				}
			} else {
				service_id id = q->q[q->head % BINDING_SERVICE_QUEUE];
				++q->head;
				if (q->head == q->tail)
					q->head = q->tail = 0;
				atomic_int_store(&w->service_ready, id.id);
				kick_running(w, id);
				worker_wakeup(w);
				debug_printf(task->logger, "Assign queue %x to worker %d", id.id, i);
			}
		}
	}
	return free_slot;
}

static int
prepare_task(struct ltask *task, service_id prepare[], int free_slot, int prepare_n) {
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
					kick_running(w, id);
					worker_wakeup(w);
					debug_printf(task->logger, "Assign bind %x to worker %d", id.id, worker);
					--free_slot;
				}
			}
		}
	}
	return prepare_n;
}

static void
trigger_blocked_workers(struct ltask *task) {
	if (!task->blocked_service)
		return;
	int i;
	int blocked = 0;
	const int worker_n = task->config->worker;
	for (i=0;i<worker_n;i++) {
		struct worker_thread * w = &task->workers[i];
		if (w->waiting.id != 0) {
			service_id running = w->running;
			if (running.id != 0) {
				// touch service who block the waiting service
				int sockevent_id = service_sockevent_get(task->services, running);
				if (sockevent_id >= 0) {
					sockevent_trigger(&task->event[sockevent_id]);
				}
				w->waiting.id = 0;
			} else {
				// continue waiting for blocked service running
				blocked = 1;
			}
		}
	}
	task->blocked_service = blocked;
}

static void
assign_prepare_task(struct ltask *task, const service_id prepare[], int prepare_n) {
	int i;
	int worker_id = 0;
	const int worker_n = task->config->worker;
	int use_busy = 0;
	int use_binding = 0;

	for (i=0;i<prepare_n;i++) {
		service_id id = prepare[i];
		for (;;) {
			if (worker_id >= worker_n) {
				if (use_busy == 0) {
					use_busy = 1;
					worker_id = 0;
				} else {
					assert(use_binding == 0);
					use_binding = 1;
					worker_id = 0;
				}
			}
			struct worker_thread * w = &task->workers[worker_id++];
			if ((use_busy || !w->busy) && (w->binding.id == 0 || use_binding)) {
				service_id assign = worker_assign_job(w, id);
				if (assign.id != 0) {
					worker_wakeup(w);
					debug_printf(task->logger, "Assign %x to worker %d", assign.id, worker_id-1);
					if (assign.id == id.id) {
						// assign a none-binding service
						break;
					}
				}
			}
		}
	}
}

static int
get_pending_jobs(struct ltask *task, service_id output[]) {
	int i;
	int worker_n = task->config->worker;
	int n = 0;
	struct service_pool * P = task->services;
	for (i=0;i<worker_n;i++) {
		struct worker_thread * w = &task->workers[i];
		if (w->busy) {
			service_id id = worker_steal_job(w, P);
			if (id.id) {
				output[n++] = id;
			}
		}
	}
	return n;
}

static int
send_external_message(struct ltask *task, struct message *msg) {
	switch (service_push_message(task->services, msg->to, msg)) {
	case 0 :
		return 0;
	case 1 :
		return 1;	// block
	default:
		// root dead, drop message
		message_delete(msg);
		return 0;
	}
}

static void
dispatch_external_messages(struct ltask *task) {
	int send = 0;
	if (task->external_last_message) {
		if (send_external_message(task, task->external_last_message))
			return;	// block
		task->external_last_message = NULL;
		send = 1;
	}
	void * msg = NULL;
	while ((msg = queue_pop_ptr(task->external_message))) {
		struct message m;
		m.from.id = 0;
		m.to.id = 1;	// root
		m.session = (session_t)0;	// no response
		m.type = MESSAGE_REQUEST;
		m.msg = seri_packstring("external", 0, msg, &m.sz);

		struct message *em = message_new(&m);
		if (send_external_message(task, em)) {
			// block
			task->external_last_message = em;
			return;
		}
		send = 1;
	}
	if (send) {
		service_id root = {1};
		check_message_to(task, root);
	}
}

static void
schedule_dispatch(struct ltask *task) {
	// Step 0 : dispatch external messsages

	if (task->external_message) {
		dispatch_external_messages(task);
	}

	// Step 1 : Collect service_done
	service_id jobs[MAX_WORKER];

	int done_job_n = collect_done_job(task, jobs);

	// Step 2: Dispatch out message by service_done
	dispath_out_messages(task, jobs, done_job_n);

	// Step 3: get pending jobs
	int job_n = get_pending_jobs(task, jobs);

	// Step 4: Assign queue task
	int free_slot = count_freeslot(task);

	assert(free_slot >= job_n);

	// Step 5: Assign task to workers
	int prepare_n = prepare_task(task, jobs, free_slot - job_n, job_n);

	// Step 6
	assign_prepare_task(task, jobs, prepare_n);

	// Step 7
	trigger_blocked_workers(task);
}

// 0 succ
static int
acquire_scheduler(struct worker_thread * worker) {
	if (atomic_int_cas(&worker->task->schedule_owner, THREAD_NONE, THREAD_WORKER(worker->worker_id))) {
		debug_printf(worker->logger, "Acquire schedule");
#ifdef TIMELOG
		worker->schedule_time = systime_thread();
#endif
		return 0;
	}
	return 1;
}

static void
release_scheduler(struct worker_thread * worker) {
	assert(atomic_int_load(&worker->task->schedule_owner) == THREAD_WORKER(worker->worker_id));
	atomic_int_store(&worker->task->schedule_owner, THREAD_NONE);
#ifdef TIMELOG
	uint64_t t = systime_thread() - worker->schedule_time;
	(void)t;
	debug_printf(worker->logger, "Release schedule %d", (int)t);
#else
	debug_printf(worker->logger, "Release schedule");
#endif
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
	if (!worker_has_job(worker)) {
		// no job to do
		if (worker->binding.id) {
			// bind a service
			return 1;
		} else {
			// steal a job
			service_id job = steal_job(worker);
			if (job.id) {
				debug_printf(worker->logger, "Steal service %x", job.id);
				atomic_int_store(&worker->service_ready, job.id);
			} else {
				// steal fail
				return 1;
			}
		}
	}
	// has a job
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
trigger_all_sockevent(struct ltask *task) {
	int i;
	for (i=0;i<MAX_SOCKEVENT;i++) {
		sockevent_trigger(&task->event[i]);
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
	atomic_int_inc(&w->task->active_worker);
	thread_setnamef("ltask!worker-%02d", w->worker_id);

	sig_register(crash_log_worker, w);

	debug_printf(w->logger, "Start worker %x", w->worker_id);

	for (;;) {
		if (w->term_signal) {
			// quit
			break;
		}
		service_id id = worker_get_job(w);
		int dead = 0;
		if (id.id) {
			w->busy = 1;
			w->running = id;
			if (w->waiting.id == id.id) {
				w->waiting.id = 0;
			}
			int status = service_status_get(P, id);
			if (status != SERVICE_STATUS_DEAD) {
				debug_printf(w->logger, "Run service %x", id.id);
				assert(status == SERVICE_STATUS_SCHEDULE);
				service_status_set(P, id, SERVICE_STATUS_RUNNING);
				if (service_resume(P, id)) {
					dead = 1;
					debug_printf(w->logger, "Service %x quit", id.id);
					service_status_set(P, id, SERVICE_STATUS_DEAD);
					if (id.id == SERVICE_ID_ROOT) {
						debug_printf(w->logger, "Root quit");
						// root quit, wakeup others
						quit_all_workers(w->task);
						trigger_all_sockevent(w->task);
						wakeup_all_workers(w->task);
						break;
					} else {
						service_send_signal(P, id);
					}
				} else {
					service_status_set(P, id, SERVICE_STATUS_DONE);
				}
			} else {
				debug_printf(w->logger, "Service %x is dead", id.id);
			}
			w->busy = 0;

			// check binding

			if (dead) {
				if (w->binding.id == id.id)
					w->binding.id = 0;
			} else if (service_binding_get(P, id) == w->worker_id) {
				w->binding = id;
			}

			while (worker_complete_job(w)) {
				// Can't complete (running -> done)
				if (!acquire_scheduler(w)) {
					if (worker_complete_job(w)) {
						// Do it self
						schedule_dispatch(w->task);
						while (worker_complete_job(w)) {}	// CAS may fail spuriously
					}
					schedule_dispatch_worker(w);
					release_scheduler(w);
					break;
				}
			}
		} else {
			// No job, try to acquire scheduler to find a job
			int nojob = 1;

			do {
				if (!acquire_scheduler(w)) {
					nojob = schedule_dispatch_worker(w);
					release_scheduler(w);
				}
			} while (w->service_done);	// retry if no one clear done flag

			if (nojob && !w->task->blocked_service) {
				// go to sleep
				atomic_int_dec(&w->task->active_worker);
				debug_printf(w->logger, "Sleeping (%d)", w->task->active_worker);
				worker_sleep(w);
				atomic_int_inc(&w->task->active_worker);
				debug_printf(w->logger, "Wakeup");
			}
		}
	}
	worker_quit(w);
	atomic_int_dec(&w->task->thread_count);
	debug_printf(w->logger, "Quit");
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
	task->external_message = NULL;
	task->external_last_message = NULL;
	if (config->external_queue) {
		task->external_message = queue_new_ptr(config->external_queue);
	}

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

	for (i=0;i<MAX_SOCKEVENT;i++) {
		sockevent_init(&task->event[i]);
		atomic_int_init(&task->event_init[i], 0);
	}

	task->blocked_service = 0;

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

struct task_context {
	int logthread;
	int threads_count;
	struct ltask *task;
	void *handle;
	struct thread t[1];
};

static int
ltask_run(lua_State *L) {
#ifdef DEBUGLOG
	int logthread = 1;
#else
	int logthread = 0;
#endif
	struct ltask *task = (struct ltask *)get_ptr(L, "LTASK_GLOBAL");
	int usemainthread = 0;
	int mainthread = -1;
	const int worker_n = task->config->worker;
	if (lua_isinteger(L, 1)) {
		usemainthread = 1;
		mainthread = luaL_checkinteger(L, 1);
		if (mainthread >= 0) {
			if (mainthread >= worker_n) {
				return luaL_error(L, "Invalid mainthread %d", mainthread);
			}
		}
	}

	int threads_count = worker_n + logthread - usemainthread;

	struct task_context *ctx = (struct task_context *)lua_newuserdatauv(L, sizeof(*ctx) + (threads_count-1+usemainthread) * sizeof(struct thread), 0);

	ctx->logthread = logthread;
	ctx->threads_count = threads_count;
	ctx->task = task;
	struct thread * t = ctx->t;
	int i;
	for (i=0;i<worker_n;i++) {
		t[i].func = thread_worker;
		t[i].ud = (void *)&task->workers[i];
	}
	task->thread_count = worker_n;
	if (logthread) {
		int logthread_index = threads_count-1;
		t[logthread_index].func = thread_logger;
		t[logthread_index].ud = (void *)task;
	}
	sig_init();
	if (usemainthread && mainthread >= 0) {
		struct thread tmp = t[mainthread];
		t[mainthread] = t[0];
		t[0] = tmp;
	}

	ctx->handle = thread_start(ctx->t, ctx->threads_count, usemainthread);
	return 1;
}

static int
ltask_wait(lua_State *L) {
	luaL_checktype(L, 1, LUA_TUSERDATA);
	struct task_context *ctx = (struct task_context *)lua_touserdata(L, 1);
	thread_join(ctx->handle, ctx->threads_count);
	if (ctx->logthread) {
		close_logger(ctx->task);
	}
	logqueue_delete(ctx->task->lqueue);
	int i;
	for (i=0;i<MAX_SOCKEVENT;i++) {
		sockevent_close(&ctx->task->event[i]);
	}
	message_delete(ctx->task->external_last_message);
	queue_delete(ctx->task->external_message);
	return 0;
}

static int
ltask_deinit(lua_State *L) {
	struct ltask *task = (struct ltask *)get_ptr(L, "LTASK_GLOBAL");

	int i;
	for (i=0;i<task->config->worker;i++) {
		worker_destory(&task->workers[i]);
	}

	service_destory(task->services);
	queue_delete(task->schedule);
	timer_destroy(task->timer);

	lua_pushnil(L);
	lua_setfield(L, LUA_REGISTRYINDEX, "LTASK_GLOBAL");
	return 0;
}

// 0 : succ
static int
newservice(lua_State *L, struct ltask *task, service_id id, const char *label, const char *source, size_t source_sz, const char *chunkname, int worker_id) {
	struct service_ud ud;
	ud.task = task;
	ud.id = id;
	struct service_pool *S = task->services;

	if (service_init(S, id, (void *)&ud, sizeof(ud), L) || service_requiref(S, id, "ltask", luaopen_ltask, L)) {
		service_delete(S, id);
		lua_pushfstring(L, "New service fail : %s", get_error_message(L));
		return -1;
	}
	service_binding_set(S, id, worker_id);
	if (service_setlabel(task->services, id, label)) {
		service_delete(S, id);
		lua_pushliteral(L, "set label fail");
		return -1;
	}
	const char * err = service_loadstring(S, id, source, source_sz, chunkname);
	if (err) {
		lua_pushstring(L, err);
		service_delete(S, id);
		return -1;
	}
	return 0;
}

static int
ltask_newservice(lua_State *L) {
	struct ltask *task = (struct ltask *)get_ptr(L, "LTASK_GLOBAL");
	const char *label = luaL_checkstring(L, 1);
	size_t source_sz = 0;
	const char *source = luaL_checklstring(L, 2, &source_sz);
	const char *chunkname = luaL_checkstring(L, 3);
	unsigned int sid = luaL_optinteger(L, 4, 0);
	int worker_id = luaL_optinteger(L, 5, -1);

	service_id id = service_new(task->services, sid);
	if (newservice(L, task, id, label, source, source_sz, chunkname, worker_id)) {
		lua_pushboolean(L, 0);
		lua_insert(L, -2);
		return 2;
	}
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

static int
external_send(void *q, void *v) {
	return queue_push_ptr((struct queue *)q, v);
}

static int
ltask_external_sender(lua_State *L) {
	luaL_checktype(L, 1, LUA_TUSERDATA);
	struct task_context *ctx = (struct task_context *)lua_touserdata(L, 1);
	struct ltask *task = ctx->task;
	if (task->external_message == NULL) {
		return luaL_error(L, "No external message queue");
	}
	lua_pushlightuserdata(L, (void *)external_send);
	lua_pushlightuserdata(L, (void *)task->external_message);
	return 2;
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
		{ "wait", ltask_wait },
		{ "post_message", lpost_message },
		{ "new_service", ltask_newservice },
		{ "init_timer", ltask_init_timer },
		{ "init_root", ltask_init_root },
		{ "init_socket", ltask_init_socket },
		{ "pushlog", ltask_boot_pushlog },
		{ "pack", luaseri_pack },
		{ "unpack", luaseri_unpack },
		{ "remove", luaseri_remove },
		{ "unpack_remove", luaseri_unpack_remove },
		{ "external_sender", ltask_external_sender },
		{ NULL, NULL },
	};
	
	luaL_newlib(L, l);
	return 1;
}

static inline const struct service_ud *
getS(lua_State *L) {
	const struct service_ud * ud = (const struct service_ud *)lua_touserdata(L, lua_upvalueindex(1));
	assert(ud);
	return ud;
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

struct timer_update_ud {
	lua_State *L;
	int n;
};

static void
timer_callback(void *ud, void *arg) {
	struct timer_update_ud *tu = (struct timer_update_ud *)ud;
	struct timer_event *event = (arg);
	lua_State *L = tu->L;
	uint64_t v = event->session;
	v = v << 32 | event->id.id;
	lua_pushinteger(L, v);
	int idx = ++tu->n;
	lua_seti(L, 1, idx);
}

static int
ltask_timer_update(lua_State *L) {
	const struct service_ud *S = getS(L);
	struct timer *t = S->task->timer;
	if (t == NULL)
		return luaL_error(L, "Init timer before bootstrap");
	if (lua_gettop(L) > 1) {
		lua_settop(L, 1);
		luaL_checktype(L, 1, LUA_TTABLE);
	}
	struct timer_update_ud tu;
	tu.L = L;
	tu.n = 0;
	timer_update(t, timer_callback, &tu);
	int n = lua_rawlen(L, 1);
	int i;
	for (i=tu.n+1;i<=n;i++) {
		lua_pushnil(L);
		lua_seti(L, 1, i);
	}
	return 1;
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
lsend_message(lua_State *L) {
	const struct service_ud *S = getS(L);
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
lrecv_message(lua_State *L) {
	const struct service_ud *S = getS(L);
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

static inline int
lmessage_receipt(lua_State *L) {
	const struct service_ud *S = getS(L);
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
ltask_sleep(lua_State *L) {
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
	uint64_t freq = lua_tointeger(L, lua_upvalueindex(2));
	uint64_t ti = systime_counter();
	lua_pushnumber(L, (double)ti / freq);
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
	int sockevent_id = service_sockevent_get(S->task->services, to);
	if (sockevent_id >= 0) {
		sockevent_trigger(&S->task->event[sockevent_id]);
		lua_pushboolean(L, 1);
		return 1;
	}
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
	uint64_t freq = lua_tointeger(L, lua_upvalueindex(2));
	lua_pushnumber(L, (double)cpucost / freq);
	return 1;
}

static int
alloc_sockevent(struct ltask *task) {
	int i;
	for (i=0;i<MAX_SOCKEVENT;i++) {
		if (atomic_int_cas(&task->event_init[i], 0, 1)) {
			return i;
		}
	}
	return -1;
}

static int
ltask_eventwait_(lua_State *L) {
	struct sockevent *e = (struct sockevent *)lua_touserdata(L, lua_upvalueindex(1));
	int r = sockevent_wait(e);
	lua_pushboolean(L, r > 0);
	return 1;
}

static int
ltask_eventinit(lua_State *L) {
	const struct service_ud *S = getS(L);
	int index = service_sockevent_get(S->task->services, S->id);
	if (index >= 0)
		return luaL_error(L, "Already init event");

	index = alloc_sockevent(S->task);
	if (index < 0)
		return luaL_error(L, "Too many sockevents");

	struct sockevent *event = &S->task->event[index];
	lua_pushlightuserdata(L, event);
	lua_pushcclosure(L, ltask_eventwait_, 1);

	if (sockevent_open(event) != 0) {
		return luaL_error(L, "Create sockevent fail");
	}

	service_sockevent_init(S->task->services, S->id, index);

	lua_pushlightuserdata(L, (void *)(intptr_t)sockevent_fd(event));

	return 2;
}

static int
ltask_eventreset(lua_State *L) {
	const struct service_ud *S = getS(L);
	int index = service_sockevent_get(S->task->services, S->id);
	if (index < 0)
		return luaL_error(L, "Init event first");

	struct sockevent *e = &S->task->event[index];
	struct sockevent tmp = *e;

	if (sockevent_open(e) != 0) {
		*e = tmp;
		return luaL_error(L, "Reset sockevent fail");
	} else {
		sockevent_close(&tmp);
	}
	lua_pushlightuserdata(L, (void *)(intptr_t)sockevent_fd(e));

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
		return luaL_error(L, "Can't find thread");
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
		{ "timer_sleep", ltask_sleep },
		{ NULL, NULL },
	};

	luaL_newlib(L, l);

	// ltask api

	luaL_Reg l2[] = {
		{ "send_message", lsend_message },
		{ "recv_message", lrecv_message },
		{ "message_receipt", lmessage_receipt },
		{ "touch_service", ltask_touch_service },
		{ "self", lself },
		{ "worker_id", lworker_id },
		{ "worker_bind", lworker_bind },
		{ "timer_add", ltask_timer_add },
		{ "timer_update", ltask_timer_update },
		{ "now", ltask_now },
		{ "pushlog", ltask_pushlog },
		{ "poplog", ltask_poplog },
		{ "get_pushlog", ltask_get_pushlog },
		{ "mem_limit", ltask_memlimit },
		{ "mem_count", ltask_memcount },
		{ "label", ltask_label },
		{ "backtrace", lbacktrace },
		{ "debuglog", ltask_debuglog },
		{ "eventinit", ltask_eventinit },
		{ "eventreset", ltask_eventreset },
		{ NULL, NULL },
	};

	if (lua_getfield(L, LUA_REGISTRYINDEX, LTASK_KEY) != LUA_TSTRING) {
		luaL_error(L, "No service id, the VM is not inited by ltask");
	}
	const struct service_ud * ud = (const struct service_ud *)luaL_checkstring(L, -1);
	lua_pop(L, 1);

	lua_pushlightuserdata(L, (void *)ud);

	luaL_setfuncs(L,l2,1);

	// counter api
	luaL_Reg l3[] = {
		{ "counter", ltask_counter },
		{ "cpucost", ltask_cpucost },
		{ NULL, NULL },
	};

	uint64_t f = systime_frequency();
	lua_pushlightuserdata(L, (void *)ud);
	lua_pushinteger(L, f);
	luaL_setfuncs(L,l3,2);

	sys_init();
	return 1;
}

static int
ltask_initservice(lua_State *L) {
	const struct service_ud *S = getS(L);
	unsigned int sid = luaL_checkinteger(L, 1);
	const char *label = luaL_checkstring(L, 2);
	size_t source_sz = 0;
	const char *source = luaL_checklstring(L, 3, &source_sz);
	const char *chunkname = luaL_checkstring(L, 4);
	int worker_id = luaL_optinteger(L, 5, -1);

	service_id id = { sid };
	if (newservice(L, S->task, id, label, source, source_sz, chunkname, worker_id)) {
		lua_pushboolean(L, 0);
		lua_insert(L, -2);
		return 2;
	} else {
		lua_pushboolean(L, 1);
		return 1;
	}
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
	int sockevent_id = service_sockevent_get(S->task->services, id);
	if (sockevent_id >= 0) {
		sockevent_close(&S->task->event[sockevent_id]);
		atomic_int_store(&S->task->event_init[sockevent_id], 0);
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
	
	luaL_newlibtable(L, l);

	if (lua_getfield(L, LUA_REGISTRYINDEX, LTASK_KEY) != LUA_TSTRING) {
		luaL_error(L, "No service id, the VM is not inited by ltask");
	}
	const struct service_ud * ud = (const struct service_ud *)luaL_checkstring(L, -1);
	lua_pop(L, 1);

	lua_pushlightuserdata(L, (void *)ud);
	luaL_setfuncs(L,l,1);

	return 1;
}
