#define LUA_LIB

#include <lua.h>
#include <lauxlib.h>
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>

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

LUAMOD_API int luaopen_ltask(lua_State *L);
LUAMOD_API int luaopen_ltask_bootstrap(lua_State *L);
LUAMOD_API int luaopen_ltask_exclusive(lua_State *L);
LUAMOD_API int luaopen_ltask_root(lua_State *L);

#define THREAD_NONE -1
#define THREAD_WORKER(n) (MAX_EXCLUSIVE + (n))
#define THREAD_EXCLUSIVE(n) (n)

#define debug_printf(...)
//#define debug_printf(fmt, ...) printf("[SCHEDULE] " fmt __VA_OPT__(,) __VA_ARGS__)

struct exclusive_thread {
	struct ltask *task;
	int thread_id;
	service_id service;
	struct queue *sending;
};

struct ltask {
	const struct ltask_config *config;
	struct worker_thread *workers;
	struct exclusive_thread exclusives[MAX_EXCLUSIVE];
	struct service_pool *services;
	struct queue *schedule;
	struct timer *timer;
	atomic_int schedule_owner;
	atomic_int active_worker;
};

struct service_ud {
	struct ltask *task;
	service_id id;
};

struct queue *
get_exclusive_thread_sending(struct ltask *task, int thread_id) {
	if (thread_id < 0 && thread_id >= MAX_EXCLUSIVE)
		return NULL;
	return task->exclusives[thread_id].sending;
}

static void
dispatch_schedule_message(struct service_pool *P, service_id id, struct message *msg) {
	if (id.id != SERVICE_ID_ROOT) {
		// only root can send schedule message
		service_write_receipt(P, id, MESSAGE_RECEIPT_ERROR, msg);
		return;
	}

	service_id sid = { msg->session };
	switch (msg->type) {
	case MESSAGE_SCHEDULE_NEW:
		msg->to = service_new(P, sid.id);
		if (msg->to.id == 0) {
			service_write_receipt(P, id, MESSAGE_RECEIPT_ERROR, msg);
		} else {
			service_write_receipt(P, id, MESSAGE_RECEIPT_RESPONCE, msg);
		}
		break;
	case MESSAGE_SCHEDULE_DEL:
		service_delete(P, sid);
		message_delete(msg);
		service_write_receipt(P, id, MESSAGE_RECEIPT_DONE, NULL);
		break;
	case MESSAGE_SCHEDULE_HANG :
		if (service_hang(P, sid)) {
			service_write_receipt(P, id, MESSAGE_RECEIPT_ERROR, msg);
		} else {
			service_write_receipt(P, id, MESSAGE_RECEIPT_DONE, NULL);
		}
		break;
	default:
		service_write_receipt(P, id, MESSAGE_RECEIPT_ERROR, msg);
		break;
	}
}

static inline void
schedule_back(struct ltask *task, service_id id) {
	int r = queue_push_int(task->schedule, (int)id.id);
	// Must succ because task->schedule is large enough.
	assert(r == 0);
}

static void
dispatch_out_message(struct ltask *task, service_id id, struct message *msg) {
	debug_printf("Message from %d to %d type=%d\n", id.id, msg->to.id, msg->type);
	struct service_pool *P = task->services;
	if (msg->to.id == SERVICE_ID_SYSTEM) {
		dispatch_schedule_message(P, id, msg);
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
		if (service_status_get(P, msg->to) == SERVICE_STATUS_IDLE) {
			debug_printf("Service %x is in schedule\n", msg->to.id);
			service_status_set(P, msg->to, SERVICE_STATUS_SCHEDULE);
			schedule_back(task, msg->to);
		}
	}
}

static void
dispatch_exclusive_sending(struct ltask *task, struct queue *sending) {
	struct service_pool *P = task->services;
	int len = queue_length(sending);
	int i;
	for (i=0;i<len;i++) {
		struct message *msg = (struct message *)queue_pop_ptr(sending);
		switch (service_push_message(P, msg->to, msg)) {
		case 0 :	// succ
			break;
		case 1 :	// block, push back
			queue_push_ptr(sending, msg);
			break;
		default :	// dead, delete message
			// todo : report somewhere or release object in message
			message_delete(msg);
			break;
		}
		if (service_status_get(P, msg->to) == SERVICE_STATUS_IDLE) {
			debug_printf("Service %x is in schedule\n", msg->to.id);
			service_status_set(P, msg->to, SERVICE_STATUS_SCHEDULE);
			schedule_back(task, msg->to);
		}
	}
}

static int
schedule_dispatch(struct ltask *task) {
	// Step 1 : Collect service_done
	int done_job_n = 0;
	service_id done_job[MAX_WORKER];
	int i;
	for (i=0;i<task->config->worker;i++) {
		service_id job = worker_done_job(&task->workers[i]);
		if (job.id) {
			debug_printf("Service %x is done\n", job.id);
			done_job[done_job_n++] = job;
		}
	}

	// Step 2: Dispatch out message by service_done

	struct service_pool *P = task->services;

	for (i=0;i<done_job_n;i++) {
		service_id id = done_job[i];
		if (service_status_get(P, id) == SERVICE_STATUS_DEAD) {
			debug_printf("Service %x is dead\n", id.id);
			// dead service may enter into schedule because of blocked response messages
			if (!service_has_message(P, id)) {
				service_delete(P, id);
			} else {
				schedule_back(task, id);
			}
		} else {
			struct message *msg = service_message_out(P, id);
			if (msg) {
				dispatch_out_message(task, id, msg);
			}
			if (!service_has_message(P, id)) {
				debug_printf("Service %x is idle\n", id.id);
				service_status_set(P, id, SERVICE_STATUS_IDLE);
			} else {
				service_status_set(P, id, SERVICE_STATUS_SCHEDULE);
				schedule_back(task, id);
			}
		}
	}

	// Step 3: Assign task to workers

	int assign_job = 0;

	int job = 0;
	for (i=0;i<task->config->worker;i++) {
		if (job == 0) {
			job = queue_pop_int(task->schedule);
			if (job == 0) {
				// No job in the queue
				break;
			}
		}
		// Todo : record assign history, and try to always assign one job to the same worker.
		service_id id = { job };
		if (!worker_assign_job(&task->workers[i], id)) {
			debug_printf("Assign %x to worker %d\n", id.id, i);
			worker_wakeup(&task->workers[i]);
			++assign_job;
			job = 0;
		}
	}
	if (job != 0) {
		// Push unassigned job back
		queue_push_int(task->schedule, job);
	}
	return assign_job;
}

// 0 succ
static int
acquire_scheduler(struct worker_thread * worker) {
	if (atomic_int_load(&worker->task->schedule_owner) == THREAD_NONE) {
		if (atomic_int_cas(&worker->task->schedule_owner, THREAD_NONE, THREAD_WORKER(worker->worker_id))) {
			debug_printf("Worker %d acquire\n", worker->worker_id);
			return 0;
		}
	}
	return 1;
}

static int
release_scheduler(struct worker_thread * worker) {
	int alive = service_alive(worker->task->services);
	assert(atomic_int_load(&worker->task->schedule_owner) == THREAD_WORKER(worker->worker_id));
	atomic_int_store(&worker->task->schedule_owner, THREAD_NONE);
	debug_printf("Worker %d release (%d)\n", worker->worker_id, alive);
	return alive == 0;
}

static void
acquire_scheduler_exclusive(struct exclusive_thread * exclusive) {
	while (!atomic_int_cas(&exclusive->task->schedule_owner, THREAD_NONE, THREAD_EXCLUSIVE(exclusive->thread_id))) {}
	debug_printf("Exclusive %d acquire\n", exclusive->thread_id);
}

static int
release_scheduler_exclusive(struct exclusive_thread * exclusive) {
	int alive = service_alive(exclusive->task->services);
	assert(atomic_int_load(&exclusive->task->schedule_owner) == THREAD_EXCLUSIVE(exclusive->thread_id));
	atomic_int_store(&exclusive->task->schedule_owner, THREAD_NONE);
	debug_printf("Exclusive %d release (%d)\n", exclusive->thread_id, alive);
	return alive == 0;
}

static service_id
steal_job(struct worker_thread * worker) {
	int i;
	for (i=0;i<worker->task->config->worker;i++) {
		service_id job = worker_get_job(&worker->task->workers[i]);
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
		service_id job = steal_job(worker);
		if (job.id) {
			debug_printf("Worker %d steal service %x\n", worker->worker_id, job.id);
			worker_assign_job(worker, job);
		} else {
			return 1;
		}
	}
	return 0;
}

static void
close_service(struct service_pool *P, service_id id) {
	service_close(P, id);
	for (;;) {
		struct message * m = service_pop_message(P, id);
		if (m) {
			// todo : response message (error)
			message_delete(m);
		} else {
			break;
		}
	}
}

static void
wakeup_all_workers(struct ltask *task) {
	int i;
	for (i=0;i<task->config->worker;i++) {
		worker_wakeup(&task->workers[i]);
	}
}

static void
thread_worker(void *ud) {
	struct worker_thread * w = (struct worker_thread *)ud;
	struct service_pool * P = w->task->services;
	atomic_int_inc(&w->task->active_worker);

	int thread_id = THREAD_WORKER(w->worker_id);
	for (;;) {
		if (w->term_signal)
			return;
		service_id id = worker_get_job(w);
		if (id.id) {
			w->running = id;
			int isdead = 0;
			if (service_status_get(P, id) != SERVICE_STATUS_DEAD) {
				debug_printf("Worker %d run service %x\n", w->worker_id, id.id);
				service_status_set(P, id, SERVICE_STATUS_RUNNING);
				if (service_resume(P, id, thread_id)) {
					debug_printf("Service %x quit\n", id.id);
					isdead = 1;
					close_service(P, id);
				}
			} else {
				isdead = 1;
				debug_printf("Service %x is dead on worker %d\n", id.id, w->worker_id);
				close_service(P, id);
			}
			int scheduler_is_owner = 0;
			while (worker_complete_job(w)) {
				// Can't complete (running -> done)
				if (!acquire_scheduler(w)) {
					scheduler_is_owner = 1;
					worker_complete_job(w);
					break;
				}
			}
			service_status_set(P, id, isdead ? SERVICE_STATUS_DEAD : SERVICE_STATUS_DONE);
			if (scheduler_is_owner) {
				schedule_dispatch_worker(w);
				if (release_scheduler(w))
					break;	// exit
			}
		} else {
			// No job, try to acquire scheduler to find a job
			int nojob = 1;
			if (!acquire_scheduler(w)) {
				nojob = schedule_dispatch_worker(w);
				if (release_scheduler(w))
					break;	// exit
			}
			if (nojob) {
				// go to sleep
				atomic_int_dec(&w->task->active_worker);
				debug_printf("Worker %d is sleeping (%d)\n", w->worker_id, atomic_int_load(&w->task->active_worker));
				worker_sleep(w);
				atomic_int_inc(&w->task->active_worker);
				debug_printf("Worker %d wakeup\n", w->worker_id);
			}
		}
	}
	worker_quit(w);
	debug_printf("Quit Worker %d\n",w->worker_id);
	wakeup_all_workers(w->task);
}

static void
thread_exclusive(void *ud) {
	struct exclusive_thread *e = (struct exclusive_thread *)ud;
	struct service_pool * P = e->task->services;
	service_id id = e->service;
	int total_worker = e->task->config->worker;
	int isdead = 0;
	int thread_id = THREAD_EXCLUSIVE(e->thread_id);
	struct queue * sending = e->sending;
	while (!isdead) {
		isdead = service_resume(P, id, thread_id);
		if (isdead) {
			service_close(P, id);
		}
		int queue_len = queue_length(sending);
		struct message *message_out = service_message_out(P, id);
		if (message_out || isdead || queue_len > 0) {
			acquire_scheduler_exclusive(e);
			if (message_out) {
				dispatch_out_message(e->task, id, message_out);
			}
			dispatch_exclusive_sending(e->task, sending);
			int jobs = schedule_dispatch(e->task);
			int active_worker = atomic_int_load(&e->task->active_worker);
			int sleeping_worker = total_worker - active_worker;
			if (sleeping_worker > 0 && jobs > active_worker) {
				jobs -= active_worker;
				int wakeup = jobs > sleeping_worker ? sleeping_worker : jobs;
				int i;
				for (i=0;i<total_worker && wakeup > 0;i++) {
					wakeup -= worker_wakeup(&e->task->workers[i]);
				}
			}
			if (isdead) {
				service_delete(P, id);
			}
			if (release_scheduler_exclusive(e))
				break;
		}
	}
	debug_printf("Exclusive quit %d\n", e->thread_id);
	wakeup_all_workers(e->task);
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

	struct ltask *task = (struct ltask *)lua_newuserdatauv(L, sizeof(*task), 0);
	lua_setfield(L, LUA_REGISTRYINDEX, "LTASK_GLOBAL");

	task->config = config;
	task->workers = (struct worker_thread *)lua_newuserdatauv(L, config->worker * sizeof(struct worker_thread), 0);
	lua_setfield(L, LUA_REGISTRYINDEX, "LTASK_WORKERS");
	task->services = service_create(config);
	task->schedule = queue_new_int(config->max_service);
	task->timer = NULL;

	int i;
	for (i=0;i<config->worker;i++) {
		worker_init(&task->workers[i], task, i);
	}

	atomic_int_init(&task->schedule_owner, THREAD_NONE);
	atomic_int_init(&task->active_worker, 0);

	for (i=0;i<MAX_EXCLUSIVE;i++) {
		task->exclusives[i].task = NULL;
		task->exclusives[i].sending = NULL;
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

static int
ltask_exclusive(lua_State *L) {
	struct ltask *task = (struct ltask *)get_ptr(L, "LTASK_GLOBAL");
	int ecount = exclusive_count(task);
	if (ecount >= MAX_EXCLUSIVE) {
		return luaL_error(L, "Too many exclusive thread");
	}
	struct exclusive_thread *e = &task->exclusives[ecount];
	e->service.id = luaL_checkinteger(L, 1);
	debug_printf("Exclusive %d : service %x\n", ecount,  e->service.id);
	if (service_status_get(task->services, e->service) != SERVICE_STATUS_IDLE) {
		return luaL_error(L, "Service is uninitialized");
	}
	service_requiref(task->services, e->service, "ltask.exclusive", luaopen_ltask_exclusive);
	service_status_set(task->services, e->service, SERVICE_STATUS_EXCLUSIVE);
	e->task = task;
	e->thread_id = ecount;
	e->sending = queue_new_ptr(task->config->queue_sending);
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

static int
ltask_run(lua_State *L) {
	struct ltask *task = (struct ltask *)get_ptr(L, "LTASK_GLOBAL");
	
	int ecount = exclusive_count(task);
	int threads_count = task->config->worker + ecount;

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
	thread_join(t, threads_count);
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

static void
newservice(lua_State *L, struct ltask *task, service_id id, const char *filename_source) {
	struct service_ud ud;
	ud.task = task;
	ud.id = id;
	struct service_pool *S = task->services;
	if (service_init(S, id, (void *)&ud, sizeof(ud)) || service_requiref(S, id, "ltask", luaopen_ltask)) {
		service_delete(S, id);
		luaL_error(L, "New services faild");
	} else {
		const char * err = NULL;
		if (filename_source[0] == '@') {
			service_loadfile(S, id, filename_source+1);
		} else {
			service_loadstring(S, id, filename_source);
		}
		if (err) {
			lua_pushstring(L, err);
			service_delete(S, id);
			lua_error(L);
		}
	}
}

static int
ltask_newservice(lua_State *L) {
	struct ltask *task = (struct ltask *)get_ptr(L, "LTASK_GLOBAL");
	const char *filename_source = luaL_checkstring(L, 1);
	unsigned int sid = luaL_optinteger(L, 2, 0);
	service_id id = service_new(task->services, sid);
	newservice(L, task, id, filename_source);
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
	if (service_status_get(task->services, msg.to) == SERVICE_STATUS_IDLE) {
		debug_printf("Service %x is in schedule\n", msg.to.id);
		service_status_set(task->services, msg.to, SERVICE_STATUS_SCHEDULE);
		schedule_back(task, msg.to);
	}
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
	if (service_requiref(task->services, id, "ltask.root", luaopen_ltask_root)) {
		return luaL_error(L, "require ltask.root fail");
	}
	return 0;
}

LUAMOD_API int
luaopen_ltask_bootstrap(lua_State *L) {
	static atomic_int init = ATOMIC_VAR_INIT(0);
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
			lua_newtable(L);
			lua_pushinteger(L, event->session);
			lua_rawseti(L, -2, 1);
			lua_pushinteger(L, event->id.id);
			lua_rawseti(L, -2 , 2);
			te->blocked = 1;
		}
	}
}

static int
lexclusive_timer_update(lua_State *L) {
	const struct service_ud *S = getS(L);
	struct timer *t = S->task->timer;
	if (t == NULL)
		return luaL_error(L, "Init timer before bootstrap");

	int ethread = service_thread_id(S->task->services, S->id);
	struct queue *q = get_exclusive_thread_sending(S->task, ethread);
	struct timer_execute te;
	te.L = L;
	te.sending = q;
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
static int
lsend_message(lua_State *L) {
	const struct service_ud *S = getS(L);
	struct message *msg = gen_send_message(L, S->id);
	if (service_send_message(S->task->services, S->id, msg)) {
		// error
		message_delete(msg);
		return luaL_error(L, "Can't send message");
	}
	return 0;
}

static int
lexclusive_send_message(lua_State *L) {
	const struct service_ud *S = getS(L);
	int ethread = service_thread_id(S->task->services, S->id);
	struct queue *q = get_exclusive_thread_sending(S->task, ethread);
	if (q == NULL)
		return luaL_error(L, "%x is not in exclusive thread", S->id);
	struct message *msg = gen_send_message(L, S->id);
	if (queue_push_ptr(q, msg)) {
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

static int
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
lsleep(lua_State *L) {
	lua_Integer csec = luaL_checkinteger(L, 1);
	sys_sleep(csec);
	return 0;
}

static int
lunpack_remove(lua_State *L) {
	if (lua_isnoneornil(L, 1)) {
		return 0;
	}
	void *msg = lua_touserdata(L, 1);
	lua_settop(L, 2);
	int n = luaseri_unpack(L);
	free(msg);
	return n;
}

static int
lself(lua_State *L) {
	const struct service_ud *S = getS(L);
	lua_pushinteger(L, S->id.id);

	return 1;
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
	lua_pushinteger(L, start * 100 + now);
	return 2;
}

LUAMOD_API int
luaopen_ltask(lua_State *L) {
	luaL_checkversion(L);
	luaL_Reg l[] = {
		{ "pack", luaseri_pack },
		{ "unpack", luaseri_unpack },
		{ "remove", luaseri_remove },
		{ "unpack_remove", lunpack_remove },
		{ "send_message", lsend_message },
		{ "recv_message", lrecv_message },
		{ "message_receipt", lmessage_receipt },
		{ "sleep", lsleep },
		{ "self", lself },
		{ "timer_add", ltask_timer_add },
		{ "now", ltask_now },
		{ NULL, NULL },
	};

	luaL_newlib(L, l);
	sys_init();
	return 1;
}

LUAMOD_API int
luaopen_ltask_exclusive(lua_State *L) {
	luaL_checkversion(L);
	luaL_Reg l[] = {
		{ "send", lexclusive_send_message },
		{ "timer_update", lexclusive_timer_update },
		{ NULL, NULL },
	};

	luaL_newlib(L, l);
	return 1;
}

static int
ltask_initservice(lua_State *L) {
	const struct service_ud *S = getS(L);
	unsigned int sid = luaL_checkinteger(L, 1);
	const char *filename_source = luaL_checkstring(L, 2);

	service_id id = { sid };
	newservice(L, S->task, id, filename_source);

	return 0;
}

static int
ltask_closeservice(lua_State *L) {
	const struct service_ud *S = getS(L);
	unsigned int sid = luaL_checkinteger(L, 1);
	service_id id = { sid };
	if (service_status_get(S->task->services, id) != SERVICE_STATUS_DEAD) {
		return luaL_error(L, "Hang %d before close it", sid);
	}
	service_close(S->task->services, id);
	return 0;
}

LUAMOD_API int
luaopen_ltask_root(lua_State *L) {
	static atomic_int init = ATOMIC_VAR_INIT(0);
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
