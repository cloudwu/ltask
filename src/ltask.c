#define LUA_LIB

#include <lua.h>
#include <lauxlib.h>
#include <assert.h>
#include <stdio.h>

#include "atomic.h"
#include "queue.h"
#include "thread.h"
#include "config.h"
#include "worker.h"
#include "service.h"
#include "message.h"
#include "lua-seri.h"

#define THREAD_NONE -1
#define THREAD_SOCKET 0
#define THREAD_TIMER 1
#define THREAD_COUNT 2
#define THREAD_WORKER(n) THREAD_COUNT + (n)

struct ltask {
	const struct ltask_config *config;
	struct worker_thread *workers;
	struct service_pool *services;
	struct queue *schedule;
	atomic_int schedule_owner;
	atomic_int active_worker;
};

struct service_ud {
	struct ltask *task;
	service_id id;
};

static int
dispatch_schedule_message(struct service_pool *P, service_id id, struct message *msg) {
	switch (msg->type) {
	case MESSAGE_SCHEDULE_NEW:
		msg->to = service_new(P, msg->from.id);
		if (msg->to.id == 0)
			return 1;	// failed
		msg->from.id = SERVICE_ID_SYSTEM;
		msg->type = MESSAGE_RESPONSE;
		service_message_resp(P, id, msg);
		break;
	case MESSAGE_SCHEDULE_DEL:
		service_delete(P, msg->from);
		msg->from.id = SERVICE_ID_SYSTEM;
		msg->to = id;
		msg->type = MESSAGE_RESPONSE;
		service_message_resp(P, id, msg);
		break;
	default:
		return 1;
	}
	return 0;
}

static void
dispatch_out_message(struct service_pool *P, service_id id) {
	struct message *msg = service_message_out(P, id);
	if (msg) {
		if (msg->to.id == SERVICE_ID_SYSTEM) {
			if (id.id != SERVICE_ID_ROOT || dispatch_schedule_message(P, id, msg)) {
				msg->from.id = SERVICE_ID_SYSTEM;
				msg->to = id;
				msg->type = MESSAGE_ERROR;
				service_message_resp(P, id, msg);
			}
		} else {
			session_t session = (session_t)msg->session;
			int type = msg->type;
			int r = service_push_message(P, msg->to, msg);
			if (r == 0) {
				// succ
				if (type == MESSAGE_POST) {
					// post message need response now
					struct message resp;
					resp.from.id = SERVICE_ID_SYSTEM;
					resp.to = id;
					resp.session = session;
					resp.type = MESSAGE_RESPONSE;
					resp.msg = NULL;
					resp.sz = 0;
					service_message_resp(P, id, message_new(&resp));
				}
			} else {
				msg->from.id = SERVICE_ID_SYSTEM;
				msg->to = id;
				if (r == 1) {
					msg->type = MESSAGE_BLOCK;
				} else {
					assert(r == -1);
					msg->type = MESSAGE_ERROR;
				}
				service_message_resp(P, id, msg);
			}
		}
	}
}

void
schedule_dispatch(struct ltask *task) {
	// Step 1 : Collect service_done
	int done_job_n = 0;
	service_id done_job[MAX_WORKER];
	int i;
	for (i=0;i<task->config->worker;i++) {
		service_id job = worker_done_job(&task->workers[i]);
		if (job.id) {
			done_job[done_job_n++] = job;
		}
	}

	// Step 2: Dispatch out message by service_done

	struct service_pool *P = task->services;

	for (i=0;i<done_job_n;i++) {
		service_id id = done_job[i];
		dispatch_out_message(P, id);

		if (service_message_count(P, id) == 0) {
			service_status_set(P, id, SERVICE_STATUS_IDLE);
		} else {
			service_status_set(P, id, SERVICE_STATUS_SCHEDULE);
			int r = queue_push_int(task->schedule, (int)id.id);
			assert(r == 0);
		}
	}

	// Step 3: Assign task to workers

	int job = queue_pop_int(task->schedule);
	if (job) {
		// Has at least one job
		// Todo : record assign history, and try to always assign one job to the same worker.
		for (i=0;i<task->config->worker;i++) {
			service_id id = { job };
			if (!worker_assign_job(&task->workers[i], id)) {
				job = queue_pop_int(task->schedule);
				if (job == 0) {
					// No job in queue
					break;
				}
			}
		}
	}
}

// 0 succ
static int
aquire_scheduler(struct worker_thread * worker) {
	if (atomic_int_load(&worker->task->schedule_owner) == THREAD_NONE) {
		if (atomic_int_cas(&worker->task->schedule_owner, THREAD_NONE, THREAD_WORKER(worker->worker_id))) {
			printf("Worker %d aquire\n", worker->worker_id);
			return 0;
		}
	}
	return 1;
}

static void
release_scheduler(struct worker_thread * worker) {
	assert(worker->task->schedule_owner == THREAD_WORKER(worker->worker_id));
	atomic_int_store(&worker->task->schedule_owner, THREAD_NONE);
	printf("Worker %d release\n", worker->worker_id);
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
			worker_assign_job(worker, job);
		} else {
			return 1;
		}
	}
	return 0;
}

static void
thread_worker(void *ud) {
	struct worker_thread * w = (struct worker_thread *)ud;
	struct service_pool * P = w->task->services;
	atomic_int_inc(&w->task->active_worker);

	for (;;) {
		if (w->term_signal)
			return;
		service_id id = worker_get_job(w);
		if (id.id) {
			w->running = id;
			service_status_set(P, id, SERVICE_STATUS_RUNNING);
			if (service_resume(P, id)) {
				service_status_set(P, id, SERVICE_STATUS_DEAD);
			}
			int scheduler_is_owner = 0;
			while (worker_complete_job(w)) {
				// Can't complete (running -> done)
				if (!aquire_scheduler(w)) {
					scheduler_is_owner = 1;
					worker_complete_job(w);
					break;
				}
			}
			service_status_set(P, id, SERVICE_STATUS_DONE);
			if (scheduler_is_owner) {
				schedule_dispatch_worker(w);
				release_scheduler(w);
			}
		} else {
			// No job, try to aquire scheduler to find a job
			int nojob = 1;
			if (!aquire_scheduler(w)) {
				nojob = schedule_dispatch_worker(w);
				release_scheduler(w);
			}
			if (nojob) {
				// go to sleep
				atomic_int_dec(&w->task->active_worker);
				thread_event_wait(&w->trigger);
			}
		}
	}
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

	int i;
	for (i=0;i<config->worker;i++) {
		worker_init(&task->workers[i], task, i);
	}

	atomic_int_init(&task->schedule_owner, THREAD_NONE);
	atomic_int_init(&task->active_worker, 0);

	return 1;
}

static void
thread_timer(void *ud) {
	// todo
}

static void
thread_socket(void *ud) {
	// todo
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
ltask_run(lua_State *L) {
	struct ltask *task = (struct ltask *)get_ptr(L, "LTASK_GLOBAL");

	int threads_count = task->config->worker + THREAD_COUNT;

	struct thread * t = (struct thread *)lua_newuserdatauv(L, threads_count * sizeof(struct thread), 0);
	t[THREAD_TIMER].func = thread_timer;
	t[THREAD_TIMER].ud = (void *)task;
	t[THREAD_SOCKET].func = thread_socket;
	t[THREAD_SOCKET].ud = (void *)task;
	int i;
	for (i=0;i<task->config->worker;i++) {
		t[THREAD_WORKER(i)].func = thread_worker;
		t[THREAD_WORKER(i)].ud = (void *)&task->workers[i];
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

	service_destory(task->services);
	queue_delete(task->schedule);

	lua_pushnil(L);
	lua_setfield(L, LUA_REGISTRYINDEX, "LTASK_GLOBAL");
	return 0;
}

static int
ltask_newservice(lua_State *L) {
	struct ltask *task = (struct ltask *)get_ptr(L, "LTASK_GLOBAL");
	const char *filename = luaL_checkstring(L, 1);
	unsigned int sid = luaL_optinteger(L, 2, 0);
	struct service_pool *S = task->services;
	service_id id = service_new(S, sid);
	struct service_ud ud;
	ud.task = task;
	ud.id = id;
	if (service_init(S, id, (void *)&ud, sizeof(ud))) {
		service_delete(S, id);
		return luaL_error(L, "New services faild");
	}
	const char * err = service_load(S, id, filename);
	if (err) {
		lua_pushstring(L, err);
		service_delete(S, id);
		return lua_error(L);
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
	if (service_status_get(task->services, msg.to) == SERVICE_STATUS_IDLE) {
		service_status_set(task->services, msg.to, SERVICE_STATUS_SCHEDULE);
		queue_push_int(task->schedule, (int)msg.to.id);
	}
	return 0;
}

LUAMOD_API int
luaopen_ltask_schedule(lua_State *L) {
	static atomic_int init = ATOMIC_VAR_INIT(0);
	if (atomic_int_inc(&init) != 1) {
		return luaL_error(L, "ltask.schedule can only require once");
	}
	luaL_checkversion(L);
	luaL_Reg l[] = {
		{ "init", ltask_init },
		{ "run", ltask_run },
		{ "newservice", ltask_newservice },
		{ "post_message", lpost_message },
		{ "deinit", ltask_deinit },
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
	struct message m;
	m.from = S->id;
	m.to.id = luaL_checkinteger(L, 1);
	m.session = (session_t)luaL_checkinteger(L, 2);
	m.type = luaL_checkinteger(L, 3);
	if (lua_isnoneornil(L, 4)) {
		m.msg = NULL;
		m.sz = 0;
	} else {
		luaL_checktype(L, 5, LUA_TLIGHTUSERDATA);
		m.msg = lua_touserdata(L, 5);
		m.sz = (size_t)luaL_checkinteger(L, 6);
	}

	struct message *msg = message_new(&m);
	if (service_send_message(S->task->services, S->id, msg)) {
		// error
		message_delete(msg);
		return luaL_error(L, "Can't send message");
	}
	return 0;	
}

static int
lrecv_message(lua_State *L) {
	const struct service_ud *S = getS(L);
	struct message *m = service_pop_message(S->task->services, S->id);
	int r = 3;
	lua_pushinteger(L, m->from.id);
	lua_pushinteger(L, m->session);
	lua_pushinteger(L, m->type);
	if (m->msg) {
		lua_pushlightuserdata(L, m->msg);
		lua_pushinteger(L, m->sz);
		m->msg = NULL;
		m->sz = 0;
	}
	message_delete(m);

	return r;
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

LUAMOD_API int
luaopen_ltask(lua_State *L) {
	luaL_checkversion(L);
	luaL_Reg l[] = {
		{ "pack", luaseri_pack },
		{ "unpack", luaseri_unpack },
		{ "remove", luaseri_remove },
		{ "send_message", lsend_message },
		{ "recv_message", lrecv_message },
		{ NULL, NULL },
	};

	luaL_newlib(L, l);
	return 1;
}
