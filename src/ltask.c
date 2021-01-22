#define LUA_LIB

#include <lua.h>
#include <lauxlib.h>
#include <assert.h>

#include "atomic.h"
#include "queue.h"
#include "thread.h"
#include "config.h"
#include "worker.h"
#include "service.h"
#include "message.h"
#include "schedule.h"
#include "lua-seri.h"

#define THREAD_SOCKET 0
#define THREAD_TIMER 1
#define THREAD_COUNT 2

struct ltask {
	const struct ltask_config *config;
	struct worker_thread *workers;
	struct service_pool *services;
	struct queue *schedule;
};

static int
dispatch_schdule_message(struct service_pool *P, service_id id, struct message *msg) {
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
			if (id.id != SERVICE_ID_ROOT || dispatch_schdule_message(P, id, msg)) {
				msg->from.id = SERVICE_ID_SYSTEM;
				msg->to = id;
				msg->type = MESSAGE_ERROR;
				service_message_resp(P, id, msg);
			}
		} else {
			int session = msg->session;
			int type = msg->type;
			int r = service_message(P, msg->to, msg);
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
schedule_dispatch(struct ltask *task, service_id id, int worker_id) {
	struct service_pool *P = task->services;
	dispatch_out_message(P, id);

	int i;
	int task_n = 0;
	for (i=0;i<task->config->worker;i++) {
		if (i != worker_id) {
			if (worker_is_ready(&task->workers[i])) {
				// todo : assign task to worker
			}
		}
	}

	if (service_message_count(P, id) == 0) {
		service_status_set(P, id, SERVICE_STATUS_IDLE);
	} else {
		service_status_set(P, id, SERVICE_STATUS_SCHEDULE);
		int r = queue_push_int(task->schedule, (int)id.id);
		assert(r == 0);
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
		worker_init(&task->workers[i], config);
	}

	return 1;
}

static void
thread_timer(void *ud) {
}

static void
thread_socket(void *ud) {
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
	t[THREAD_SOCKET].func = thread_socket;
	int i;
	for (i=0;i<task->config->worker;i++) {
		t[i+THREAD_COUNT].func = worker_thread_func;
	}
	for (i=0;i<threads_count; i++) {
		t[i].ud = (void *)task;
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
	if (service_init(S, id)) {
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
lmessage(lua_State *L) {
	luaL_checktype(L, 1, LUA_TTABLE);
	struct message msg;
	msg.from.id = checkfield(L, 1, "from");
	msg.to.id = checkfield(L, 1, "to");
	msg.session = checkfield(L, 1, "session");
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
	if (service_message(task->services, msg.to, m)) {
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
		{ "message", lmessage },
		{ "deinit", ltask_deinit },
		{ NULL, NULL },
	};
	
	luaL_newlib(L, l);
	return 1;
}

LUAMOD_API int
luaopen_ltask(lua_State *L) {
	luaL_checkversion(L);
	luaL_Reg l[] = {
		{ "pack", luaseri_pack },
		{ "unpack", luaseri_unpack },
		{ NULL, NULL },
	};

	luaL_newlib(L, l);
	return 1;
}
