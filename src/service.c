#include "service.h"
#include "queue.h"
#include "config.h"
#include "message.h"

#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>

#include <stdlib.h>
#include <assert.h>
#include <stdio.h>

// test whether an unsigned value is a power of 2 (or zero)
#define ispow2(x)	(((x) & ((x) - 1)) == 0)

struct service {
	lua_State *L;
	struct queue *msg;
	struct message *out;
	struct message *bounce;
	int status;
	int receipt;
	int thread_id;
	service_id id;
};

struct service_pool {
	int mask;
	int queue_length;
	unsigned int id;
	struct service **s;
};

struct service_pool *
service_create(struct ltask_config *config) {
	struct service_pool tmp;
	assert(ispow2(config->max_service));
	tmp.mask = config->max_service - 1;
	tmp.id = 0;
	tmp.queue_length = config->queue;
	tmp.s = (struct service **)malloc(sizeof(struct service *) * config->max_service);
	if (tmp.s == NULL)
		return NULL;
	struct service_pool * r = (struct service_pool *)malloc(sizeof(tmp));
	*r = tmp;
	int i;
	for (i=0;i<config->max_service;i++) {
		r->s[i] = NULL;
	}
	return r;
}

static void
free_service(struct service *S) {
	if (S->L != NULL)
		lua_close(S->L);
	if (S->msg) {
		for (;;) {
			struct message *m = queue_pop_ptr(S->msg);
			if (m) {
				message_delete(m);
			} else {
				break;
			}
		}
		queue_delete(S->msg);
	}
	message_delete(S->out);
	message_delete(S->bounce);
	S->receipt = MESSAGE_RECEIPT_NONE;
}

void
service_destory(struct service_pool *p) {
	if (p == NULL)
		return;
	int i;
	for (i=0;i<=p->mask;i++) {
		struct service *s = p->s[i];
		if (s) {
			free_service(s);
		}
	}
	free(p->s);
	free(p);
}

static int
init_service(lua_State *L) {
	void *ud = lua_touserdata(L, 1);
	int sz = lua_tointeger(L, 2);
	lua_pushlstring(L, (const char *)ud, sz);
	lua_setfield(L, LUA_REGISTRYINDEX, LTASK_KEY);
	luaL_openlibs(L);
	return 0;
}

static inline struct service **
service_slot(struct service_pool *p, unsigned int id) {
	return &p->s[id & p->mask];
}

service_id
service_new(struct service_pool *p, unsigned int sid) {
	service_id result = { 0 };
	unsigned int id;
	if (sid != 0) {
		id = sid;
		if (*service_slot(p, id) != NULL) {
			return result;
		}
	} else {
		id = p->id;
		int i = 0;
		while (id == 0 || *service_slot(p, id) != NULL) {
			++id;
			if (++i > p->mask) {
				return result;
			}
		}
		p->id = id + 1;
	}
	struct service *s = (struct service *)malloc(sizeof(*s));
	if (s == NULL)
		return result;
	s->L = NULL;
	s->msg = NULL;
	s->out = NULL;
	s->bounce = NULL;
	s->receipt = MESSAGE_RECEIPT_NONE;
	s->id.id = id;
	s->status = SERVICE_STATUS_UNINITIALIZED;
	s->thread_id = -1;
	*service_slot(p, id) = s;
	result.id = id;
	return result;
}

static inline struct service *
get_service(struct service_pool *p, service_id id) {
	struct service *S = *service_slot(p, id.id);
	if (S == NULL || S->id.id != id.id)
		return NULL;
	return S;
}

int
service_init(struct service_pool *p, service_id id, void *ud, size_t sz) {
	struct service *S = get_service(p, id);
	assert(S != NULL && S->L == NULL && S->status == SERVICE_STATUS_UNINITIALIZED);
	lua_State *L = luaL_newstate();
	if (L == NULL)
		return 1;
	lua_pushcfunction(L, init_service);
	lua_pushlightuserdata(L, ud);
	lua_pushinteger(L, sz);
	if (lua_pcall(L, 2, 0, 0) != LUA_OK) {
		lua_close(L);
		return 1;
	}
	S->msg = queue_new_ptr(p->queue_length);
	if (S->msg == NULL) {
		lua_close(L);
		return 1;
	}
	S->L = L;
	return 0;
}

static int
require_cmodule(lua_State *L) {
	const char *name = (const char *)lua_touserdata(L, 1);
	lua_CFunction f = (lua_CFunction)lua_touserdata(L, 2);
	luaL_requiref(L, name, f, 0);
	return 0;
}

int
service_requiref(struct service_pool *p, service_id id, const char *name, void *f) {
	struct service *S = get_service(p, id);
	if (S == NULL || S->L == NULL)
		return 1;
	lua_State *L = S->L;
	lua_pushcfunction(L, require_cmodule);
	lua_pushlightuserdata(L, (void *)name);
	lua_pushlightuserdata(L, f);
	if (lua_pcall(L, 2, 0, 0) != LUA_OK) {
		lua_pop(L, 1);
		return 1;
	}
	return 0;
}

static int
setp(lua_State *L) {
	const char * key = (const char *)lua_touserdata(L, 1);
	lua_settop(L, 2);
	lua_setfield(L, LUA_REGISTRYINDEX, key);
	return 0;
}

int
service_setp(struct service_pool *p, service_id id, const char *key, void *value) {
	struct service *S = get_service(p, id);
	if (S == NULL || S->L == NULL)
		return 1;
	lua_State *L = S->L;
	lua_pushcfunction(L, setp);
	lua_pushlightuserdata(L, (void *)key);
	lua_pushlightuserdata(L, value);
	if (lua_pcall(L, 2, 0, 0) != LUA_OK) {
		lua_pop(L, 1);
		return 1;
	}
	return 0;
}

static int
sets(lua_State *L) {
	const char * key = (const char *)lua_touserdata(L, 1);
	const char * value = (const char *)lua_touserdata(L, 2);
	lua_pushstring(L, value);
	lua_setfield(L, LUA_REGISTRYINDEX, key);
	return 0;
}

int
service_sets(struct service_pool *p, service_id id, const char *key, const char *value) {
	struct service *S = get_service(p, id);
	if (S == NULL || S->L == NULL)
		return 1;
	lua_State *L = S->L;
	lua_pushcfunction(L, sets);
	lua_pushlightuserdata(L, (void *)key);
	lua_pushlightuserdata(L, value);
	if (lua_pcall(L, 2, 0, 0) != LUA_OK) {
		lua_pop(L, 1);
		return 1;
	}
	return 0;
}

void 
service_close(struct service_pool *p, service_id id) {
	struct service * s = get_service(p, id);
	if (s) {
		if (s->L) {
			lua_close(s->L);
			s->L = NULL;
		}
		s->status = SERVICE_STATUS_DEAD;
	}
}

void
service_delete(struct service_pool *p, service_id id) {
	struct service * s = get_service(p, id);
	if (s) {
		*service_slot(p, id.id) = NULL;
		free_service(s);
	}
}

static inline lua_State *
service_L(struct service_pool *p, service_id id) {
	struct service *S= get_service(p, id);
	if (S == NULL)
		return NULL;
	return S->L;
}

const char *
service_loadfile(struct service_pool *p, service_id id, const char *filename) {
	struct service *S= get_service(p, id);
	if (S == NULL || S->L == NULL)
		return "Init service first";
	lua_State *L = S->L;
	if (luaL_loadfile(L, filename) != LUA_OK) {
		const char * r = lua_tostring(S->L, -1);
		lua_pop(S->L, 1);
		return r;
	}
	S->status = SERVICE_STATUS_IDLE;
	return NULL;
}

const char *
service_loadstring(struct service_pool *p, service_id id, const char *source) {
	struct service *S= get_service(p, id);
	if (S == NULL || S->L == NULL)
		return "Init service first";
	lua_State *L = S->L;
	if (luaL_loadstring(L, source) != LUA_OK) {
		const char * r = lua_tostring(S->L, -1);
		lua_pop(S->L, 1);
		return r;
	}
	S->status = SERVICE_STATUS_IDLE;
	return NULL;
}

int
service_resume(struct service_pool *p, service_id id, int thread_id) {
	struct service *S= get_service(p, id);
	if (S == NULL)
		return 1;
	S->thread_id = thread_id;
	lua_State *L = S->L;
	if (L == NULL)
		return 1;
	int nresults = 0;
	int r = lua_resume(L, NULL, 0, &nresults);
	if (r == LUA_YIELD) {
		assert(nresults == 0);
		return 0;
	}
	if (r == LUA_OK) {
		return 1;
	}
	if (!lua_checkstack(L, LUA_MINSTACK)) {
		lua_writestringerror("%s\n", lua_tostring(L, -1));
		lua_pop(L, 1);
		return 1;
	}
	lua_pushfstring(L, "Service %d error: %s", id.id, lua_tostring(L, -1));
	luaL_traceback(L, L, lua_tostring(L, -1), 0);
	lua_writestringerror("%s\n", lua_tostring(L, -1));
	lua_pop(L, 3);
	return 1;
}

int
service_thread_id(struct service_pool *p, service_id id) {
	struct service *S= get_service(p, id);
	if (S == NULL)
		return -1;
	return S->thread_id;
}

int
service_push_message(struct service_pool *p, service_id id, struct message *msg) {
	struct service *s = get_service(p, id);
	if (s == NULL || s->status == SERVICE_STATUS_DEAD)
		return -1;
	if (queue_push_ptr(s->msg, msg)) {
		// blocked
		return 1;
	}
	return 0;
}

int
service_status_get(struct service_pool *p, service_id id) {
	struct service *s = get_service(p, id);
	if (s == NULL)
		return SERVICE_STATUS_DEAD;
	return s->status;	
}

void
service_status_set(struct service_pool *p, service_id id, int status) {
	struct service *s = get_service(p, id);
	if (s == NULL)
		return;
	s->status = status;
}

struct message *
service_message_out(struct service_pool *p, service_id id) {
	struct service *s = get_service(p, id);
	if (s == NULL)
		return NULL;
	struct message * r = s->out;
	if (r)
		s->out = NULL;
	return r;
}

int
service_send_message(struct service_pool *p, service_id id, struct message *msg) {
	struct service *s = get_service(p, id);
	if (s == NULL || s->out != NULL)
		return 1;
	s->out = msg;
	return 0;
}

void
service_write_receipt(struct service_pool *p, service_id id, int receipt, struct message *bounce) {
	struct service *s = get_service(p, id);
	if (s != NULL && s->receipt == MESSAGE_RECEIPT_NONE) {
		s->receipt = receipt;
		s->bounce = bounce;
	} else {
		fprintf(stderr, "WARNING: write receipt %d fail (%d)\n", id.id, s->receipt);
		if (s) {
			message_delete(s->bounce);
			s->receipt = receipt;
			s->bounce = bounce;
		}
	}
}

struct message *
service_read_receipt(struct service_pool *p, service_id id, int *receipt) {
	struct service *s = get_service(p, id);
	if (s == NULL) {
		*receipt = MESSAGE_RECEIPT_NONE;
		return NULL;
	}
	*receipt = s->receipt;
	struct message *r = s->bounce;
	s->receipt = MESSAGE_RECEIPT_NONE;
	s->bounce = NULL;
	return r;
}

struct message *
service_pop_message(struct service_pool *p, service_id id) {
	struct service *s = get_service(p, id);
	if (s == NULL)
		return NULL;
	if (s->bounce) {
		struct message *r = s->bounce;
		s->bounce = NULL;
		return r;
	}
	return queue_pop_ptr(s->msg);
}

int
service_has_message(struct service_pool *p, service_id id) {
	struct service *s = get_service(p, id);
	if (s == NULL)
		return 0;
	if (s->receipt != MESSAGE_RECEIPT_NONE) {
		return 1;
	}
	return queue_length(s->msg) > 0;
}

void
service_send_signal(struct service_pool *p, service_id id) {
	struct service *s = get_service(p, id);
	if (s == NULL)
		return;
	if (s->out)
		message_delete(s->out);
	struct message msg;
	msg.from = id;
	msg.to.id = SERVICE_ID_ROOT;
	msg.session = 0;
	msg.type = MESSAGE_SIGNAL;
	msg.msg = NULL;
	msg.sz = 0;

	s->out = message_new(&msg);
}

int
service_hang(struct service_pool *p, service_id id) {
	struct service *s = get_service(p, id);
	if (s == NULL)
		return 0;
	switch (s->status) {
	case SERVICE_STATUS_UNINITIALIZED:
	case SERVICE_STATUS_IDLE:
	case SERVICE_STATUS_SCHEDULE:
		s->status = SERVICE_STATUS_DEAD;
		service_send_signal(p, id);
		return 0;
	case SERVICE_STATUS_DEAD:
		return 0;
	}
	// service is running
	return 1;
}
