#include "service.h"
#include "queue.h"
#include "config.h"
#include "message.h"

#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>

#include <stdlib.h>
#include <assert.h>

// test whether an unsigned value is a power of 2 (or zero)
#define ispow2(x)	(((x) & ((x) - 1)) == 0)

struct service {
	lua_State *L;
	struct queue *msg;
	struct message *out;
	struct message *resp;
	service_id id;
	int status;
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
	queue_delete(S->msg);
	message_delete(S->out);
	message_delete(S->resp);
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

int luaopen_ltask(lua_State *L);

static int
init_service(lua_State *L) {
	void *ud = lua_touserdata(L, 1);
	int sz = lua_tointeger(L, 2);
	lua_pushlstring(L, (const char *)ud, sz);
	lua_setfield(L, LUA_REGISTRYINDEX, LTASK_KEY);
	luaL_openlibs(L);
	luaL_requiref(L, "ltask", luaopen_ltask, 1);
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
		p->id = id;
	}
	struct service *s = (struct service *)malloc(sizeof(*s));
	if (s == NULL)
		return result;
	s->L = NULL;
	s->msg = NULL;
	s->out = NULL;
	s->resp = NULL;
	s->id.id = id;
	s->status = SERVICE_STATUS_UNINITIALIZED;
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
	lua_pushlightuserdata(L, ud);
	lua_pushinteger(L, sz);
	lua_pushcfunction(L, init_service);
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
	S->status = SERVICE_STATUS_IDLE;
	return 0;
}

void
service_delete(struct service_pool *p, service_id id) {
	struct service * s = get_service(p, id);
	assert(s != NULL);
	*service_slot(p, id.id) = NULL;
	free_service(s);
}

static inline lua_State *
service_L(struct service_pool *p, service_id id) {
	struct service *S= get_service(p, id);
	if (S == NULL)
		return NULL;
	return S->L;
}

const char *
service_load(struct service_pool *p, service_id id, const char *filename) {
	lua_State *L = service_L(p, id);
	if (L == NULL)
		return "Init service first";
	if (luaL_loadfile(L, filename) != LUA_OK) {
		const char * r = lua_tostring(L, -1);
		lua_pop(L, 1);
		return r;
	}
	return NULL;
}

int
service_resume(struct service_pool *p, service_id id) {
	lua_State *L = service_L(p, id);
	int nresults = 0;
	int r = lua_resume(L, NULL, 0, &nresults);
	if (r == LUA_YIELD) {
		assert(nresults == 0);
		return 0;
	}
	// term or error
	return 1;
}

int
service_push_message(struct service_pool *p, service_id id, struct message *msg) {
	struct service *s = get_service(p, id);
	if (s == NULL)
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
service_message_resp(struct service_pool *p, service_id id, struct message *resp) {
	struct service *s = get_service(p, id);
	assert (s != NULL && s->resp == NULL);
	s->resp = resp;
}

struct message *
service_pop_message(struct service_pool *p, service_id id) {
	struct service *s = get_service(p, id);
	if (s == NULL)
		return NULL;
	if (s->resp) {
		struct message *r = s->resp;
		s->resp = NULL;
		return r;
	}
	return queue_pop_ptr(s->msg);
}

int
service_message_count(struct service_pool *p, service_id id) {
	struct service *s = get_service(p, id);
	if (s == NULL || s->msg == NULL)
		return 0;
	int count = 0;
	if (s->resp) {
		count++;
	}
	count += queue_length(s->msg);
	return count;
}

