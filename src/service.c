#include "service.h"
#include "queue.h"
#include "config.h"
#include "message.h"
#include "systime.h"

#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>

#ifndef LTASK_EXTERNAL_OPENLIBS
#define LTASK_EXTERNAL_OPENLIBS luaL_openlibs
#else
void LTASK_EXTERNAL_OPENLIBS(lua_State *L);
#endif

#include <stdlib.h>
#include <assert.h>
#include <stdio.h>
#include <string.h>

// test whether an unsigned value is a power of 2 (or zero)
#define ispow2(x)	(((x) & ((x) - 1)) == 0)

#define TYPEID_STRING 0
#define TYPEID_TABLE 1
#define TYPEID_FUNCTION 2
#define TYPEID_USERDATA 3
#define TYPEID_THREAD 4
#define TYPEID_NONEOBJECT 5
#define TYPEID_COUNT 6

static int
lua_typeid[LUA_NUMTYPES] = {
	TYPEID_NONEOBJECT,	// LUA_TNIL
	TYPEID_NONEOBJECT,	// LUA_TBOOLEAN
	TYPEID_NONEOBJECT,	// LUA_TLIGHTUSERDATA
	TYPEID_NONEOBJECT,	// LUA_TNUMBER
	TYPEID_STRING,
	TYPEID_TABLE,
	TYPEID_FUNCTION,
	TYPEID_USERDATA,
	TYPEID_THREAD,
};

struct memory_stat {
	size_t count[TYPEID_COUNT];
	size_t mem;
	size_t limit;
};

struct service {
	lua_State *L;
	lua_State *rL;
	struct queue *msg;
	struct message *out;
	struct message *bounce;
	int status;
	int receipt;
	int binding_thread;
	int sockevent_id;
	service_id id;
	char label[32];
	struct memory_stat stat;
	uint64_t cpucost;
	uint64_t clock;
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
	free(S);
}

void
service_destroy(struct service_pool *p) {
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

static void
init_service_key(lua_State *L, void *ud, size_t sz) {
	lua_pushlstring(L, (const char *)ud, sz);
	lua_setfield(L, LUA_REGISTRYINDEX, LTASK_KEY);
}

static int
init_service(lua_State *L) {
	void *ud = lua_touserdata(L, 1);
	size_t sz = lua_tointeger(L, 2);
	init_service_key(L, ud, sz);
	LTASK_EXTERNAL_OPENLIBS(L);
	lua_gc(L, LUA_GCGEN, 0, 0);
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
	s->rL = NULL;
	s->msg = NULL;
	s->out = NULL;
	s->bounce = NULL;
	s->receipt = MESSAGE_RECEIPT_NONE;
	s->id.id = id;
	s->status = SERVICE_STATUS_UNINITIALIZED;
	s->binding_thread = -1;
	s->sockevent_id = -1;
	s->cpucost = 0;
	s->clock = 0;
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

static inline int
check_limit(struct memory_stat *stat) {
	if (stat->limit == 0)
		return 0;
	return (stat->mem > stat->limit);
}

static void *
service_alloc(void *ud, void *ptr, size_t osize, size_t nsize) {
	struct memory_stat *stat = (struct memory_stat *)ud;
	if (nsize == 0) {
		stat->mem -= osize;
		free(ptr);
		return NULL;
	} else if (ptr == NULL) {
		// new object
		if (check_limit(stat)) {
			return NULL;
		}
		if (osize >=0 && osize < LUA_NUMTYPES) {
			int id = lua_typeid[osize];
			stat->count[id]++;
		}
		void * ret = malloc(nsize);
		if (ret == NULL) {
			return NULL;
		}
		stat->mem += nsize;
		return ret;
	} else {
		if (osize > nsize && check_limit(stat)) {
			return NULL;
		}
		void * ret = realloc(ptr, nsize);
		if (ret == NULL)
			return NULL;
		stat->mem += nsize;
		stat->mem -= osize;
		return ret;
	}
}

static int
pushstring(lua_State *L) {
	const char * msg = (const char *)lua_touserdata(L, 1);
	lua_settop(L, 1);
	lua_pushstring(L, msg);
	return 1;
}

static void
error_message(lua_State *fromL, lua_State *toL, const char *msg) {
	if (toL == NULL)
		return;
	if (fromL == NULL) {
		lua_pushlightuserdata(toL, (void *)msg);
	} else {
		const char * err = lua_tostring(fromL, -1);
		lua_pushcfunction(toL, pushstring);
		lua_pushlightuserdata(toL, (void *)err);
		if (lua_pcall(toL, 1, 1, 0) != LUA_OK) {
			lua_pop(toL, 1);
			lua_pushlightuserdata(toL, (void *)msg);
		}
	}
}

int
service_init(struct service_pool *p, service_id id, void *ud, size_t sz, void *pL) {
	struct service *S = get_service(p, id);
	assert(S != NULL && S->L == NULL && S->status == SERVICE_STATUS_UNINITIALIZED);
	lua_State *L;
	memset(&S->stat, 0, sizeof(S->stat));
#if LUA_VERSION_NUM == 505
	L = lua_newstate(service_alloc, &S->stat, luaL_makeseed(NULL));
#else
	L = lua_newstate(service_alloc, &S->stat);
#endif
	if (L == NULL)
		return 1;
	lua_pushcfunction(L, init_service);
	lua_pushlightuserdata(L, ud);
	lua_pushinteger(L, sz);
	if (lua_pcall(L, 2, 0, 0) != LUA_OK) {
		error_message(L, pL, "Init lua state error");
		lua_close(L);
		return 1;
	}
	S->msg = queue_new_ptr(p->queue_length);
	if (S->msg == NULL) {
		error_message(NULL, pL, "New queue error");
		lua_close(L);
		return 1;
	}
	S->L = L;
	S->rL = lua_newthread(L);
	luaL_ref(L, LUA_REGISTRYINDEX);

	return 0;
}

size_t
service_memlimit(struct service_pool *p, service_id id, size_t limit) {
	struct service *S = get_service(p, id);
	if (S == NULL || S->L == NULL)
		return (size_t)-1;
	size_t ret = S->stat.limit;
	S->stat.limit = limit;
	return ret;
}

size_t
service_memcount(struct service_pool *p, service_id id, int luatype) {
	struct service *S = get_service(p, id);
	assert(luatype >=0 && luatype < LUA_NUMTYPES);
	if (S == NULL || S->L == NULL)
		return (size_t)-1;
	int type = lua_typeid[luatype];
	return S->stat.count[type];
}

static int
require_cmodule(lua_State *L) {
	const char *name = (const char *)lua_touserdata(L, 1);
	lua_CFunction f = (lua_CFunction)lua_touserdata(L, 2);
	luaL_requiref(L, name, f, 0);
	return 0;
}

int
service_requiref(struct service_pool *p, service_id id, const char *name, void *f, void *pL) {
	struct service *S = get_service(p, id);
	if (S == NULL || S->rL == NULL) {
		error_message(NULL, pL, "requiref : No service");
		return 1;
	}
	lua_State *L = S->rL;
	lua_pushcfunction(L, require_cmodule);
	lua_pushlightuserdata(L, (void *)name);
	lua_pushlightuserdata(L, f);
	if (lua_pcall(L, 2, 0, 0) != LUA_OK) {
		error_message(L, pL, "requiref : pcall error");
		lua_pop(L, 1);
		return 1;
	}
	return 0;
}

int
service_setlabel(struct service_pool *p, service_id id, const char *label) {
	struct service *S = get_service(p, id);
	if (S == NULL)
		return 1;
	strncpy(S->label, label, sizeof(S->label)-1);
	S->label[sizeof(S->label)-1] = '\0';
	return 0;
}

const char *
service_getlabel(struct service_pool *p, service_id id) {
	struct service *S = get_service(p, id);
	if (S == NULL)
		return "<dead service>";
	return S->label;
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

const char *
service_loadstring(struct service_pool *p, service_id id, const char *source, size_t source_sz, const char *chunkname) {
	struct service *S= get_service(p, id);
	if (S == NULL || S->L == NULL)
		return "Init service first";
	lua_State *L = S->L;
	if (luaL_loadbuffer(L, source, source_sz, chunkname) != LUA_OK) {
		const char * r = lua_tostring(S->L, -1);
		S->status = SERVICE_STATUS_DEAD;
		return r;
	}
	S->status = SERVICE_STATUS_IDLE;
	return NULL;
}

#define writestringerror(s,p) \
	(fprintf(stderr, (s), (p)), fflush(stderr))

int
service_resume(struct service_pool *p, service_id id) {
	struct service *S= get_service(p, id);
	if (S == NULL)
		return 1;
	lua_State *L = S->L;
	if (L == NULL)
		return 1;
	int nresults = 0;
	uint64_t start = systime_thread();
	S->clock = start;
	int r = lua_resume(L, NULL, 0, &nresults);
	uint64_t end = systime_thread();
	S->cpucost += end - start;
	if (r == LUA_YIELD) {
		lua_pop(L, nresults);
		return 0;
	}
	if (r == LUA_OK) {
		return 1;
	}
	if (!lua_checkstack(L, LUA_MINSTACK)) {
		writestringerror("%s\n", lua_tostring(L, -1));
		lua_pop(L, 1);
		return 1;
	}
	lua_pushfstring(L, "Service %d error: %s", id.id, lua_tostring(L, -1));
	luaL_traceback(L, L, lua_tostring(L, -1), 0);
	writestringerror("%s\n", lua_tostring(L, -1));
	lua_pop(L, 3);
	return 1;
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

struct strbuff {
	char *buf;
	size_t sz;
};

static size_t
addstr(struct strbuff *b, const char *str, size_t sz) {
	if (b->sz < sz) {
		size_t n = b->sz - 1;
		memcpy(b->buf, str, n);
		b->buf[n] = 0;
		b->buf += n;
		b->sz = 0;
	} else {
		memcpy(b->buf, str, sz);
		b->buf += sz;
		b->sz -= sz;
	}
	return b->sz;
}

#define addliteral(b, s) addstr(b, s, sizeof(s "") -1)

static size_t
addfuncname(lua_Debug *ar, struct strbuff *b) {
	if (*ar->namewhat != '\0')  {	/* is there a name from code? */
		char name[1024];
		int n = snprintf(name, sizeof(name), "%s '%s'", ar->namewhat, ar->name);  /* use it */
		return addstr(b, name, n);
	} else if (*ar->what == 'm') {  /* main? */
		return addliteral(b, "main chunk");
	} else if (*ar->what != 'C') { /* for Lua functions, use <file:line> */
		char name[1024];
		int n = snprintf(name, sizeof(name), "function <%s:%d>", ar->short_src, ar->linedefined);
		return addstr(b, name, n);
	} else { /* nothing left... */
		return addliteral(b, "?");
	}
}

static lua_State *
find_running(lua_State *L) {
	int level = 0;
	lua_Debug ar;
	while (lua_getstack(L, level++, &ar)) {
		lua_getinfo(L, "u", &ar);
		if (ar.nparams > 0) {
			lua_getlocal(L, &ar, 1);
			if (lua_type(L, -1) == LUA_TTHREAD) {
				lua_State *co = lua_tothread(L, -1);
				lua_pop(L, 1);
				return co;
			} else {
				lua_pop(L, 1);
			}
		}
	}
	return L;
}

int
service_backtrace(struct service_pool *p, service_id id, char *buf, size_t sz) {
	struct service *S= get_service(p, id);
	if (S == NULL)
		return 0;
	lua_State *L = find_running(S->L);
	struct strbuff b = { buf, sz };
	int level = 0;
	lua_Debug ar;
	char line[1024];
	while (lua_getstack(L, level++, &ar)) {
		lua_getinfo(L, "Slnt", &ar);
		int n;
		if (ar.currentline <= 0) {
			n = snprintf(line, sizeof(line), "%s: in ", ar.short_src);
		} else {
			n = snprintf(line, sizeof(line), "%s:%d: in ", ar.short_src, ar.currentline);
		}
		if (addstr(&b, line, n) == 0) {
			return sz;
		}
		if (addfuncname(&ar, &b) == 0) {
			return sz;
		}
		if (addliteral(&b, "\n") == 0) {
			return sz;
		}
		if (ar.istailcall) {
			if (addliteral(&b, "(...tail calls...)\n") == 0) {
				return sz;
			}
		}
	}
	return (int)(sz - b.sz);
}

uint64_t
service_cpucost(struct service_pool *p, service_id id) {
	struct service *S= get_service(p, id);
	if (S == NULL)
		return 0;
	return S->cpucost + systime_thread() - S->clock;
}

int
service_binding_get(struct service_pool *p, service_id id) {
	struct service *S= get_service(p, id);
	if (S == NULL)
		return -1;
	return S->binding_thread;
}

void
service_binding_set(struct service_pool *p, service_id id, int worker_thread) {
	struct service *S= get_service(p, id);
	if (S == NULL)
		return;
	S->binding_thread = worker_thread;
}

int
service_sockevent_get(struct service_pool *p, service_id id) {
	struct service *S= get_service(p, id);
	if (S == NULL)
		return -1;
	return S->sockevent_id;
}

void
service_sockevent_init(struct service_pool *p, service_id id, int index) {
	struct service *S= get_service(p, id);
	if (S == NULL)
		return;
	S->sockevent_id = index;
}
