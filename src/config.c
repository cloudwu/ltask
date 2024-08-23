#include "config.h"
#include "sysinfo.h"

#include <lauxlib.h>
#include <string.h>

static int
config_getint(lua_State *L, int index, const char *key, int opt) {
	int t = lua_getfield(L, index, key);
	if (t == LUA_TNIL) {
		lua_pop(L, 1);
		return opt;
	}
	if (!lua_isinteger(L, -1)) {
		return luaL_error(L, ".%s should be an integer", key);
	}
	int r = lua_tointeger(L, -1);
	lua_pop(L, 1);
	return r;
}

static inline int
align_pow2(int x) {
	int r = 1;
	while (r < x) {
		r *= 2;
	}
	return r;
}

void
config_load(lua_State *L, int index, struct ltask_config *config) {
	luaL_checktype(L, index, LUA_TTABLE);
	config->worker = config_getint(L, index, "worker", 0);
	int ncores = sysinfo_ncores();
	if (ncores <= 1) {
		luaL_error(L, "Need at least 2 cores");
		return;
	}
	if (config->worker == 0) {
		config->worker = ncores - 1;
	}
	if (config->worker > MAX_WORKER) {
		config->worker = MAX_WORKER;
	}
	config->queue = config_getint(L, index, "queue", DEFAULT_QUEUE);
	config->queue = align_pow2(config->queue);
	config->queue_sending = config_getint(L, index, "queue_sending", DEFAULT_QUEUE_SENDING);
	config->queue_sending = align_pow2(config->queue_sending);
	config->max_service = config_getint(L, index, "max_service", DEFAULT_MAX_SERVICE);
	config->external_queue = config_getint(L, index, "external_queue", 0);
	config->max_service = align_pow2(config->max_service);
	if (lua_getfield(L, index, "crashlog") != LUA_TSTRING) {
		config->crashlog[0] = 0;
	} else {
		size_t sz;
		const char *log = lua_tolstring(L, -1, &sz);
		if (sz >= sizeof(config->crashlog)) {
			// filename is too long
			config->crashlog[0] = 0;
		} else {
			memcpy(config->crashlog, log, sz+1);
		}
	}
	
	lua_pushinteger(L, config->worker);
	lua_setfield(L, index, "worker");
	lua_pushinteger(L, config->queue);
	lua_setfield(L, index, "queue");
	lua_pushinteger(L, config->max_service);
	lua_setfield(L, index, "max_service");
	lua_pushvalue(L, index);
}

