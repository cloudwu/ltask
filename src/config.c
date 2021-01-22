#include "config.h"
#include "sysinfo.h"

#include <lauxlib.h>

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

static const char *
config_getstring(lua_State *L, int index, const char *key) {
	int t = lua_getfield(L, index, key);
	if (t == LUA_TNIL) {
		lua_pop(L, 1);
		return NULL;
	}
	if (t != LUA_TSTRING) {
		luaL_error(L, ".%s should be a string", key);
		return NULL;
	}
	return lua_tostring(L, -1);
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
	config->queue = config_getint(L, index, "queue", DEFAULT_QUEUE);
	config->queue = align_pow2(config->queue);
	config->max_service = config_getint(L, index, "max_service", DEFAULT_MAX_SERVICE);
	config->max_service = align_pow2(config->max_service);
	
	lua_newtable(L);
	lua_pushinteger(L, config->worker);
	lua_setfield(L, -2, "worker");
	lua_pushinteger(L, config->queue);
	lua_setfield(L, -2, "queue");
	lua_pushinteger(L, config->max_service);
	lua_setfield(L, -2, "max_service");

	config->service = config_getstring(L, index, "service");
	if (config->service == NULL) {
		luaL_error(L, "Need .service as a lua filename");
		return;
	}
	lua_setfield(L, -2, "service");
	config->root = config_getstring(L, index, "root");
	if (config->root == NULL) {
		luaL_error(L, "Need .root as a lua filename");
		return;
	}
	lua_setfield(L, -2, "root");
}

