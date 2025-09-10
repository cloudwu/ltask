#define LUA_LIB

#include <lua.h>
#include <lauxlib.h>
#include "queue.h"

static int
lmqueue_new(lua_State *L) {
	int size = luaL_checkinteger(L, 1);
	struct queue * q = queue_new_ptr(size);
	lua_pushlightuserdata(L, q);
	return 1;
}

static int
lmqueue_delete(lua_State *L) {
	luaL_checktype(L, 1, LUA_TLIGHTUSERDATA);
	struct queue *q = (struct queue *)lua_touserdata(L, 1);
	queue_delete(q);
	return 0;
}

static int
lmqueue_send(lua_State *L) {
	luaL_checktype(L, 1, LUA_TLIGHTUSERDATA);
	luaL_checktype(L, 2, LUA_TLIGHTUSERDATA);
	struct queue *q = (struct queue *)lua_touserdata(L, 1);
	void *v = lua_touserdata(L, 2);
	if (queue_push_ptr(q, v)) {
		// block
		return 0;
	}
	lua_pushboolean(L, 1);
	return 1;
}

static int
lmqueue_recv(lua_State *L) {
	luaL_checktype(L, 1, LUA_TLIGHTUSERDATA);
	struct queue *q = (struct queue *)lua_touserdata(L, 1);
	void *msg = queue_pop_ptr(q);
	if (msg == NULL)
		return 0;
	lua_pushlightuserdata(L, msg);
	return 1;
}

LUAMOD_API int
luaopen_ltask_mqueue(lua_State *L) {
	luaL_Reg l[] = {
		{ "new", lmqueue_new },
		{ "delete", lmqueue_delete },
		{ "send", lmqueue_send },
		{ "recv", lmqueue_recv },
		{ NULL, NULL },
	};
	luaL_newlib(L, l);
	return 1;
}
