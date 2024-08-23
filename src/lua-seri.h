#ifndef LUA_SERIALIZE_H
#define LUA_SERIALIZE_H

#include <lua.h>
#include <stddef.h>

// todo: raise OOM error

int luaseri_pack(lua_State *L);
int luaseri_unpack(lua_State *L);
int luaseri_unpack_remove(lua_State *L);
int luaseri_remove(lua_State *L);

void * seri_packstring(const char * str, int sz, void *p, size_t *output_sz);

#endif
