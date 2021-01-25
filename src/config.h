#ifndef ltask_config_h
#define ltask_config_h

#include <lua.h>

#define DEFAULT_MAX_SERVICE 65536
#define DEFAULT_QUEUE 4096
#define MAX_WORKER 256

struct ltask_config {
	int worker;
	int queue;
	int max_service;
	const char * service;
	const char * root;
};

void config_load(lua_State *L, int index, struct ltask_config *config);

#endif
