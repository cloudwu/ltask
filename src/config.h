#ifndef ltask_config_h
#define ltask_config_h

#include <lua.h>

#define DEFAULT_MAX_SERVICE 65536
#define DEFAULT_QUEUE 4096
#define DEFAULT_QUEUE_SENDING DEFAULT_QUEUE
#define MAX_WORKER 256
#define MAX_SOCKEVENT 16

struct ltask_config {
	int worker;
	int queue;
	int queue_sending;
	int max_service;
	char crashlog[128];
};

void config_load(lua_State *L, int index, struct ltask_config *config);

#endif
