#ifndef log_queue_h
#define log_queue_h

#include <stddef.h>
#include <stdint.h>
#include "service.h"

struct logqueue;

struct logmessage {
	uint64_t timestamp;
	service_id id;
	uint32_t sz;
	void *msg;
};

struct logqueue * logqueue_new();
void logqueue_delete(struct logqueue *);
// 0 : succ
int logqueue_push(struct logqueue *q, struct logmessage *m);
int logqueue_pop(struct logqueue *q, struct logmessage *m);

#endif
