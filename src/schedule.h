#ifndef ltask_schedule_h
#define ltask_schedule_h

#include "service.h"

struct ltask;

void schedule_dispatch(struct ltask *task, service_id id, int worker_id);

#endif
