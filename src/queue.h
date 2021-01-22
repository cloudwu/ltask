#ifndef ltask_queue_h
#define ltask_queue_h

#include "atomic.h"

// Allow only one reader and one writer 
struct queue;

struct queue * queue_new_int(int size);
struct queue * queue_new_ptr(int size);
void queue_delete(struct queue *q);
// 0 succ
int queue_push_int(struct queue *q, int v);
int queue_pop_int(struct queue *q);
int queue_push_ptr(struct queue *q, void *v);
void * queue_pop_ptr(struct queue *q);
int queue_length(struct queue *q);

#endif
