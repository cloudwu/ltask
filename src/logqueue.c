#include "logqueue.h"
#include "spinlock.h"
#include <stdlib.h>

struct log_item {
	struct log_item *next;
	struct logmessage msg;
};

struct logqueue {
	struct log_item *freelist;
	struct log_item *head;
	struct log_item *tail;
	struct spinlock lock;
};

struct logqueue *
logqueue_new() {
	struct logqueue *q = (struct logqueue *)malloc(sizeof(*q));
	if (spinlock_init(&q->lock)) {
		free(q);
		return NULL;
	}
	q->freelist = NULL;
	q->head = NULL;
	q->tail = NULL;
	return q;
}

static void
free_items(struct log_item *item) {
	while (item) {
		struct log_item *temp = item;
		item = item->next;
		free(temp);
	}
}

void
logqueue_delete(struct logqueue *q) {
	struct logmessage m;
	while (!logqueue_pop(q, &m)) {
		free(m.msg);
	}
	spinlock_destroy(&q->lock);
	free_items(q->freelist);
	free(q);
}

static inline struct log_item *
alloc_item(struct logqueue *q) {
	struct log_item * ret = q->freelist;
	if (ret == NULL) {
		ret = (struct log_item *)malloc(sizeof(*ret));
	} else {
		q->freelist = ret->next;
	}
	return ret;
}

int
logqueue_push(struct logqueue *q, struct logmessage *m) {
	spinlock_acquire(&q->lock);
	struct log_item *item = alloc_item(q);
	if (item == NULL) {
		spinlock_release(&q->lock);
		return 1;
	}
	if (q->head == NULL) {
		q->head = q->tail = item;
		item->next = NULL;
	} else {
		q->tail->next = item;
		item->next = NULL;
		q->tail = item;
	}
	spinlock_release(&q->lock);
	item->msg = *m;
	return 0;
}

int
logqueue_pop(struct logqueue *q, struct logmessage *m) {
	spinlock_acquire(&q->lock);
	if (q->head == NULL) {
		spinlock_release(&q->lock);
		return 1;
	}
	struct log_item * ret = q->head;
	q->head = ret->next;
	if (q->head == NULL) {
		q->tail = NULL;
	}
	ret->next = q->freelist;
	q->freelist = ret;
	spinlock_release(&q->lock);
	*m = ret->msg;
	return 0;
}
