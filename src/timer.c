#include "spinlock.h"
#include "systime.h"
#include "timer.h"

#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <inttypes.h>

#define TIME_NEAR_SHIFT 8
#define TIME_NEAR (1 << TIME_NEAR_SHIFT)
#define TIME_LEVEL_SHIFT 6
#define TIME_LEVEL (1 << TIME_LEVEL_SHIFT)
#define TIME_NEAR_MASK (TIME_NEAR-1)
#define TIME_LEVEL_MASK (TIME_LEVEL-1)

struct timer_node {
	struct timer_node *next;
	uint32_t expire;
};

struct link_list {
	struct timer_node head;
	struct timer_node *tail;
};

struct timer {
	struct link_list n[TIME_NEAR];
	struct link_list t[4][TIME_LEVEL];
	struct spinlock lock;
	uint32_t time;
	uint32_t starttime;
	uint64_t current;
	uint64_t current_point;
	timer_execute_func func;
	void *ud;
};

static inline struct timer_node *
link_clear(struct link_list *list) {
	struct timer_node * ret = list->head.next;
	list->head.next = 0;
	list->tail = &(list->head);

	return ret;
}

static inline void
link(struct link_list *list,struct timer_node *node) {
	list->tail->next = node;
	list->tail = node;
	node->next=0;
}

static void
add_node(struct timer *T,struct timer_node *node) {
	uint32_t time=node->expire;
	uint32_t current_time=T->time;
	
	if ((time|TIME_NEAR_MASK)==(current_time|TIME_NEAR_MASK)) {
		link(&T->n[time&TIME_NEAR_MASK],node);
	} else {
		int i;
		uint32_t mask=TIME_NEAR << TIME_LEVEL_SHIFT;
		for (i=0;i<3;i++) {
			if ((time|(mask-1))==(current_time|(mask-1))) {
				break;
			}
			mask <<= TIME_LEVEL_SHIFT;
		}

		link(&T->t[i][((time>>(TIME_NEAR_SHIFT + i*TIME_LEVEL_SHIFT)) & TIME_LEVEL_MASK)],node);	
	}
}

void
timer_add(struct timer *T,void *arg,size_t sz,int time) {
	struct timer_node *node = (struct timer_node *)malloc(sizeof(*node)+sz);
	memcpy(node+1,arg,sz);

	spinlock_acquire(&T->lock);

		node->expire=time+T->time;
		add_node(T,node);

	spinlock_release(&T->lock);
}

static void
move_list(struct timer *T, int level, int idx) {
	struct timer_node *current = link_clear(&T->t[level][idx]);
	while (current) {
		struct timer_node *temp=current->next;
		add_node(T,current);
		current=temp;
	}
}

static void
timer_shift(struct timer *T) {
	int mask = TIME_NEAR;
	uint32_t ct = ++T->time;
	if (ct == 0) {
		move_list(T, 3, 0);
	} else {
		uint32_t time = ct >> TIME_NEAR_SHIFT;
		int i=0;

		while ((ct & (mask-1))==0) {
			int idx=time & TIME_LEVEL_MASK;
			if (idx!=0) {
				move_list(T, i, idx);
				break;				
			}
			mask <<= TIME_LEVEL_SHIFT;
			time >>= TIME_LEVEL_SHIFT;
			++i;
		}
	}
}

static inline void
dispatch_list(struct timer_node *current, timer_execute_func func, void *ud) {
	do {
		func(ud, (void *)(current+1));
		struct timer_node * temp = current;
		current=current->next;
		free(temp);	
	} while (current);
}

static inline void
timer_execute(struct timer *T, timer_execute_func func, void *ud) {
	int idx = T->time & TIME_NEAR_MASK;
	
	while (T->n[idx].head.next) {
		struct timer_node *current = link_clear(&T->n[idx]);
		spinlock_release(&T->lock);
		// dispatch_list don't need lock T
		dispatch_list(current, func, ud);
		spinlock_acquire(&T->lock);
	}
}

static void 
timer_update_tick(struct timer *T, timer_execute_func func, void *ud) {
	spinlock_acquire(&T->lock);

	// try to dispatch timeout 0 (rare condition)
	timer_execute(T, func, ud);

	// shift time first, and then dispatch timer message
	timer_shift(T);

	timer_execute(T, func, ud);

	spinlock_release(&T->lock);
}

static struct timer *
timer_new() {
	struct timer *r=(struct timer *)malloc(sizeof(struct timer));
	memset(r,0,sizeof(*r));

	int i,j;

	for (i=0;i<TIME_NEAR;i++) {
		link_clear(&r->n[i]);
	}

	for (i=0;i<4;i++) {
		for (j=0;j<TIME_LEVEL;j++) {
			link_clear(&r->t[i][j]);
		}
	}

	spinlock_init(&r->lock);

	r->current = 0;

	return r;
}

static void
timer_release_func(void *ud, void *arg) {
	// do nothing
	(void)ud;
	(void)arg;
}

void
timer_destroy(struct timer *T) {
	if (T == NULL)
		return;
	spinlock_acquire(&T->lock);

	int i,j;
	for (i=0;i<TIME_NEAR;i++) {
		struct timer_node *current = link_clear(&T->n[i]);
		if (current) {
			dispatch_list(current, timer_release_func, NULL);
		}
	}
	for (i=0;i<4;i++) {
		for (j=0;j<TIME_LEVEL;j++) {
			struct timer_node *current = link_clear(&T->t[i][j]);
			if (current) {
				dispatch_list(current, timer_release_func, NULL);
			}
		}
	}
	spinlock_release(&T->lock);
	spinlock_destroy(&T->lock);

	free(T);
}

void
timer_update(struct timer *TI, timer_execute_func func, void *ud) {
	uint64_t cp = systime_mono();
	if(cp < TI->current_point) {
		printf("time diff error: change from %" PRId64 " to %" PRId64, cp, TI->current_point);
		TI->current_point = cp;
	} else if (cp != TI->current_point) {
		uint32_t diff = (uint32_t)(cp - TI->current_point);
		TI->current_point = cp;
		TI->current += diff;
		int i;
		for (i=0;i<diff;i++) {
			timer_update_tick(TI, func, ud);
		}
	}
}

uint32_t
timer_starttime(struct timer *TI) {
	return TI->starttime;
}

uint64_t 
timer_now(struct timer *TI) {
	return TI->current;
}

struct timer *
timer_init() {
	struct timer *TI = timer_new();
	uint64_t walltime = systime_wall();
	TI->starttime = walltime/100;
	TI->current = walltime % 100;
	TI->current_point = systime_mono();
	return TI;
}
