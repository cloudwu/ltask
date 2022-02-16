#ifndef ltask_cond_h
#define ltask_cond_h

#if defined(_MSC_VER) || defined(__MINGW32__) || defined(__MINGW64__)

struct cond {
    CONDITION_VARIABLE c;
    CRITICAL_SECTION lock;
    int flag;
};

static inline void
cond_create(struct cond *c) {
    InitializeCriticalSection(&c->lock);
    InitializeConditionVariable(&c->c);
	c->flag = 0;    
}

static inline void
cond_release(struct cond *c) {
    DeleteCriticalSection(&c->lock);
}

static inline void
cond_trigger_begin(struct cond *c) {
    EnterCriticalSection(&c->lock);
	c->flag = 1;
}

static inline void
cond_trigger_end(struct cond *c, int trigger) {
    if (trigger) {
	    WakeConditionVariable(&c->c);
    } else {
        c->flag = 0;
    }
	LeaveCriticalSection(&c->lock);
}

static inline void
cond_wait_begin(struct cond *c) {
	EnterCriticalSection(&c->lock);
}

static inline void
cond_wait_end(struct cond *c) {
	c->flag = 0;
    LeaveCriticalSection(&c->lock);
}

static inline void
cond_wait(struct cond *c) {
	while (!c->flag)
        SleepConditionVariableCS(&c->c, &c->lock, INFINITE);
}

#else

#include <pthread.h>

struct cond {
    pthread_cond_t c;
    pthread_mutex_t lock;
    int flag;
};

static inline void
cond_create(struct cond *c) {
	pthread_mutex_init(&c->lock, NULL);
	pthread_cond_init(&c->c, NULL);
	c->flag = 0;    
}

static inline void
cond_release(struct cond *c) {
	pthread_mutex_destroy(&c->lock);
	pthread_cond_destroy(&c->c);
}

static inline void
cond_trigger_begin(struct cond *c) {
	pthread_mutex_lock(&c->lock);
	c->flag = 1;
}

static inline void
cond_trigger_end(struct cond *c, int trigger) {
    if (trigger) {
	    pthread_cond_signal(&c->c);
    } else {
        c->flag = 0;
    }
	pthread_mutex_unlock(&c->lock);
}

static inline void
cond_wait_begin(struct cond *c) {
	pthread_mutex_lock(&c->lock);
}

static inline void
cond_wait_end(struct cond *c) {
	c->flag = 0;
    pthread_mutex_unlock(&c->lock);
}

static inline void
cond_wait(struct cond *c) {
	while (!c->flag)
		pthread_cond_wait(&c->c, &c->lock);
}

#endif

#endif
