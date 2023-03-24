#ifndef ltask_cond_h
#define ltask_cond_h

#if defined(_MSC_VER) || defined(__MINGW32__) || defined(__MINGW64__)

#include <windows.h>

struct cond {
    CONDITION_VARIABLE c;
    SRWLOCK lock;
    int flag;
};

static inline void
cond_create(struct cond *c) {
	memset(c, 0, sizeof(*c));
}

static inline void
cond_release(struct cond *c) {
    (void)c;
}

static inline void
cond_trigger_begin(struct cond *c) {
    AcquireSRWLockExclusive(&c->lock);
	c->flag = 1;
}

static inline void
cond_trigger_end(struct cond *c, int trigger) {
    if (trigger) {
	    WakeConditionVariable(&c->c);
    } else {
        c->flag = 0;
    }
	ReleaseSRWLockExclusive(&c->lock);
}

static inline void
cond_wait_begin(struct cond *c) {
	AcquireSRWLockExclusive(&c->lock);
}

static inline void
cond_wait_end(struct cond *c) {
	c->flag = 0;
    ReleaseSRWLockExclusive(&c->lock);
}

static inline void
cond_wait(struct cond *c) {
	while (!c->flag)
        SleepConditionVariableSRW(&c->c, &c->lock, INFINITE, 0);
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
