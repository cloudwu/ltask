#ifndef ltask_spinlock_h
#define ltask_spinlock_h

#if defined(_MSC_VER) || defined(__MINGW32__) || defined(__MINGW64__)

#include <windows.h>

struct spinlock {
	SRWLOCK lock;
};

static inline int
spinlock_init(struct spinlock *sp) {
	memset(sp, 0, sizeof(*sp));
	return 0;
}

static inline int
spinlock_destroy(struct spinlock *sp) {
	return 0;
}

static inline int
spinlock_acquire(struct spinlock *sp) {
	AcquireSRWLockExclusive(&sp->lock);
	return 0;
}

static inline int
spinlock_release(struct spinlock *sp) {
	ReleaseSRWLockExclusive(&sp->lock);
	return 0;
}

static inline int
spinlock_try(struct spinlock *sp) {
	return !TryAcquireSRWLockExclusive(&sp->lock);
}

#else

#include <pthread.h>

struct spinlock {
	pthread_mutex_t mutex;
};

static inline int
spinlock_init(struct spinlock *lock) {
	return pthread_mutex_init(&lock->mutex, NULL);
}

static inline int
spinlock_destroy(struct spinlock *lock) {
	return pthread_mutex_destroy(&lock->mutex);
}

static inline int
spinlock_acquire(struct spinlock *lock) {
	return pthread_mutex_lock(&lock->mutex);
}

static inline int
spinlock_release(struct spinlock *lock) {
	return pthread_mutex_unlock(&lock->mutex);
}

static inline int
spinlock_try(struct spinlock *lock) {
	return pthread_mutex_trylock(&lock->mutex);
}

#endif

#endif
