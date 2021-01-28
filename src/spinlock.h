#ifndef ltask_spinlock_h
#define ltask_spinlock_h

#if defined(_MSC_VER) || defined(__MINGW32__) || defined(__MINGW64__)

#include <windows.h>

struct spinlock {
	CRITICAL_SECTION cs;
};

static inline int
spinlock_init(struct spinlock *lock) {
	InitializeCriticalSection(&lock->cs);
	return 0;
}

static inline int
spinlock_destroy(struct spinlock *lock) {
	DeleteCriticalSection(&lock->cs);
	return 0;
}

static inline int
spinlock_acquire(struct spinlock *lock) {
	EnterCriticalSection(&lock->cs);
	return 0;
}

static inline int
spinlock_release(struct spinlock *lock) {
	LeaveCriticalSection(&lock->cs);
	return 0;
}

static inline int
spinlock_try(struct spinlock *lock) {
	return !TryEnterCriticalSection(&lock->cs);
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
