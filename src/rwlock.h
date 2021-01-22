#ifndef ltask_rwlock_h
#define ltask_rwlock_h

#if defined(_MSC_VER) || defined(__MINGW32__) || defined(__MINGW64__)

#include <windows.h>

struct rwlock {
	SRWLOCK rw;
};

static inline int
rwlock_init(struct rwlock *lock) {
	InitializeSRWLock(&lock->rw);
	return 0;
}

static inline int
rwlock_destroy(struct rwlock *lock) {
	(void)lock;
	return 0;
}

static inline int
rwlock_acquire_read(struct rwlock *lock) {
	AcquireSRWLockShared(&lock->rw);
	return 0;
}

static inline int
rwlock_acquire_write(struct rwlock *lock) {
	AcquireSRWLockExclusive(&lock->rw);
	return 0;
}

static inline int
rwlock_release_read(struct rwlock *lock) {
	ReleaseSRWLockShared(&lock->rw);
	return 0;
}

static inline int
rwlock_release_write(struct rwlock *lock) {
	ReleaseSRWLockExclusive(&lock->rw);
	return 0;
}

#else

#include <pthread.h>

struct rwlock {
	pthread_rwlock_t rw;
};

static inline int
rwlock_init(struct rwlock *lock) {
	return pthread_rwlock_init(&lock->rw);
}

static inline int
rwlock_destroy(struct rwlock *lock) {
	return pthread_rwlock_destroy(&lock->rw);
}

static inline int
rwlock_acquire_read(struct rwlock *lock) {
	return pthread_rwlock_rdlock(&lock->rw);
}

static inline int
rwlock_acquire_write(struct rwlock *lock) {
	return pthread_rwlock_wrlock(&lock->rw);
}

static inline int
rwlock_release_read(struct rwlock *lock) {
	return pthread_rwlock_unlock(&lock->rw);
}

static inline int
rwlock_release_write(struct rwlock *lock) {
	return pthread_rwlock_unlock(&lock->rw);
}

#endif

#endif
