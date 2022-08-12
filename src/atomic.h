#ifndef ltask_atomic_h
#define ltask_atomic_h

#if defined(_MSC_VER) || defined(USE_WINATOMIC)

#include <windows.h>

typedef struct {
	LONG volatile v;
} atomic_int;

typedef struct {
	LPVOID volatile v;
} atomic_ptr;

#define ATOMIC_VAR_INIT(v) { v }

static inline void
atomic_int_init(atomic_int *aint, int v) {
	aint->v = (LONG)v;
	MemoryBarrier();
}

static inline int
atomic_int_load(atomic_int *aint) {
	MemoryBarrier();
	return (int)aint->v;
}

static inline void
atomic_int_store(atomic_int *aint, int v) {
	aint->v = (LONG)v;
	MemoryBarrier();
}

static inline int
atomic_int_inc(atomic_int *aint) {
	return (int)InterlockedIncrement(&aint->v);
}

static inline int
atomic_int_dec(atomic_int *aint) {
	return (int)InterlockedDecrement(&aint->v);
}

static inline int
atomic_int_cas(atomic_int *aint, int oval, int nval) {
	return (InterlockedCompareExchange(&aint->v, nval, oval) == oval);
}

static inline void
atomic_ptr_init(atomic_ptr *aptr, void *v) {
	aptr->v = v;
	MemoryBarrier();
}

static inline void *
atomic_ptr_load(atomic_ptr *aptr) {
	MemoryBarrier();
	return aptr->v;
}

static inline void
atomic_ptr_store(atomic_ptr *aptr, void *v) {
	aptr->v = v;
	MemoryBarrier();
}

static inline int
atomic_ptr_cas(atomic_ptr *aptr, void *oval, void *nval) {
	return (InterlockedCompareExchangePointer(&aptr->v, nval, oval) == oval);
}

#else

#include <stdatomic.h>
#include <stdint.h>
#include <stddef.h>

typedef atomic_uintptr_t atomic_ptr;

static inline void
atomic_int_init(atomic_int *aint, int v) {
	atomic_init(aint, v);
}

static inline int
atomic_int_load(atomic_int *aint) {
	return atomic_load(aint);
}

static inline void
atomic_int_store(atomic_int *aint, int v) {
	atomic_store(aint, v);
}

static inline int
atomic_int_inc(atomic_int *aint) {
	return atomic_fetch_add(aint, 1)+1;
}

static inline int
atomic_int_dec(atomic_int *aint) {
	return atomic_fetch_sub(aint, 1)-1;
}

static inline int
atomic_int_cas(atomic_int *aint, int oval, int nval) {
	return atomic_compare_exchange_weak(aint, &oval, nval);
}

static inline void
atomic_ptr_init(atomic_ptr *aptr, void *v) {
	atomic_init(aptr, (uintptr_t)v);
}

static inline void *
atomic_ptr_load(atomic_ptr *aptr) {
	return (void *)atomic_load(aptr);
}

static inline void
atomic_ptr_store(atomic_ptr *aptr, void *v) {
	atomic_store(aptr, (uintptr_t)v);
}

static inline int
atomic_ptr_cas(atomic_ptr *aptr, void *oval, void *nval) {
	uintptr_t temp = (uintptr_t)oval;
	return atomic_compare_exchange_weak(aptr, &temp, (uintptr_t)nval);
}

#endif

#endif
