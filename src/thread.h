#ifndef ltask_thread_h
#define ltask_thread_h

struct thread {
	void (*func)(void *);
	void *ud;
};

static void thread_join(struct thread * threads, int n);

#if defined(_MSC_VER) || defined(__MINGW32__) || defined(__MINGW64__)

#include <windows.h>

static DWORD inline WINAPI
thread_function(LPVOID lpParam) {
	struct thread * t = (struct thread *)lpParam;
	t->func(t->ud);
	return 0;
}

static inline void
thread_join(struct thread * threads, int n) {
	int i;
	struct thread *mainthread = &threads[0];	// Use main thread for the 1st thread
	++threads;
	--n;
	HANDLE *thread_handle = (HANDLE *)HeapAlloc(GetProcessHeap(),HEAP_ZERO_MEMORY,n*sizeof(HANDLE));
	for (i=0;i<n;i++) {
		thread_handle[i] = CreateThread(NULL, 0, thread_function, (LPVOID)&threads[i], 0, NULL);
		if (thread_handle[i] == NULL) {
			HeapFree(GetProcessHeap(), 0, thread_handle);
			return;
		}
	}
	mainthread->func(mainthread->ud);
	WaitForMultipleObjects(n, thread_handle, TRUE, INFINITE);
	for (i=0;i<n;i++) {
		CloseHandle(thread_handle[i]);
	}
	HeapFree(GetProcessHeap(), 0, thread_handle);
}

#else

#include <pthread.h>

static inline void *
thread_function(void * args) {
	struct thread * t = (struct thread *)args;
	t->func(t->ud);
	return NULL;
}

static inline void
thread_join(struct thread *threads, int n) {
	struct thread *mainthread = &threads[0];	// Use main thread for the 1st thread
	++threads;
	--n;
	pthread_t pid[n];
	int i;
	for (i=0;i<n;i++) {
		if (pthread_create(&pid[i], NULL, thread_function, &threads[i])) {
			return;
		}
	}
	mainthread->func(mainthread->ud);
	for (i=0;i<n;i++) {
		pthread_join(pid[i], NULL); 
	}
}

#endif

#endif
