#ifndef ltask_thread_h
#define ltask_thread_h

struct thread {
	void (*func)(void *);
	void *ud;
};

static void thread_join(struct thread * threads, int n);
static void * thread_run(struct thread thread);
static void thread_wait(void *pid);

#if defined(_MSC_VER) || defined(__MINGW32__) || defined(__MINGW64__)

#include <windows.h>

#if defined(DEBUGTHREADNAME)

static void inline
thread_setname(const char* name) {
	typedef HRESULT (WINAPI *SetThreadDescriptionProc)(HANDLE, PCWSTR);
	SetThreadDescriptionProc SetThreadDescription = (SetThreadDescriptionProc)GetProcAddress(GetModuleHandleW(L"kernel32.dll"), "SetThreadDescription");
	if (SetThreadDescription) {
		size_t size = (strlen(name)+1) * sizeof(wchar_t);
		wchar_t* wname = (wchar_t*)_alloca(size);
		mbstowcs(wname, name, size-2);
		SetThreadDescription(GetCurrentThread(), wname);
	}
#if defined(_MSC_VER)
	const DWORD MS_VC_EXCEPTION = 0x406D1388;
#pragma pack(push, 8)
	struct ThreadNameInfo {
		DWORD  type;
		LPCSTR name;
		DWORD  id;
		DWORD  flags;
	};
#pragma pack(pop)
	struct ThreadNameInfo info;
	info.type  = 0x1000;
	info.name  = name;
	info.id    = GetCurrentThreadId();
	info.flags = 0;
	__try {
		RaiseException(MS_VC_EXCEPTION, 0, sizeof(info)/sizeof(ULONG_PTR), (ULONG_PTR*)&info);
	} __except(EXCEPTION_EXECUTE_HANDLER) {}
#endif
}

#else

#define thread_setname(NAME)

#endif

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

static DWORD inline WINAPI
thread_function_run(LPVOID lpParam) {
	struct thread * t = (struct thread *)lpParam;
	t->func(t->ud);
	HeapFree(GetProcessHeap(), 0, t);
	return 0;
}

static inline void *
thread_run(struct thread thread) {
	struct thread * t = (struct thread *)HeapAlloc(GetProcessHeap(),HEAP_ZERO_MEMORY, sizeof(*t));
	*t = thread;
	HANDLE h = CreateThread(NULL, 0, thread_function_run, (LPVOID)t, 0, NULL);
	if (h == NULL) {
		HeapFree(GetProcessHeap(), 0, t);
	}
	return (void *)h;
}

static inline void
thread_wait(void *pid) {
	HANDLE h = (HANDLE)pid;
	WaitForSingleObject(h, INFINITE);
}

#else

#include <pthread.h>
#include <stdlib.h>

#if defined(DEBUGTHREADNAME)

#if defined(__linux__)
#	define LTASK_GLIBC (__GLIBC__ * 100 + __GLIBC_MINOR__)
#	if LTASK_GLIBC < 212
#		include <sys/prctl.h>
#	endif
#endif

#if defined(__FreeBSD__) || defined(__OpenBSD__)
#	include <pthread_np.h>
#endif

static void inline
thread_setname(const char* name) {
#if defined(__APPLE__)
	pthread_setname_np(name);
#elif defined(__linux__)
#	if LTASK_GLIBC >= 212
		pthread_setname_np(pthread_self(), name);
#	else
		prctl(PR_SET_NAME, name, 0, 0, 0);
#	endif
#elif defined(__NetBSD__)
	pthread_setname_np(pthread_self(), "%s", (void*)name);
#elif defined(__FreeBSD__) || defined(__OpenBSD__)
	pthread_set_name_np(pthread_self(), name);
#endif
}

#else

#define thread_setname(NAME)

#endif

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

static inline void *
thread_function_run(void * args) {
	struct thread * t = (struct thread *)args;
	t->func(t->ud);
	free(t);
	return NULL;
}

static inline void *
thread_run(struct thread thread) {
	pthread_t pid;
	struct thread *h = (struct thread *)malloc(sizeof(*h));
	*h = thread;
	if (pthread_create(&pid, NULL, thread_function_run, h)) {
		free(h);
		return NULL;
	}
	return (void *)pid;
}

static inline void
thread_wait(void *p) {
	pthread_t pid = (pthread_t)p;
	pthread_join(pid, NULL);
}

#endif

#if defined(DEBUGTHREADNAME)
static void inline
thread_setnamef(const char* fmt, ...) {
	char buf[256];
	va_list ap;
	va_start(ap, fmt);
	vsnprintf(buf, sizeof(buf), fmt, ap);
	va_end(ap);
	thread_setname(buf);
}
#else
#define thread_setnamef(...)
#endif

#endif
