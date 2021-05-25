#include "debuglog.h"
#include "atomic.h"
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <assert.h>

#define CHUNKSIZE 8000

struct log_item {
	int id;
	unsigned int size;
};

struct log_chunk {
	struct log_chunk *last;
	unsigned int size;
	char buffer[CHUNKSIZE];
};

struct debug_logger {
	struct debug_logger *link;
	struct log_chunk *c;
	const char * name;
};

struct logger_all {
	struct debug_logger *logger;
//	int write_id;
	atomic_int id;
};

static struct logger_all G;

static struct log_chunk *
new_chunk() {
	struct log_chunk * c = (struct log_chunk *)malloc(sizeof(struct log_chunk));
	c->last = NULL;
	c->size = 0;
	return c;
}

struct debug_logger *
dlog_new(const char *name, int id) {
	struct debug_logger * logger = (struct debug_logger *)malloc(sizeof(struct debug_logger));
	logger->c = new_chunk();
	if (id < 0) {
		logger->name = strdup(name);
	} else {
		char namestring[128];
		int n = snprintf(namestring, 127, "%s%d", name, id);
		namestring[n] = 0;
		logger->name = strdup(namestring);
	}
	logger->link = G.logger;
	G.logger = logger;
	return logger;
}

void
dlog_flush(struct debug_logger *logger) {
	if (logger->c->size == 0)
		return;
	struct log_chunk *c = new_chunk();
	c->last = logger->c;
	logger->c = c;
};

void
dlog_write(struct debug_logger *logger, const char *fmt, ...) {
	struct log_item header;
	char tmp[CHUNKSIZE];
	va_list ap;
	va_start(ap, fmt);
	header.size = vsnprintf(tmp, CHUNKSIZE, fmt, ap);
	header.id = atomic_int_inc(&G.id);
	va_end(ap);
	unsigned int sz = header.size + sizeof(struct log_item);
	unsigned int new_sz = logger->c->size + sz;
	if (new_sz > CHUNKSIZE) {
		dlog_flush(logger);
	}
	unsigned int offset = logger->c->size;
	char *ptr = logger->c->buffer + offset;
	memcpy(ptr, &header, sizeof(header));
	ptr += sizeof(header);
	memcpy(ptr, tmp, header.size);
	logger->c->size = sz + offset;
}

// todo : sort by id

static void
writefile_chunk(FILE *f, const char *name, struct log_chunk *c) {
	unsigned int sz = c->size;
	char * ptr = c->buffer;
	while (sz > 0) {
		struct log_item header;
		memcpy(&header, ptr, sizeof(header));
		ptr += sizeof(header);
		sz -= sizeof(header);
		fprintf(f, "[%06u:%s] %.*s\n", header.id, name, header.size, ptr);
		fflush(f);
		ptr += header.size;
		sz -= header.size;
	}
}

static void
writefile_all_chunk(FILE *f, const char *name, struct log_chunk *c) {
	if (c->last) {
		writefile_all_chunk(f, name, c->last);
	}
	writefile_chunk(f, name, c);
	free(c);
}

static void
writefile_logger(FILE *f, struct debug_logger *logger) {
	struct log_chunk *c = logger->c;
	if (c->last) {
		writefile_all_chunk(f, logger->name, c->last);
		c->last = NULL;
	}
}

static void
writefile_all_logger(FILE *f, struct debug_logger *logger) {
	if (logger == NULL)
		return;
	dlog_flush(logger);
	writefile_logger(f, logger);
	writefile_all_logger(f, logger->link);
}

void
dlog_writefile(FILE *f) {
	if (f == NULL)
		f = stdout;
	writefile_all_logger(f, G.logger);
}

void
dlog_close(FILE *f) {
	if (f == NULL)
		f = stdout;
	struct debug_logger *logger = G.logger;
	while (logger) {
		dlog_flush(logger);
		writefile_all_logger(f, logger);
		struct debug_logger *next = logger->link;
		free(logger->c);
		free((void *)logger->name);
		free(logger);
		logger = next;
	}
	G.logger = NULL;
}

#ifdef TEST_MAIN

int
main() {
	struct debug_logger * logger = dlog_new("test");
	struct debug_logger * logger2 = dlog_new("logger");

	dlog_write(logger, "Hello World");
	dlog_write(logger2, "debug logger");
	dlog_writefile(stdout);
	dlog_close(stdout);
	return 0;
}

#endif
