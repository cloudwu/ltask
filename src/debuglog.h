#ifndef debug_log_h
#define debug_log_h

#include <stdio.h>

struct debug_logger;

struct debug_logger * dlog_new(const char *name, int id);
void dlog_flush(struct debug_logger *log);
void dlog_write(struct debug_logger *log, const char *fmt, ...);
void dlog_writefile(FILE *f);
void dlog_close(FILE *f);

#endif
