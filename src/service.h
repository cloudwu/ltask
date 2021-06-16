#ifndef ltask_service_h
#define ltask_service_h

#include <stddef.h>

#define LTASK_KEY "LTASK_ID"

#define SERVICE_ID_SYSTEM 0
#define SERVICE_ID_ROOT 1

#define SERVICE_STATUS_UNINITIALIZED 0
#define SERVICE_STATUS_IDLE 1
#define SERVICE_STATUS_SCHEDULE 2
#define SERVICE_STATUS_RUNNING 3
#define SERVICE_STATUS_DONE 4
#define SERVICE_STATUS_DEAD 5
#define SERVICE_STATUS_EXCLUSIVE 6

struct service_pool;
struct ltask_config;
struct message;

typedef struct {
	unsigned int id;
} service_id;

struct service_pool * service_create(struct ltask_config *config);
void service_destory(struct service_pool *p);
service_id service_new(struct service_pool *p, unsigned int id);
// 0 succ
int service_init(struct service_pool *p, service_id id, void *ud, size_t sz);
int service_requiref(struct service_pool *p, service_id id, const char *name, void *f);
int service_setp(struct service_pool *p, service_id id, const char *key, void *value);
int service_sets(struct service_pool *p, service_id id, const char *key, const char *value);
// 0 succ
int service_hang(struct service_pool *p, service_id id);
void service_send_signal(struct service_pool *p, service_id id);
void service_close(struct service_pool *p, service_id id);
void service_delete(struct service_pool *p, service_id id);
const char * service_loadfile(struct service_pool *p, service_id id, const char *filename);
const char * service_loadstring(struct service_pool *p, service_id id, const char *source);
// 0 yield , 1 term or error
int service_resume(struct service_pool *p, service_id id, int thread_id);
int service_thread_id(struct service_pool *p, service_id id);
// 0 succ, 1 blocked, -1 not exist
int service_push_message(struct service_pool *p, service_id id, struct message *msg);
struct message * service_pop_message(struct service_pool *p, service_id id);
int service_has_message(struct service_pool *p, service_id id);
int service_status_get(struct service_pool *p, service_id id);
void service_status_set(struct service_pool *p, service_id id, int status);
// 0 succ
int service_send_message(struct service_pool *p, service_id id, struct message *msg);
struct message * service_message_out(struct service_pool *p, service_id id);
void service_write_receipt(struct service_pool *p, service_id id, int receipt, struct message *bounce);
struct message * service_read_receipt(struct service_pool *p, service_id id, int *receipt);

#endif
