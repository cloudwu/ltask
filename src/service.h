#ifndef ltask_service_h
#define ltask_service_h

#define SERVICE_ID_SYSTEM 0
#define SERVICE_ID_ROOT 1

#define SERVICE_STATUS_UNINITIALIZED 0
#define SERVICE_STATUS_IDLE 1
#define SERVICE_STATUS_SCHEDULE 2
#define SERVICE_STATUS_READY 3
#define SERVICE_STATUS_RUNNING 4
#define SERVICE_STATUS_DONE 5
#define SERVICE_STATUS_DEAD 6


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
int service_init(struct service_pool *p, service_id id);
void service_delete(struct service_pool *p, service_id id);
const char * service_load(struct service_pool *p, service_id id, const char *filename);
void service_resume(struct service_pool *p, service_id id);
// 0 succ, 1 blocked, -1 not exist
int service_message(struct service_pool *p, service_id id, struct message *msg);
int service_status_get(struct service_pool *p, service_id id);
void service_status_set(struct service_pool *p, service_id id, int status);
struct message * service_message_out(struct service_pool *p, service_id id);
void service_message_resp(struct service_pool *p, service_id id, struct message *resp);
int service_message_count(struct service_pool *p, service_id id);

#endif
