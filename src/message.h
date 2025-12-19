#ifndef ltask_message_h
#define ltask_message_h

#include <stddef.h>
#include "service.h"

typedef unsigned int session_t;

#define MESSAGE_SYSTEM 0
#define MESSAGE_REQUEST 1
#define MESSAGE_RESPONSE 2
#define MESSAGE_ERROR 3
#define MESSAGE_SIGNAL 4
#define MESSAGE_IDLE 5

#define MESSAGE_RECEIPT_NONE 0
#define MESSAGE_RECEIPT_DONE 1
#define MESSAGE_RECEIPT_ERROR 2
#define MESSAGE_RECEIPT_BLOCK 3
#define MESSAGE_RECEIPT_RESPONSE 4

// If to == 0, it's a schedule message. It should be post from root service (1).
// type is MESSAGE_SCHEDULE_* from is the parameter (for DEL service_id).
#define MESSAGE_SCHEDULE_NEW 0
#define MESSAGE_SCHEDULE_DEL 1

struct message {
	service_id from;
	service_id to;
	session_t session;
	int type;
	void *msg;
	size_t sz;
};

struct message * message_new(struct message *msg);
void message_delete(struct message *msg);


#endif
