#include "message.h"
#include <stdlib.h>

struct message *
message_new(struct message *msg) {
	struct message * r = (struct message *)malloc(sizeof(*r));
	if (r == NULL)
		return NULL;
	*r = *msg;
	return r;
}

void
message_delete(struct message *msg) {
	if (msg) {
		free(msg->msg);
		free(msg);
	}
}
