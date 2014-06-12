// messages.c

#include "messages.h"

messages_t * messages_new(void)
{
	messages_t* msgs;
	
	msgs = malloc(sizeof(messages_t));
	assert(msgs);
	msgs->messages = NULL;
	msgs->max = 0;
	
	return(msgs);
}


void messages_free(messages_t *msgs)
{
	assert(msgs);
	
	if (msgs->messages) {
		assert(msgs->max >= 0);
		
		while (msgs->max > 0) {
			msgs->max --;
			
			assert(msgs->messages[msgs->max].command == 0);
		}
		
		free(msgs->messages);
		msgs->messages = NULL;
		msgs->max = 0;
	}
	else {
		assert(msgs->max == 0);
	}
	
	free(msgs);
}




message_t * message_get(messages_t *msgs, int userid)
{
	message_t *msg;
	
	assert(msgs);
	assert(userid >= 0);
	
	assert(userid < msgs->max);
	assert(msgs->messages);
	
	msgs->next = userid;
	
	msg = &(msgs->messages[userid]);
	assert(msg->status > 0);
	msg->status = 0;
	assert(msg);
	return(msg);
}
