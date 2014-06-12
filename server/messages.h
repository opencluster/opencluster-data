// messages.h

#ifndef __MESSAGES_H
#define __MESSAGES_H

// The messages are kept when a command is sent to another node (either client or server).  The required information is generally kept until a reply is received.   

typedef struct {
	short int status;
	short int command;
	int userid;
	void *data;
} message_t;


typedef struct {
	
	int max;
	message_t *messages;
	
} messages_t;


void messages_init(messages_t *msgs);
void messages_free(messages_t *msgs);


message_t * message_get(messages_t *msgs, int userid);
int message_set(messages_t *msgs, short int command, void *data);


#endif