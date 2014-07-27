// messages.h

#ifndef __MESSAGES_H
#define __MESSAGES_H

#include "client.h"



void messages_init(void);
void messages_cleanup(void);

// create a message and store the data, will return a userid that will be used to identify this 
// message.
int message_set(client_t *client, short int command, void *data);


// get the data that was stored for this message.  This can only be done once, the message is 
// available again for use after this call.  The information must match both the userID and the command that the reply is for.
void * message_get(client_t *client, int userid, int command);

// return the number of remaining messages for this client.
int messages_count(client_t *client);





#endif