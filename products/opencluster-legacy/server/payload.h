// payload.h

#ifndef __PAYLOAD_H
#define __PAYLOAD_H

// When a node needs to send a new message to another node, it generally has to build a payload, 
// which contains the values that it wants to send.  This payload system will provide the memory 
// space to add those parameters, and will keep the payload available in case the message needs to 
// be re-sent to a different node, or when processing hte reply, to obtain the original values that 
// were sent.
//
// The idea is that the 'push' operation will get a new payload by calling payload_new().  It will 
// then add params to that payload and it will be attached to the 'send'.   When a reply comes in, 
// that payload will be given as a pointer and can be parsed to get the values out.   
//
// When the processing is complete, the payload will be cleaned up and ready to be used for another 
// message.  However, if the processing invokes a re-send of the message (possibly to another 
// server) the payload will remain for that message.   To accomplish this, each payload has a 'used' 
// counter.  When it goes down to zero, it can be re-used, but a resend will cause it to remain.
//
// This also means that if the server needs to send out the same message to a bunch of servers, it 
// could actually re-use the same payload.



// The handle Identifier, which is essentially an index into the array.
typedef int PAYLOAD;


// For replies, sometimes there is no payload.  Sent commands always need a payload however, even if 
// the buffer is empty.
#define NO_PAYLOAD (-1)


typedef struct {
	
	// usage counter.  when at zero, this payload can be re-used.  Not really needed, except as a 
	// double-check that the payloads are moving from the 'avail' list to the 'active' list and 
	// back again.
	int used;

	// we need to make sure that when a userid is referenced, it is assigned to that connection.  This 
	// is not necessary if proper security controls are in place, but is an extra measure to make 
	// sure that connections are not able to cause problems by referencing payloads that were not sent 
	// to them.
	void *connection;	// actually will reference a connection_t pointer.
	
	// details about this particular instance.  Used to verify integrity.
	int command;
	int userid;
	
	// contents of the payload.
	void *buffer;
	int length;		// the current length of data used in the buffer.
	int max;		// the maximum allocated size of the buffer.
	
	// make a note of the 'seconds' since this payload was sent.
	int sent;
	
} payload_t;



// Initialisation to be performed once at startup, and cleanup to be performed at shutdown.
void payload_init(void);
void payload_free(void);

// Since the payloads are assigned to particular connections, we will need to provide an interface from 
// the connection's perspective to help manage those connections.
int payload_client_count(void *connection);
void payload_free_client(void *connection);

PAYLOAD payload_new(void *client, int command);
PAYLOAD payload_new_reply(void);
void payload_release(PAYLOAD payload);

// int payload_length(void);
// void payload_clear(void);
// void * payload_ptr(void);

void payload_int(PAYLOAD entry, int value);
void payload_long(PAYLOAD entry, long long value);
void payload_string(PAYLOAD entry, const char *str);
void payload_data(PAYLOAD entry, int length, void *data);

void payload_free_client(void *client_ptr);

payload_t * payload_get(PAYLOAD entry);
payload_t * payload_get_verify(PAYLOAD entry, int command, void *client);
void payload_release(PAYLOAD entry);



#endif