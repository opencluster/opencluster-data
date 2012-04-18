//-----------------------------------------------------------------------------
// libopencluster
// library interface to communicate with the opencluster service.

#include "opencluster.h"

#include <arpa/inet.h>
#include <assert.h>
#include <netdb.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>


#if (LIBOPENCLUSTER_VERSION != 0x00000100)
#error "Incorrect opencluster.h header version."
#endif

#define DEFAULT_BUFFER_SIZE   1024



#define WAIT_FOR_REPLY  0
#define DONT_WAIT       1

#define CMD_ACK         1
#define CMD_HELLO       10
#define CMD_GOODBYE     20
#define CMD_SERVER_INFO 100
#define CMD_HASHMASK    120
#define CMD_SET_INT     2000
#define CMD_SET_STR     2020
#define CMD_GET_INT     2100
#define CMD_DATA_INT    2105
#define CMD_GET_STR     2120
#define CMD_DATA_STR    2125

// this structure is not packed on word boundaries, so it should represent the 
// data received over the network.
#pragma pack(push,1)
typedef struct {
	short command;
	short repcmd;
	int userid;
	int length;
} raw_header_t;
#pragma pack(pop)

// The following macro can be used to help find situations where a peice of memory is free'd twice.  
// Useful to track down some difficult memory corruption bugs.
// #define free(x) printf("free: %08X, line:%d\n", (unsigned int)(x), __LINE__); free(x); x=NULL;


typedef struct {
	int available;
	int id;

	// data going out.
	struct {
		void *data;
		int length;
		int max;
		int command;
	} out;
	
	// data coming in.
	struct {
		
		// 0 indicates that the reply has not been received.
		// > 0 indicates the command that was received.
		int result;
		
		int length;
		int max;
		void *payload;
		int offset;		// used when reading out the data from the message.
	} in;

} message_t;

typedef unsigned int hash_t;



// information about a server we know about.
typedef struct {
	int handle;
	char active;
	char closing;
	char shutdown;

	char *host;
	int port;

	char *in_buffer;
	int in_length;
	int in_max;

	int ready;
	
} server_t;



typedef struct {
	char *host;
	int port;
	server_t *server;
	server_t *backup;
} hashmask_t;


//-----------------------------------------------------------------------------
// function pre-declaration.
int server_connect(cluster_t *cluster, server_t *server);





//-----------------------------------------------------------------------------
// initialise the stash_t structure.  If a NULL is passed in, a new object is 
// created for you, alternatively, you can pass in a pointer to an object you 
// want to control.... normally just pass a NULL and let us take care of it.
cluster_t * cluster_init(void)
{
	cluster_t *cluster;
	
	cluster = calloc(1, sizeof(cluster_t));
	assert(cluster);

	cluster->servers = NULL;
	cluster->server_count = 0;

	cluster->payload = malloc(DEFAULT_BUFFER_SIZE);
	cluster->payload_max = DEFAULT_BUFFER_SIZE;
	cluster->payload_length = 0;
	
	cluster->msg_count = 0;
	cluster->messages = NULL;
	
	cluster->mask = 0;			// a mask of zero, indicates that hashmasks havent been loaded.
	cluster->hashmasks = NULL;
	
	return(cluster);
}



static void server_free(server_t *server)
{
	assert(server->host);
	free(server->host);

	assert(server->handle < 0);
	
	assert(server->in_length == 0);
	assert(server->in_buffer);
	assert(server->in_max > 0);
	free(server->in_buffer);
	
	free(server);
}

//-----------------------------------------------------------------------------
// free all the resources used by the 'cluster' object.
void cluster_free(cluster_t *cluster)
{
	message_t *msg;
	int i;
	hashmask_t **hashmasks;
	
	assert(cluster);

	while (cluster->server_count > 0) {
		cluster->server_count --;
		
		assert(cluster->servers);
		assert(cluster->servers[cluster->server_count]);
		
		server_free(cluster->servers[cluster->server_count]);
	}
	free(cluster->servers);
	cluster->servers = NULL;
	assert(cluster->server_count == 0);


	// we may have messages in the message-pool, so we need to make sure they have all been 
	// processed and then clean them all out.
	while (cluster->msg_count > 0) {
		cluster->msg_count --;
		msg = cluster->messages[cluster->msg_count];
		assert(msg);
		
		assert(msg->available > 0);
		assert(msg->id == cluster->msg_count);

		assert(msg->out.data);
		assert(msg->out.length == 0);
		assert(msg->out.max > 0);
		assert(msg->out.command == 0);
		
		free(msg->out.data);
		msg->out.data = NULL;
		msg->out.max = 0;

		assert(msg->in.result == 0);
		assert(msg->in.length == 0);
		assert(msg->in.max > 0);
		assert(msg->in.payload);
		assert(msg->in.offset == 0);
		
		free(msg->in.payload);
		msg->in.payload = NULL;
		msg->in.max = 0;
		
		free(msg);
	}
	free(cluster->messages);
	cluster->messages = NULL;

	// the hashmasks array should be empty at this point, because freeing each server should have 
	// removed it from this list already.
	assert((cluster->mask == 0 && cluster->hashmasks == NULL) || (cluster->mask > 0 && cluster->hashmasks));
	if (cluster->hashmasks) {
		hashmasks = (hashmask_t **) cluster->hashmasks;
		for (i=0; i <= cluster->mask; i++) {
			if (hashmasks[i]) {
				assert(hashmasks[i]->server == NULL);
				assert(hashmasks[i]->backup == NULL);
				if (hashmasks[i]->host) {
 					free(hashmasks[i]->host);
					hashmasks[i]->host = NULL;
				}
				assert(hashmasks[i]);
				free(hashmasks[i]);
				hashmasks[i] = NULL;
			}
		}
		free(cluster->hashmasks);
		cluster->hashmasks = NULL;
		cluster->mask = 0;
	}
	

	if (cluster->payload) {
		assert(cluster->payload_max > 0);
		assert(cluster->payload_length == 0);
		free(cluster->payload);
		cluster->payload = NULL;
		cluster->payload_max = 0;
	}
	
	free(cluster);
}



// add a server to the list.
void cluster_addserver(cluster_t *cluster, const char *host)
{
	server_t *server;
	char *copy;
	char *first;
	char *next;
	
	assert(cluster);
	assert(host);
	
	server = calloc(1, sizeof(server_t));
	assert(server);

	server->handle = -1;		// socket handle to the connected controller.
	server->active = 0;
	server->closing = 0;
	server->shutdown = 0;
	
	server->in_buffer = malloc(DEFAULT_BUFFER_SIZE);
	assert(server->in_buffer);
	server->in_length = 0;
	server->in_max = DEFAULT_BUFFER_SIZE;
	
	// parse the host string, to remove the port part.
	copy = strdup(host);
	assert(copy);
	next = copy;
	first = strsep(&next, ":");
	assert(first == copy);
	
	if (next == NULL) {
		// no port was supplied.
		server->port = OPENCLUSTER_DEFAULT_PORT;
		server->host = strdup(host);
	}
	else {
		server->port = atoi(next);
		server->host = strdup(first);
	}
	
	free (copy);

	//TODO *** first need to check that the server isn't already in the list.  If it is, return -1.
	
	
	// add the conn to the list.
	if (cluster->servers == NULL) {
		assert(cluster->server_count == 0);
		cluster->servers = calloc(1, sizeof(cluster_t *));
	}
	else {
		cluster->servers = realloc(cluster->servers, sizeof(cluster_t *) * (cluster->server_count + 1));
		assert(cluster->servers);
	}
	cluster->servers[cluster->server_count] = server;
	cluster->server_count ++;
}




static int sock_resolve(const char *szAddr, int iPort, struct sockaddr_in *pSin)
{
	unsigned long ulAddress;
	struct hostent *hp;
	
	assert(szAddr != NULL && szAddr[0] != '\0' && iPort > 0);
	assert(pSin != NULL);
	
	// First, assign the family and port.
	pSin->sin_family = AF_INET;
	pSin->sin_port = htons(iPort);
	
	// Look up by standard notation (xxx.xxx.xxx.xxx) first.
	ulAddress = inet_addr(szAddr);
	if ( ulAddress != (unsigned long)(-1) )  {
		// Success. Assign, and we're done.  Since it was an actual IP address, then we dont doany 
		// DNS lookup for that, so we cant do any check ing for any other address type (such as MX).
		pSin->sin_addr.s_addr = ulAddress;
		return 0;
	}
	
	
	// If that didn't work, try to resolve host name by DNS.
	hp = gethostbyname(szAddr);
	if( hp == NULL ) {
		// Didn't work. We can't resolve the address.
		return -1;
	}
	
	// Otherwise, copy over the converted address and return success.
	memcpy( &(pSin->sin_addr.s_addr), &(hp->h_addr[0]), hp->h_length);
	return 0;
}



int sock_connect(char *host, int port)
{
	int handle = -1;
	struct sockaddr_in sin;
	
	assert(host != NULL);
	assert(port > 0);
	
	if (sock_resolve(host,port,&sin) >= 0) {
		// CJW: Create the socket
		handle = socket(AF_INET,SOCK_STREAM,0);
		if (handle >= 0) {
			// CJW: Connect to the server
			if (connect(handle, (struct sockaddr*)&sin, sizeof(struct sockaddr)) < 0) {
				// the connect failed.
				close(handle);
				handle = -1;
			}
		}
	}
	
	return(handle);
}


// the connection to the server was closed. 
static void server_closed(cluster_t *cluster, server_t *server)
{
	assert(cluster);
	assert(server);
	
	
	close(server->handle);
	server->handle = -1;
	server->active = 0;;
}


// When creating the header, we dont actually know the 'length' of the payload, so we will adjust 
// that as we add data to it.   This payload will be used mostly just for returning ACK and NACK 
// replies to teh server.
static void payload_header(cluster_t *cluster, short command, short repcmd, int userid)
{
	raw_header_t *raw;
	
	assert(cluster);
	assert(command > 0);
	assert(repcmd > 0);
	
	assert(cluster->payload);
	assert(cluster->payload_length == 0);
	assert(cluster->payload_max >= sizeof(raw_header_t));
	
	raw = cluster->payload;
	raw->command = htons(command);
	raw->repcmd = htons(repcmd);
	raw->userid = htonl(userid);
	raw->length = 0;
	
	cluster->payload_length = sizeof(raw_header_t);
}




// the payload is only used for the replies that are sent to the server.  
static void payload_int(cluster_t *cluster, int value)
{
	int avail;
	int *ptr;
	raw_header_t *raw;
	
	assert(cluster);
	assert(cluster->payload_length >= sizeof(raw_header_t));
	
	avail = cluster->payload_max - cluster->payload_length;
	if (avail < sizeof(int)) {
		cluster->payload = realloc(cluster->payload, cluster->payload_max + DEFAULT_BUFFER_SIZE);
		assert(cluster->payload);
		cluster->payload_max += DEFAULT_BUFFER_SIZE;
	}
	
	ptr = ((void*) cluster->payload + cluster->payload_length);
	ptr[0] = htonl(value);
	
	cluster->payload_length += sizeof(int);

	// update the length in the header.
	assert(cluster->payload_length > sizeof(raw_header_t));
	raw = cluster->payload;
	raw->length = htonl(cluster->payload_length - sizeof(raw_header_t));
	
	assert(cluster->payload);
	assert(cluster->payload_length <= cluster->payload_max);
}



static void payload_data(cluster_t *cluster, int length, void *data)
{
	int avail;
	int *ptr;
	raw_header_t *raw;
	
	assert(cluster);
	
	avail = cluster->payload_max - cluster->payload_length;
	while (avail < (sizeof(int)+length)) {
		cluster->payload = realloc(cluster->payload, cluster->payload_max + DEFAULT_BUFFER_SIZE);
		assert(cluster->payload);
		cluster->payload_max += DEFAULT_BUFFER_SIZE;
		avail += DEFAULT_BUFFER_SIZE;
	}

	// add the length of the string first.
	ptr = ((void*) cluster->payload + cluster->payload_length);
	ptr[0] = htonl(length);
	
	memcpy(cluster->payload + cluster->payload_length + sizeof(int), data, length);
	
	cluster->payload_length += sizeof(int) + length;

	// update the length in the header.
	assert(cluster->payload_length > sizeof(raw_header_t));
	raw = cluster->payload;
	raw->length = htonl(cluster->payload_length - sizeof(raw_header_t));

	assert(cluster->payload);
	assert(cluster->payload_length <= cluster->payload_max);
}



static void payload_string(cluster_t *cluster, const char *str)
{
	assert(cluster);
	assert(str);
	payload_data(cluster, strlen(str), (void*)str);
}


// assumes that the reply is already in the cluster->payload,
static void send_reply(cluster_t *cluster, server_t *server)
{
	ssize_t sent, datasent;
	int avail;

	assert(cluster);
	assert(server);
	
	assert(cluster->payload);
	assert(cluster->payload_length <= cluster->payload_max);
	assert(cluster->payload_length >= sizeof(raw_header_t));
	
	// send the data
	datasent = 0;
	assert(server->handle > 0);
	while (datasent < cluster->payload_length && server->handle > 0) {
		avail = cluster->payload_length - datasent;
		sent = send(server->handle, cluster->payload + datasent, avail, 0);
		assert(sent != 0);
		assert(sent <= avail);
		if (sent < 0) {
			server_closed(cluster, server);
			assert(server->active == 0);
			assert(server->handle < 0);
		}
		else {
			datasent += sent;
			assert(server->handle > 0);
		}
	}
	cluster->payload_length = 0;
	
	assert(cluster->payload_length == 0);
}



static void reply_ack(cluster_t *cluster, server_t *server, short repcmd, int userid)
{

	assert(cluster);
	assert(server);
	assert(repcmd > 0);
	
	// if we are not connected to this server, then how did we get the message we are replying to?
	assert(server->handle > 0);

	payload_header(cluster, CMD_ACK, repcmd, userid);
	send_reply(cluster, server);
}


static void * data_int(void *data, int *value)
{
	void *next;
	int *raw;
	
	assert(data);
	assert(value);
	
	raw = data;
	value[0] = ntohl(raw[0]);
	next = data + 4;
	
	return(next);
}



static void * data_string(void *data, int *length, char **ptr)
{
	void *next;
	int *raw_length;
	
	assert(data);
	assert(length);
	assert(ptr);
	
	raw_length = data;
	length[0] = ntohl(raw_length[0]);
	*ptr = data + 4;
	next = *ptr + length[0];
	
	return(next);
}



static void process_serverinfo(cluster_t *cluster, server_t *server, int userid, int length, void *data)
{
	char *next;
	int slen;
	char *sinfo;
	char *str_info;
	
	assert(cluster);
	assert(server);
	assert(length >= 4);

	next = data;
	next = data_string(next, &slen, &sinfo);
	assert(slen > 0);
	assert(sinfo);
	
	str_info = malloc(slen+1);
	memcpy(str_info, sinfo, slen);
	
// 	printf("received server info: '%s'\n", str_info);

	// if we receive actual servers, we need to process them somehow.
	cluster_addserver(cluster, str_info);
	
	free(str_info);
	str_info = NULL;
	
	reply_ack(cluster, server, CMD_SERVER_INFO, userid);
}



// this function will take the current array, and put it aside, creating a new array based on the 
// new mask supplied (we can only make the mask bigger, and cannot shrink it).
// We create a new hashmasks array, and for each entry, we compare it against the old mask, and use 
// the data for that hash from the old list.
// NOTE: We may be starting off with an empty hashmasks lists.  If this is the first time we've 
//       received some hashmasks.  To implement this easily, if we dont already have hashmasks, we 
//       may need to create one that has a dummy entry in it.
static void split_mask(cluster_t *cluster, int mask) 
{
	hashmask_t **newlist = NULL;
	hashmask_t **oldlist = NULL;
	int i;
	int index;
	
	assert(cluster);
	assert(mask > cluster->mask);
	
// 	printf("Splitting bucket list: oldmask=%08X, newmask=%08X\n", cluster->mask, mask);
	
	oldlist = (hashmask_t **) cluster->hashmasks;
	cluster->hashmasks = NULL;
	
	if (oldlist == NULL) {
		
		oldlist = malloc(sizeof(hashmask_t *));
		assert(oldlist);
		
		oldlist[0] = malloc(sizeof(hashmask_t));
		assert(oldlist[0]);
		
		oldlist[0]->host = NULL;
		oldlist[0]->port =0;
		oldlist[0]->server = NULL;
		oldlist[0]->backup = NULL;
	}
	
	newlist = malloc(sizeof(hashmask_t *) * (mask+1));
	assert(newlist);
	for (i=0; i<=mask; i++) {
		
		newlist[i] = malloc(sizeof(hashmask_t));
		
		index = i & cluster->mask;
		assert(cluster->mask == 0 || index < cluster->mask);
		if (oldlist[index]->host) {
			newlist[i]->host = strdup(oldlist[index]->host);
			newlist[i]->port = oldlist[index]->port;
		}
		else {
			newlist[i]->host = NULL;
		}
		newlist[i]->server = oldlist[index]->server;
		newlist[i]->backup = oldlist[index]->backup;
		
// 		printf("New Mask: %08X/%08X --> '%s:%d'\n", mask, i, newlist[i]->host, newlist[i]->port);
	}

	
	// now we clean up the old list.
	for (i=0; i<=cluster->mask; i++) {
		assert(oldlist[i]);
		if (oldlist[i]->host) {
			free(oldlist[i]->host);
			assert(oldlist[i]->port > 0);
		}
		free(oldlist[i]);
	}
	free(oldlist);
	oldlist = NULL;
	
	cluster->hashmasks = (void *) newlist;
	cluster->mask = mask;
	
}

// we are receiving a hashmasks list from the server.  We need to do two things with it.  
//  1. We need to update the main hashmasks array so that it can determine which server needs to 
//     receive particular data.
//  2. Mark the server object so that it knows that it has received hashmasks.  It cannot really do 
//     anything until it has.
static void process_hashmask(cluster_t *cluster, server_t *server, int userid, int length, void *data)
{
	char *next;
	int mask;
	int hash;
	int level;
	hashmask_t **hashmasks;
	
	assert(cluster);
	assert(server);
	assert(length >= 4);

	next = data;
	
	next = data_int(next, &mask);
	assert(mask >= 0);

	// is it possible to receive a mask of 0?
	assert(mask > 0);
	
	if (mask > 0) {
		
		if (mask > cluster->mask) {
			// this server is using a mask that is more fine tuned than the mask we've been using.  
			// Therefore, we need to migrade to the new mask.
			split_mask(cluster, mask);
		}
		
		assert(mask == cluster->mask);
		hashmasks = (hashmask_t **) cluster->hashmasks;
		assert(hashmasks);
		
		next = data_int(next, &hash);
		next = data_int(next, &level);
			
		assert(level >= 0 || level == -1);
			
		if (level == 0) {
			
			assert(mask == cluster->mask);
			assert(hash == (mask & hash));
			assert(hashmasks[hash]);
			
			if (hashmasks[hash]->server != server) {
				if(hashmasks[hash]->host) {
					free(hashmasks[hash]->host);
					hashmasks[hash]->host = NULL;
				}
				assert(server->host);
				hashmasks[hash]->host = strdup(server->host);
				hashmasks[hash]->port = server->port;
				hashmasks[hash]->server = server;
			}
			else if (level == 1) {
				hashmasks[hash]->backup = server;
			}
			else if (level == -1) {
				// the server is no longer supporting this mask, so it should be removed from the list.
				assert(0);
			}
			else {
				
				// we are not the primary or secondary backup of the bucket, but if we dont already 
				// have a backup listed, then we should mark it.
				if (hashmasks[hash]->backup == NULL) {
					hashmasks[hash]->backup = server;
				}
			}
		}
	}
	
	reply_ack(cluster, server, CMD_HASHMASK, userid);
	
	// now that this server has supplied hashmasks, we can mark this server as active.
	server->active = 1;
}







// this function will ensure that there is not any pending data on the // incoming socket for the 
// server.  Since the server details are not exposed outside of the library, this is an internal 
// function.   Developers will need to call cluster_pending which will process pending data on all 
// the connected servers.
static void pending_server(cluster_t *cluster, server_t *server, int blocking)
{
	int done = 0;
	int offset = 0;
	int avail;
	int sent;
	raw_header_t *header;
	message_t *msg;
	int length;
	short repcmd;
	int userid;
	int inner = 0;
	short command;
	
	assert(cluster);
	assert(server);
	
	// if non-blocking, set socket in non-blocking mode.
	if (blocking == OC_NON_BLOCKING) {
		assert(0);
	}
	
	assert(server->in_length == 0);

	// we have two loops because we need to 
	done = 0;
	while (done == 0) {
	
		assert(server->in_buffer);
		assert(server->in_max > 0);
		assert(server->in_length >= 0);

		// if we have less than a buffer size available, then we need to expand the size of the buffer.
		if ((server->in_max - server->in_length) < DEFAULT_BUFFER_SIZE) {
			server->in_max += DEFAULT_BUFFER_SIZE;
			server->in_buffer = realloc(server->in_buffer, server->in_max);
			assert(server->in_buffer);
		}
		
		
		avail = server->in_max - server->in_length;
		assert(avail > 0);
		assert(offset >= 0);
		
// 		printf("recv: max=%d\n", avail);

		
		sent = recv(server->handle, server->in_buffer + offset + server->in_length, avail, 0);
		if (sent <= 0) {
			// socket has shutdown
			assert(0);
		}
		else {
			assert(sent > 0);
			assert(sent <= avail);
			server->in_length += sent;

// 			printf("recv: got=%d\n", sent);
			
			inner = 0;
			while (inner == 0) {
			
				// now that we've got data, we need to make sure we have enough for the header.
				if (server->in_length < sizeof(raw_header_t)) {
					inner ++;
				}
				else {
					
					void *ptr = server->in_buffer + offset;
					
					header = ptr;
					length = ntohl(header->length);
					
					ptr += sizeof(raw_header_t);
					
					
					if (length < 0) {
						// if we get some obviously wrong data, we need to close the socket.
						assert(0);
					}
					
					if (server->in_length >= sizeof(raw_header_t) + length) {
						// we have enough data to parse this entire message.
						
						repcmd = ntohs(header->repcmd);
						userid = ntohl(header->userid);
						assert(userid >= 0);
						
						// if message is a reply, add it to the reply data.
						if (repcmd > 0) {
							
							
							userid = ntohl(header->userid);
							assert(userid >= 0);
							assert(userid < cluster->msg_count);
							
							assert(cluster->messages);
							
							msg = cluster->messages[userid];
							
							assert(msg->id == userid);
							assert(msg->available == 0);
							assert(msg->out.command == repcmd);

							if (msg->in.max < length) {
								msg->in.payload = realloc(msg->in.payload, length);
								msg->in.max = length;
							}
							
							assert(msg->in.payload);
							memcpy(msg->in.payload, ptr, length);
							msg->in.result = ntohs(header->command);
							
// 							printf("Received reply (%d) from command (%d), length=%d\n", msg->in.result, repcmd, length);

						}
						else {
							// it is not a reply, it is a command.  We need to process that as well.
					
							command = ntohs(header->command);
// 							printf("Command Received: %d\n", command); 
							switch (command) {
		
								case CMD_SERVER_INFO: process_serverinfo(cluster, server, userid, length, ptr); break;
								case CMD_HASHMASK:    process_hashmask(cluster, server, userid, length, ptr);  break;
			
								default:
									printf("Unexpected command: cmd=%d\n", command);
									assert(0);
									break;
								
							}
						}

						// if we've processed all the data, and we dont have any partial messages at the end of 
						// it, then we exit the loop.  If we do have a partial message, then we need to continue 
						// waiting for more data.
						
						offset += (sizeof(raw_header_t) + length);
						server->in_length -= (sizeof(raw_header_t) + length);
						assert(offset <= server->in_max);
						
						if (server->in_length == 0) {
							// we've processed all the messages in the buffer, and there are no more partial ones... so we are done.
							offset = 0;
							inner ++;
							done = 1;
						}
					}
				}
			}
		}
	}
	
	assert(server->in_length == 0);
	
	// if non-blocking, set socket back to blocking mode.
	if (blocking == OC_NON_BLOCKING) {
		assert(0);
	}
}



//--------------------------------------------------------------------------------------------------
// the included message has the details for the reply, so we need to just send the data, and then 
// wait for the replies to come in (if we are waiting for them).
static int send_request(cluster_t *cluster, server_t *server, message_t *msg, int wait_for_reply)
{
	ssize_t sent, datasent;
	int avail;
	int status;

	assert(cluster);
	assert(server);
	
	// if we are not connected to this server, then we need to connect.
	if (server->handle < 0) {
		if (server_connect(cluster, server) != 0) {
			return(-1);
		}
	}
	
	assert(msg);
	assert(msg->out.length > 0);
	assert(msg->out.max >= msg->out.length);
	assert(msg->out.data);
	assert(msg->in.result == 0);
	
	// send the data
	datasent = 0;
	assert(server->handle > 0);
	status = 0;
	while (datasent < msg->out.length && server->handle > 0) {
		avail = msg->out.length - datasent;
		assert(avail > 0);
		sent = send(server->handle, msg->out.data + datasent, avail, 0);
		assert(sent != 0);
		assert(sent <= avail);
		if (sent < 0) {
			server_closed(cluster, server);
			assert(server->active == 0);
			assert(status == 0);
		}
		else {
			datasent += sent;
			status = 1;
		}
	}
	
// 	printf("sent: %d\n", datasent);
	
	if (status == 0) {
		// we lost connection during the send.  make sure we will return a NULL.
		return(-1);
	}
	
	
	// if we are going to be waiting for the data....
	if (wait_for_reply == WAIT_FOR_REPLY) {
		while (msg->in.result == 0  && server->handle > 0) {
// 			printf("waiting for reply\n");
 			pending_server(cluster, server, OC_BLOCKING);
		}
	}
	
	
	return(0);
}




// this function will return a message object that can be used to build an outgoing message to a 
// server.  It will grab a 'reply' slot and include it in the message, so when the message is sent, 
// the reply is kept with it.
//
// when finished with the message, you need to use message_return() to return it to the pool.
static message_t * message_new(cluster_t *cluster, short int command)
{
	int i;
	message_t *msg = NULL;
	message_t *tmp;
	raw_header_t *header;
	
	assert(cluster);
	assert(command > 0);
	
	// get a message from the pool.
	assert((cluster->msg_count == 0 && cluster->messages == NULL) || (cluster->msg_count > 0 && cluster->messages));
	for (i=0; i<cluster->msg_count && msg == NULL; i++) {
		
		assert(msg == NULL);
		tmp = cluster->messages[i];
		assert(tmp);
		assert(tmp->id == i);
		
		if (tmp->available == 1) {
			msg = tmp;
			msg->available = 0;
		}
	}

	if (msg == NULL) {
		cluster->messages = realloc(cluster->messages, sizeof(message_t *) * (cluster->msg_count + 1));
		msg = malloc(sizeof(message_t));
		assert(msg);
		cluster->messages[cluster->msg_count] = msg;
		
		msg->id = cluster->msg_count;
		msg->available = 0;
		msg->out.length = 0;
		msg->out.data = malloc(DEFAULT_BUFFER_SIZE);
		msg->out.max = DEFAULT_BUFFER_SIZE;
		
		msg->in.payload = malloc(DEFAULT_BUFFER_SIZE);
		msg->in.max = DEFAULT_BUFFER_SIZE;
		msg->in.result = 0;
		msg->in.offset = 0;
		
		cluster->msg_count ++;
	}
	
	assert(cluster->msg_count > 0);
	
	assert(msg->available == 0);
	assert(msg->out.length == 0);
	assert(msg->out.max > 0);
	assert(msg->out.data);
	assert(msg->in.result == 0);
	
	// build the header, and set the 'command' and the 'userid' field.
	assert(msg->out.max >= sizeof(raw_header_t));
	header = msg->out.data;
	header->command = htons(command);
	header->repcmd = 0;
	header->userid = htonl(msg->id);
	header->length = 0;
	
	msg->out.command = command;
	
	msg->out.length = sizeof(raw_header_t);
	assert(msg->out.length <= msg->out.max);
	
	assert(msg);
	return(msg);
}



static void message_return(message_t *msg)
{
	assert(msg);
	
	assert(msg->available == 0);

	msg->out.length = 0;
	msg->out.command = 0;

	msg->in.result = 0;
	msg->in.length = 0;
	msg->in.offset = 0;
		
	// mark the message as ready to use.
	msg->available = 1;
}





int server_connect(cluster_t *cluster, server_t *server)
{
	message_t *msg;
	int res=-1;
	
	assert(cluster);
	assert(server);
	
	// if server has a handle, then we exit, because we are already connected to it.
	if (server->handle < 0) {
		
		assert(server->active == 0);
		assert(server->closing == 0);
		assert(server->shutdown == 0);
		
		assert(server->host);
		assert(server->port > 0);

		server->handle = sock_connect(server->host, server->port);
		assert(res < 0);
		
		if (server->handle < 0) {
			res = -1;
			assert(server->active == 0);
		}
		else {
			assert(res < 0);

// 			printf("sending HELLO(%d)\n", CMD_HELLO);
			msg = message_new(cluster, CMD_HELLO);
			
			// send the request and receive the reply.
			if (send_request(cluster, server, msg, WAIT_FOR_REPLY) != 0) {
				// some error occured.  SHould handle this some how.
				assert(0);
			}
			else {
			
				if (msg->in.result != 0) {
					assert(res < 0);
					
					// after a HELLO is given, we should expect a number of SERVER_INFO and HASHMASK 
					// messages to arrive.  We need to keep processing data until they arrive, then 
					// we can be sure that we have connected to the cluster properly.
					//
					// NOTE: Its possible that this has already been received.  This is just to 
					//       catch if the server is busy and a delay occurs from it.
					while (server->active == 0 && server->handle > 0) {
// 						printf("Waiting for Hashmasks from server\n");
						pending_server(cluster, server, OC_BLOCKING);
		
						assert(server->active > 0);
						
						// if we dont yet have a set of hashmasks, then we need to wait until we do.
						if (server->active != 1) {
							
							// TODO: we should put a timeout here.  If we dont get the hashmasks after a 
							// period of time, then we give up and mark the server as not-responding.
							
							assert(0);
							
							assert(res < -1);
						}
						else {
							assert(res < 0);
							res = 0;
						}
					}
					
					res = 0;
				}
				else {
					res = -1;
					server->active = 0;
					
					
					// failed to connect to the server.  Should we do anything else with it?
					// TODO: we should probably put some sort of timestamp in there so we dont try and connect again to this server for a few seconds.
					assert(0);
				}
			}
			
			message_return(msg);
		}
	}

	return(res);
}


// This function is used to make sure there is no pending commands in any of the server connections.
void cluster_pending(cluster_t *cluster, int blocking) 
{
	int i;
	
	assert(cluster);
	assert(cluster->server_count > 0);
	assert(cluster->servers);
	
	assert(blocking == OC_BLOCKING || blocking == OC_NON_BLOCKING);
	
	for (i=0; i < cluster->server_count; i++) {
		if (cluster->servers[i] != NULL) {
			pending_server(cluster, cluster->servers[i], blocking);
		}
	}
}


// do nothing if we are already connected.  If we are not connected, then 
// connect to the first server in the list.  Since we are setup for blocking 
// activity, we will wait until the connect succeeds or fails.
int cluster_connect(cluster_t *cluster)
{
	server_t *server;
	int try = 0;
	int connected = 0;
	int res;

	assert(cluster->servers);
	assert(cluster->server_count > 0);

	for (try=0; try < cluster->server_count && connected == 0; try++) {
	
		server = cluster->servers[try];
		assert(server);

		res = server_connect(cluster, server);
		if (res == 0) {
			connected ++;
		}
	}
	
	return(connected);
	
}


void server_disconnect(cluster_t *cluster, server_t *server)
{
	int i;
	hashmask_t *hashmask;
	message_t *msg;
	
	assert(cluster);
	assert(server);
	
	// remove the server from the hashmasks list.
	assert(cluster->hashmasks);
	for (i=0; i<= cluster->mask; i++) {
		hashmask = cluster->hashmasks[i];
		assert(hashmask);
		
		if (hashmask->server == server) {
			hashmask->server = NULL;
		}
		if (hashmask->backup == server) {
			hashmask->backup = NULL;
		}
	}
	
	// send a GOODBYE command  first.
	msg = message_new(cluster, CMD_GOODBYE);
			
	// send the request and receive the reply.
	if (send_request(cluster, server, msg, WAIT_FOR_REPLY) != 0) {
		assert(0); // what?
	}
	
	// close the connection.
	assert(server->handle > 0);
	close(server->handle);
	server->handle = -1;
}

void cluster_disconnect(cluster_t *cluster)
{
	server_t *server;
	int try = 0;

	assert(cluster->servers);
	assert(cluster->server_count > 0);

	for (try=0; try < cluster->server_count; try++) {
	
		server = cluster->servers[try];
		assert(server);

		server_disconnect(cluster, server);
	}
}


// FNV hash.  Public domain function converted from public domain Java version.
unsigned int generate_hash_str(const char *str, const int length)
{
	register int i;
	register int hash = 2166136261l;

	for (i=0; i<length; i++)  {
		hash ^= (int)str[i];
		hash *= 16777619;
	}

	return(hash);
}


unsigned int generate_hash_int(const int key)
{
	register int i;
	register int hash = 2166136261l;
	union {
		int nkey;
		char str[sizeof(int)];
	} match;
	
	assert(sizeof(key) == 4);
	assert(sizeof(match) == sizeof(key));
	
	match.nkey = ntohl(key);

	for (i=0; i<sizeof(key); i++)  {
		hash ^= (int)match.str[i];
		hash *= 16777619;
	}

	return(hash);
}

static void msg_setint(message_t *msg, const int value) 
{
	raw_header_t *header;
	int *ptr;
	
	assert(msg);
	
	// make sure there is enough space in the buffer.
	if ((msg->out.length + sizeof(int)) > msg->out.max) {
		msg->out.data = realloc(msg->out.data, msg->out.max + DEFAULT_BUFFER_SIZE);
		msg->out.max += DEFAULT_BUFFER_SIZE;
	}

	ptr = ((void*)msg->out.data + msg->out.length);
	ptr[0] = htonl(value);
	msg->out.length += sizeof(int);
	
	// add the msg details to the outgoing payload buffer.
	assert(msg->out.max >= sizeof(raw_header_t));
	assert(msg->out.data);
	
	header = msg->out.data;
	header->length = htonl(msg->out.length - sizeof(raw_header_t));
}


static void msg_setstr(message_t *msg, const char *str) 
{
	raw_header_t *header;
	int *ptr;
	char * sptr;
	int slen;
	
	assert(msg);
	assert(str);
	
	slen = strlen(str);
	
	// make sure there is enough space in the buffer.
	while ((msg->out.length + sizeof(int) + slen) > msg->out.max) {
		msg->out.data = realloc(msg->out.data, msg->out.max + DEFAULT_BUFFER_SIZE);
		msg->out.max += DEFAULT_BUFFER_SIZE;
	}

	ptr = ((void*)msg->out.data + msg->out.length);
	ptr[0] = htonl(slen);

	sptr = ((void*)msg->out.data + msg->out.length + sizeof(int));
	memcpy(sptr, str, slen);
	
	msg->out.length += (sizeof(int) + slen);
	
	// add the msg details to the outgoing payload buffer.
	assert(msg->out.max >= sizeof(raw_header_t));
	assert(msg->out.data);
	
	header = msg->out.data;
	header->length = htonl(msg->out.length - sizeof(raw_header_t));
}





int cluster_setint(cluster_t *cluster, const char *name, const int value, const int expires)
{
	int res = 0;
	int name_hash;
	int mask_index;
	server_t *server;
	hashmask_t *hashmask;
	message_t *msg;
	
	assert(cluster);
	assert(name);
	assert(expires >= 0);
	
	// get a hash of the name
	name_hash = generate_hash_str(name, strlen(name));
	
	// get the index from the hash mask.
	assert(cluster->mask > 0);
	mask_index = name_hash & cluster->mask;
	
	// get the server object from the index.
	assert(cluster->hashmasks);
	hashmask = cluster->hashmasks[mask_index];
	assert(hashmask);
	server = hashmask->server;
	
	// make sure server is connected.  if not, then connect and wait for the initial handshaking.
	while (server == NULL) {
		// we are not connected to this server yet.  So we need to connect first.
		cluster_connect(cluster);
		server = hashmask->server;
		
		// *** should we have some sort of loop counter in here?
	}
	
	assert(server);
	
	// build the message and send it off.
	msg = message_new(cluster, CMD_SET_INT);
	assert(msg);
	msg_setint(msg, 0); 		// integer - map hash (0 for non-map items)
	msg_setint(msg, name_hash);	// integer - key hash
	msg_setint(msg, expires);	// integer - expires (in seconds from now, 0 means it never expires)
	msg_setint(msg, 0);			// integer - full wait (0 indicates dont wait for sync to backup servers, 1 indicates to wait).
	msg_setstr(msg, name);		// string  - name
	msg_setint(msg, value);		// integer - value (to be stored).

	send_request(cluster, server, msg, WAIT_FOR_REPLY);
	
	// process pending data on server (blocking), until reply is received.
	while (msg->in.result == 0 && server->handle > 0) {
		pending_server(cluster, server, OC_NON_BLOCKING);
	}

	// now we've got a reply, we free the message, because there is no 
	assert(msg->in.result == CMD_ACK);
	
	
	message_return(msg);
	
	return(res);
}



int cluster_setstr(cluster_t *cluster, const char *name, const char *value, const int expires)
{
	int res = 0;
	int name_hash;
	int mask_index;
	server_t *server;
	hashmask_t *hashmask;
	message_t *msg;
	
	assert(cluster);
	assert(name);
	assert(expires >= 0);
	
	// get a hash of the name
	name_hash = generate_hash_str(name, strlen(name));
	
	// get the index from the hash mask.
	assert(cluster->mask > 0);
	mask_index = name_hash & cluster->mask;
	
	// get the server object from the index.
	assert(cluster->hashmasks);
	hashmask = cluster->hashmasks[mask_index];
	assert(hashmask);
	server = hashmask->server;
	
	// make sure server is connected.  if not, then connect and wait for the initial handshaking.
	while (server == NULL) {
		// we are not connected to this server yet.  So we need to connect first.
		cluster_connect(cluster);
		server = hashmask->server;
	}
	
	assert(server);
	
	// build the message and send it off.
	msg = message_new(cluster, CMD_SET_STR);
	assert(msg);
	msg_setint(msg, 0); 		// integer - map hash (0 for non-map items)
	msg_setint(msg, name_hash);	// integer - key hash
	msg_setint(msg, expires);	// integer - expires (in seconds from now, 0 means it never expires)
	msg_setint(msg, 0);			// integer - full wait (0 indicates dont wait for sync to backup servers, 1 indicates to wait).
	msg_setstr(msg, name);		// string  - name
	msg_setstr(msg, value);		// integer - value (to be stored).

	send_request(cluster, server, msg, WAIT_FOR_REPLY);
	
	// process pending data on server (blocking), until reply is received.
	while (msg->in.result == 0 && server->handle > 0) {
		pending_server(cluster, server, OC_NON_BLOCKING);
	}

	// now we've got a reply, we free the message, because there is no 
	assert(msg->in.result == CMD_ACK);
	
	
	message_return(msg);
	
	return(res);
}



static void msg_getint(message_t *msg, int *value)
{
	int *ptr;
	
	assert(msg);
	assert(value);
	assert(msg->in.offset >= 0);
	
	ptr = ((void*)(msg->in.payload) + msg->in.offset);
	*value = ntohl(*ptr);

	msg->in.offset += sizeof(int);
}


static void msg_getstr(message_t *msg, char **value, int *length)
{
	int *ptr_len;
	char *ptr_str;
	int len;
	char *str;

	assert(msg);
	assert(value);
	assert(msg->in.offset >= 0);
	
	ptr_len = ((void*)(msg->in.payload) + msg->in.offset);
	len = ntohl(*ptr_len);
	msg->in.offset += sizeof(int);
	
	ptr_str = ((void*)(msg->in.payload) + msg->in.offset);
	assert(len > 0);
	str = malloc(len + 1);
	memcpy(str, ptr_str, len);
	str[len] = 0;
	
	*value = str;
	*length = len;
}



int cluster_getint(cluster_t *cluster, const char *name, int *value)
{
	int res = 0;
	int name_hash;
	int mask_index;
	int map_hash;
	int key_hash;
	server_t *server;
	hashmask_t *hashmask;
	message_t *msg;
	
	assert(cluster);
	assert(name);
	assert(value);
	
	// get a hash of the name
	name_hash = generate_hash_str(name, strlen(name));
	
	// get the index from the hash mask.
	assert(cluster->mask > 0);
	mask_index = name_hash & cluster->mask;
	
	// get the server object from the index.
	assert(cluster->hashmasks);
	hashmask = cluster->hashmasks[mask_index];
	assert(hashmask);
	server = hashmask->server;
	
	// make sure server is connected.  if not, then connect and wait for the initial handshaking.
	if (server == NULL) {
		// we are not connected to this server yet.  So we need to connect first.
		assert(0);
	}
	
	// build the message and send it off.
	msg = message_new(cluster, CMD_GET_INT);
	assert(msg);
	msg_setint(msg, 0); 		// integer - map hash (0 for non-map items)
	msg_setint(msg, name_hash);	// integer - key hash

	send_request(cluster, server, msg, WAIT_FOR_REPLY);
	
	// process pending data on server (blocking), until reply is received.
	while (msg->in.result == 0 && server->handle > 0) {
		pending_server(cluster, server, OC_NON_BLOCKING);
	}

	// now we've got a reply, we free the message, because there is no 
// 	printf("msg result == %d\n", msg->in.result);
	assert(msg->in.result == CMD_DATA_INT);

	msg_getint(msg, &map_hash);
	msg_getint(msg, &key_hash);
	msg_getint(msg, value);

// printf("---- map_hash:%d, key_hash:%d, name_hash:%d, value:%d\n", map_hash, key_hash, name_hash, *value);
	
	assert(key_hash == name_hash);
	
	message_return(msg);
	
	return(res);
}


int cluster_getstr(cluster_t *cluster, const char *name, char **value, int *length)
{
	int res = 0;
	int name_hash;
	int mask_index;
	int map_hash;
	int key_hash;
	server_t *server;
	hashmask_t *hashmask;
	message_t *msg;
	char *str;
	int str_len;
	
	assert(cluster);
	assert(name);
	assert(value);
	
	// get a hash of the name
	name_hash = generate_hash_str(name, strlen(name));
	
	// get the index from the hash mask.
	assert(cluster->mask > 0);
	mask_index = name_hash & cluster->mask;
	
	// get the server object from the index.
	assert(cluster->hashmasks);
	hashmask = cluster->hashmasks[mask_index];
	assert(hashmask);
	server = hashmask->server;
	
	// make sure server is connected.  if not, then connect and wait for the initial handshaking.
	if (server == NULL) {
		// we are not connected to this server yet.  So we need to connect first.
		assert(0);
	}
	
	// build the message and send it off.
	msg = message_new(cluster, CMD_GET_STR);
	assert(msg);
	msg_setint(msg, 0); 		// integer - map hash (0 for non-map items)
	msg_setint(msg, name_hash);	// integer - key hash

	send_request(cluster, server, msg, WAIT_FOR_REPLY);
	
	// process pending data on server (blocking), until reply is received.
	while (msg->in.result == 0 && server->handle > 0) {
		pending_server(cluster, server, OC_NON_BLOCKING);
	}

	// now we've got a reply, we free the message, because there is no 
// 	printf("msg result == %d\n", msg->in.result);
	assert(msg->in.result == CMD_DATA_STR);

	msg_getint(msg, &map_hash);
	msg_getint(msg, &key_hash);
	msg_getstr(msg, &str, &str_len);
	
	assert(str);
	assert(str_len > 0);
	
	*value = str;
	*length = str_len;

// printf("---- map_hash:%d, key_hash:%d, name_hash:%d, value:%d\n", map_hash, key_hash, name_hash, *value);
	
	assert(key_hash == name_hash);
	
	message_return(msg);
	
	return(res);
}

