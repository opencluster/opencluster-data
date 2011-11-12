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

#define CMD_HELLO       10



// this structure is not packed on word boundaries, so it should represent the 
// data received over the network.
#pragma pack(push,1)
typedef struct {
	short unused;
	short command;
	int userid;
	int length;
} raw_header_t;
#pragma pack(pop)




typedef struct {
	int id;
	int available;
	int result;
} reply_t;


typedef struct {
	reply_t *reply;
	int datalen;
	void *data;
} message_t;

typedef unsigned int hash_t;


typedef struct {
	hash_t hash;
	char *backup_server;
} server_hashmask_t;

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
	
	unsigned int mask;
	server_hashmask_t **hashmasks;
	
} server_t;




//-----------------------------------------------------------------------------
// function pre-declaration.
int server_connect(cluster_t *cluster, server_t *server);





//-----------------------------------------------------------------------------
// initialise the stash_t structure.  If a NULL is passed in, a new object is 
// created for you, alternatively, you can pass in a pointer to an object you 
// want to control.... normally just pass a NULL and let us take care of it.
cluster_t * cluster_init(void)
{
	cluster_t *s;
	
	s = calloc(1, sizeof(cluster_t));
	assert(s);

	s->servers = NULL;
	s->server_count = 0;

	s->reply_count = 0;
	s->replies = NULL;

	s->msg_count = 0;
	s->messages = NULL;
	
	s->mask = 0;			// a mask of zero, indicates that hashmasks havent been loaded.
	s->hashmasks = NULL;
	
	return(s);
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
	reply_t *reply;
	message_t *msg;
	int i;
	
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

	// we might have replies in the reply-pool, so we need to make sure they have all been processed and then clean them out.
	assert((cluster->replies && cluster->reply_count > 0) || (cluster->replies == NULL && cluster->reply_count == 0));
	if (cluster->replies) {
		while (cluster->reply_count > 0) {
			reply = cluster->replies[cluster->reply_count];
			if (reply) {
				assert(reply->available == 1);
			}
			cluster->reply_count --;
		}
		free(cluster->replies);
		cluster->replies = NULL;
	}

	// we may have messages in the message-pool, so we need to make sure they have all been processed nd then clean them all out.
	assert((cluster->messages && cluster->msg_count > 0) || (cluster->messages == NULL && cluster->msg_count == 0));
	if (cluster->messages) {
		while (cluster->msg_count > 0) {
			msg = cluster->messages[cluster->msg_count];
			if (msg) {
				
			}
			cluster->msg_count --;
		}
		free(cluster->messages);
		cluster->messages = NULL;
	}

	// the hashmasks array should be empty at this point, because freeing each server should have 
	// removed it from this list already.
	assert((cluster->mask == 0 && cluster->hashmasks == NULL) || (cluster->mask > 0 && cluster->hashmasks));
	for (i=0; i<cluster->mask; i++) {
		assert(cluster->hashmasks[i] == NULL);
	}
	free(cluster->hashmasks);
	cluster->hashmasks = NULL;
	
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
	server->in_max = 0;

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


// get an available reply from the replies list for the server.
// TODO:PERFormance.  Instead of going through the list all the time, we should have some sort of 
// indicator of the last reply that was returned so that it can just use that.
reply_t * next_reply(cluster_t *cluster)
{
	reply_t *reply = NULL;
	reply_t *tmp;
	int i;
	
	assert(cluster);
	assert(cluster->reply_count >= 0);
	assert((cluster->reply_count == 0 && cluster->replies == NULL) || (cluster->reply_count > 0 && cluster->replies));
	
	for (i=0; i < cluster->reply_count; i++) {
		tmp = cluster->replies[i];
		assert(tmp);
		assert(tmp->id == i);
		
		if (tmp->available == 1) {
			
			reply = tmp;
			reply->available = 0;
			assert(reply->result == 0);
			
			// we found what we are looking for, so break out of the loop.
			i = cluster->reply_count;
		}
	}
	
	// if we didn't find an empty reply slot, then we need to create one.
	if (reply == NULL) {
		cluster->replies = realloc(cluster->replies, sizeof(reply_t *) * (cluster->reply_count + 1));
		reply = cluster->replies[cluster->reply_count];
		
		reply->id = cluster->reply_count;
		reply->available = 0;
		reply->result = 0;
		
		cluster->reply_count ++;
	}
	
	
	assert(reply);
	return(reply);
}


// when a reply structure is no longer needed, it can be returned to the pool to be re-used.  
// Since the pointer is just referencing an item in an array, we only need to clear and initialise 
// the structure and mark it as available.
void return_reply(reply_t *reply)
{
	assert(reply);
	
	assert(reply->available == 0);
	assert(reply->id >= 0);
	
	reply->available = 1;
	reply->result = 0;
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
	
	assert(cluster);
	assert(server);
	
	// if non-blocking, set socket in non-blocking mode.
	if (blocking == OC_NON_BLOCKING) {
		assert(0);
	}
	
	assert(server->in_length == 0);

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
		sent = recv(server->handle, server->in_buffer + offset + server->in_length, avail, 0);
		if (sent <= 0) {
			// socket has shutdown
			assert(0);
		}
		else {
			assert(sent <= avail);
			server->in_length += sent;

			// now that we've got data, we need to make sure we have enough for the header.
			assert(0);
			

			// if we've processed all the data, and we dont have any partial messages at the end of 
			// it, then we exit the loop.  If we do have a partial message, then we need to continue 
			// waiting for more data.
			assert(0);
		}
	}
	
	
	// if the data is cluster control stuff (commands), then handle it automatically. 
	assert(0);
	
	// if the data is a reply, then we need to put it in the 'replies' array..
	assert(0);
	
	// if non-blocking, set socket back to blocking mode.
	if (blocking == OC_NON_BLOCKING) {
		assert(0);
	}
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

//--------------------------------------------------------------------------------------------------
// the included message has the details for the reply, so we need to just send the data, and then 
// wait for the replies to come in (if we are waiting for them).
static int send_request(cluster_t *cluster, server_t *server, message_t *msg, int wait_for_reply)
{
	ssize_t sent, datasent;
	int avail;

	assert(cluster);
	assert(server);
	
	// if we are not connected to this server, then we need to connect.
	if (server->handle < 0) {
		if (server_connect(cluster, server) != 0) {
			return(-1);
		}
	}
	
	assert(msg);
	assert(msg->datalen > 0);
	assert(msg->data);
	assert(msg->reply);
	
	// send the data
	datasent = 0;
	assert(server->handle > 0);
	while (datasent < msg->datalen && server->handle > 0) {
		avail = msg->datalen - datasent;
		sent = send(server->handle, msg->data + datasent, avail, 0);
		assert(sent != 0);
		assert(sent <= avail);
		if (sent < 0) {
			server_closed(cluster, server);
			assert(server->active == 0);
		}
		else {
			datasent += sent;
		}
	}
	
	if (server->active == 0) {
		// we lost connection during the send.  make sure we will return a NULL.
		return(-1);
	}
	
	
	// if we are going to be waiting for the data....
	if (wait_for_reply == WAIT_FOR_REPLY) {
		assert(0);
// 		while (not found) {
// 			pending_server(cluster, server, OC_BLOCKING);
// 			check for our reply.
// 		}
	}
	
	
	return(0);
}




// this function will return a message object that can be used to build an outgoing message to a 
// server.  It will grab a 'reply' slot and include it in the message, so when the message is sent, 
// the reply is kept with it.
//
// when finished with the message, you need to use message_return() to return it to the pool.
static message_t * message_new(cluster_t *cluster, int command)
{
	message_t *msg = NULL;
	
	assert(cluster);
	assert(command > 0);
	
	// get a message from the pool.
	assert(0);
	
	// get the reply object from the pool in the cluster.
	assert(0);
	
	// build the header, and set the 'command' and the 'userid' field.
	assert(0);
	
	
	assert(msg);
	return(msg);
}



static void message_return(cluster_t *cluster, message_t *msg)
{
	assert(cluster);
	assert(msg);
	
	// verify that there is no data left on the message.
	assert(0);
	
	// verify that there is a reply attached to the message, return it to the pool.
	assert(0);
	
	// mark the message as ready to use.
	assert(0);
}





int server_connect(cluster_t *cluster, server_t *server)
{
	message_t *msg;
	int res=-1;
	
	
	// if server has a handle, then we exit, cause we are already connected to it.
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
			server->active = 1;

			msg = message_new(cluster, CMD_HELLO);
			
			// send the request and receive the reply.
			if (send_request(cluster, server, msg, WAIT_FOR_REPLY) != 0) {
				// some error occured.  SHould handle this some how.
				assert(0);
			}
			else {
			
				if (msg->reply->result == 0) {
					assert(server->active == 1);
					assert(res == 0);
					
					// after a HELLO is given, we should expect a serverlist and a 
					// hashmask to arrive.  We need to keep processing data until they 
					// arrive, then we can be sure that we have connected to the 
					// cluster properly.
					while (server->hashmasks == NULL) {
						pending_server(cluster, server, OC_BLOCKING);
						
						// if we dont yet have a set of hashmasks, then we need to wait until we do.
						if (server->hashmasks == NULL) {
							
							// TODO: we should put a timeout here.  If we dont get the hashmasks after a 
							// period of time, then we give up and mark the server as not-responding.
							
							assert(0);
						}
						else {
							assert(res < 0);
							res = 0;
						}
					}
				}
				else {
					res = msg->reply->result;
					server->active = 0;
					
					
					// failed to connect to the server.  Should we do anything else with it?
					// TODO: we should probably put some sort of timestamp in there so we dont try and connect again to this server for a few seconds.
					assert(0);
				}
			}
			
			message_return(cluster, msg);
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
	int res = 0;
	server_t *server;
	int try = 0;
	int connected = 0;

	assert(cluster->servers);
	assert(cluster->server_count > 0);

	for (try=0; try < cluster->server_count && connected == 0; try++) {
	
		server = cluster->servers[try];
		assert(server);

		if (server_connect(cluster, server) == 0) {
			connected ++;
		}
	}
	
	if (connected == 0) { assert(res != 0); }
	
	return(res);
}


void server_disconnect(cluster_t *cluster, server_t *server)
{
	assert(cluster);
	assert(server);
	
	
	// make sure there are no outstanding requests to be retrieved.
	assert(0);
	
	// close the connection.
	assert(0);
	
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




int cluster_setint(cluster_t *cluster, const char *name, const int value)
{
	int res = 0;;
	
	assert(cluster);
	assert(name);
	
	// get a hash of the name
	assert(0);
	
	// get the index from the hash mask.
	assert(0);
	
	// get the server object from the index.
	assert(0);
	
	// make sure server is connected.  if not, then connect and wait for the initial handshaking.
	assert(0);
	
	// build the message and send it off.
	assert(0);

	// process pending data on server (blocking), until reply is received.
	assert(0);
	
	return(res);
}

int cluster_getint(cluster_t *cluster, const char *name, const int *value)
{
	int res = 0;;
	
	assert(cluster);
	assert(name);
	assert(value);
	
	// get a hash of the name
	assert(0);
	
	// get the index from the hash mask.
	assert(0);
	
	// get the server object from the index.
	assert(0);
	
	// make sure server is connected.  if not, then connect and wait for the initial handshaking.
	assert(0);
	
	// build the message and send it off.
	assert(0);

	// process pending data on server (blocking), until reply is received.
	assert(0);

	return(res);
}

