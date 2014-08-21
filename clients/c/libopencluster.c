//-----------------------------------------------------------------------------
// libopencluster
// library interface to communicate with the opencluster service.

#include "opencluster.h"

#include <arpa/inet.h>
#include <assert.h>
#include <ctype.h>
#include <endian.h>
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

// start out with an 1kb buffer.  Whenever it is full, we will double the
// buffer, so this is just a minimum starting point.
#define OPENCLUSTER_DEFAULT_BUFFSIZE (1024)


#define DEFAULT_BUFFER_SIZE   OPENCLUSTER_DEFAULT_BUFFSIZE

// #define WAIT_FOR_REPLY 1


#define REPLY_FAIL                          0x0003
#define REPLY_OK                            0x0010
#define REPLY_DATA_INT                      0x0110
#define REPLY_DATA_STRING                   0x0120

#define COMMAND_HELLO                       0x0010
#define COMMAND_GOODBYE                     0x0040
#define COMMAND_GET_INT                     0x2010
#define COMMAND_GET_STRING                  0x2020
#define COMMAND_SET_INT                     0x2200
#define COMMAND_SET_STRING                  0x2210


// this structure is not packed on word boundaries, so it should represent the 
// data received over the network.
#pragma pack(push,1)
typedef struct {
	uint16_t command;
	uint16_t reply;
	uint32_t userid;
	uint32_t length;
} raw_header_t;
#pragma pack(pop)

// The following macro can be used to help find situations where a peice of memory is free'd twice.  
// Useful to track down some difficult memory corruption bugs.
// #define free(x) printf("free: %08X, line:%d\n", (unsigned int)(x), __LINE__); free(x); x=NULL;


typedef struct {
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


typedef uint64_t mask_t;


// information about a server we know about.
typedef struct {
	int handle;
	char active;
	char closing;
	char shutdown;

	conninfo_t *conninfo;
	
	char *in_buffer;
	int in_length;
	int in_max;

	int ready;
	
} server_t;




typedef struct {
	
	int server_count;
	void **servers;

	unsigned long long mask;
	
	void *payload;
	int payload_max;
	int payload_length;
	
	int disconnecting;
	short debug;

	message_t message;
	
} cluster_t;


//-----------------------------------------------------------------------------
// function pre-declaration.
static int server_connect(cluster_t *cluster, server_t *server);





//-----------------------------------------------------------------------------
// Initialise a cluster object, and return it.
OPENCLUSTER cluster_init(void)
{
	cluster_t *cluster;
	
	cluster = calloc(1, sizeof(cluster_t));
	assert(cluster);

	cluster->servers = NULL;
	cluster->server_count = 0;

	assert(DEFAULT_BUFFER_SIZE > 0);
	cluster->payload = malloc(DEFAULT_BUFFER_SIZE);
	cluster->payload_max = DEFAULT_BUFFER_SIZE;
	cluster->payload_length = 0;
	assert(cluster->payload);
	
	// useful when trying to get more servers for backup connections.  We need to know what key's to look for.
	cluster->mask = 0;			
	
	cluster->disconnecting = 0;

	assert(cluster->message.out.command == 0);
	assert(cluster->message.id == 0);
	
	return(cluster);
}


void cluster_debug_on(OPENCLUSTER cluster_ptr)
{
	cluster_t *cluster = cluster_ptr;
	assert(cluster);
	assert(cluster->debug >> 1 == 0); // check that it is either a 1 or a zero.
	cluster->debug = 1;
}

void cluster_debug_off(OPENCLUSTER cluster_ptr) 
{
	cluster_t *cluster = cluster_ptr;
	assert(cluster);
	assert(cluster->debug >> 1 == 0); // check that it is either a 1 or a zero.
	cluster->debug = 0;
}





static void server_free(server_t *server)
{
	assert(server->conninfo);
	conninfo_free(server->conninfo);
	server->conninfo = NULL;

	assert(server->handle < 0);
	
	assert(server->in_length == 0);
	assert(server->in_buffer);
	assert(server->in_max > 0);
	free(server->in_buffer);
	
	free(server);
}

//-----------------------------------------------------------------------------
// free all the resources used by the 'cluster' object.
void cluster_free(OPENCLUSTER cluster_ptr)
{
	cluster_t *cluster = cluster_ptr;
	
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
// 'conninfo' is controlled by the cluster after this function.  If it is a duplicate of an existing entry, it will be discarded.
void cluster_addserver(OPENCLUSTER cluster_ptr, conninfo_t *conninfo)
{
	server_t *server = NULL;
	cluster_t *cluster = cluster_ptr;
	int i;
	
	assert(cluster);
	assert(conninfo);

	//TODO *** first need to check that the server isn't already in the list.  If it is, return -1.
	assert((cluster->server_count == 0 && cluster->servers == NULL) || (cluster->server_count > 0 && cluster->servers));
	for (i=0; i<cluster->server_count && server==NULL; i++) {
		assert(cluster->servers[i]);
		server = cluster->servers[i];
		assert(server->conninfo);
		if (conninfo_compare(server->conninfo, conninfo) != 0) {
			server = NULL;
		}
	}
	
	if (server == NULL) {
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
		
		server->conninfo = conninfo;
		
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
}




static int sock_resolve(const char *remote_addr, struct sockaddr_in *pSin)
{
	unsigned long ulAddress;
	struct hostent *hp;
	char *copy;
	char *first;
	char *next;
	char *host = NULL;
	int port = OPENCLUSTER_DEFAULT_PORT;
	int result = -1;

	assert(remote_addr);
	
	if (remote_addr) {
		copy = strdup(remote_addr);
		assert(copy);
		next = copy;
		first = strsep(&next, ":");
		assert(first == copy);
		
		if (next == NULL) {
			// no port was supplied.
			host = strdup(remote_addr);
		}
		else {
			port = atoi(next);
			assert(port > 0);
			host = strdup(first);
		}
		free (copy); copy = NULL;
	
		assert(host != NULL && host[0] != '\0' && port > 0);
		assert(pSin != NULL);
		
		// First, assign the family and port.
		pSin->sin_family = AF_INET;
		pSin->sin_port = htons(port);
		
		// Look up by standard notation (xxx.xxx.xxx.xxx) first.
		ulAddress = inet_addr(host);
		if ( ulAddress != (unsigned long)(-1) )  {
			// Success. Assign, and we're done.  Since it was an actual IP address, then we dont doany 
			// DNS lookup for that, so we cant do any check ing for any other address type (such as MX).
			pSin->sin_addr.s_addr = ulAddress;
			result = 0;
		}
		else {
		
			// If that didn't work, try to resolve host name by DNS.
			hp = gethostbyname(host);
			if( hp == NULL ) {
				// Didn't work. We can't resolve the address.
				result = -1;
			}
			else {
		
				// Otherwise, copy over the converted address and return success.
				memcpy( &(pSin->sin_addr.s_addr), &(hp->h_addr[0]), hp->h_length);
				result = 0;
			}
		}
	}
	else {
		assert(result == -1);
	}
	
	if (host) { free(host); }
	
	return(result);
}



static int sock_connect(const char *remote_addr)
{
	int handle = -1;
	struct sockaddr_in sin;
	
	assert(remote_addr);
		
	if (sock_resolve(remote_addr,&sin) >= 0) {
		handle = socket(AF_INET,SOCK_STREAM,0);
		if (handle >= 0) {
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
// replies to the server.
static void payload_header(cluster_t *cluster, uint16_t command, uint16_t reply, uint32_t userid)
{
	raw_header_t *raw;
	
	assert(cluster);
	assert(command > 0);
	assert(reply > 0);
	
	assert(cluster->payload);
	assert(cluster->payload_length == 0);
	assert(cluster->payload_max >= sizeof(raw_header_t));
	
	raw = cluster->payload;
	raw->command = htobe16(command);
	raw->reply = htobe16(reply);
	raw->userid = htobe32(userid);
	raw->length = 0;
	
	cluster->payload_length = sizeof(raw_header_t);
}



/*
// the payload is only used for the replies that are sent to the server.  
static void payload_int(cluster_t *cluster, int value)
{
	int avail;
	int32_t *ptr;
	raw_header_t *raw;
	
	// sanity check, making sure the integer type is the same as the 32-bit integer we will be pushing.
	assert(sizeof(int32_t) == sizeof(int));
	
	assert(cluster);
	assert(cluster->payload_length >= sizeof(raw_header_t));
	
	avail = cluster->payload_max - cluster->payload_length;
	if (avail < sizeof(int)) {
		cluster->payload = realloc(cluster->payload, cluster->payload_max + DEFAULT_BUFFER_SIZE);
		assert(cluster->payload);
		cluster->payload_max += DEFAULT_BUFFER_SIZE;
	}
	
	ptr = ((void*) cluster->payload + cluster->payload_length);
	ptr[0] = htobe32(value);
	
	cluster->payload_length += sizeof(int32_t);
	
	// update the length in the header.
	assert(cluster->payload_length > sizeof(raw_header_t));
	raw = cluster->payload;
	raw->length = htobe32(cluster->payload_length - sizeof(raw_header_t));
	
	assert(cluster->payload);
	assert(cluster->payload_length <= cluster->payload_max);
}
*/


/*
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
	ptr[0] = htobe32(length);
	
	memcpy(cluster->payload + cluster->payload_length + sizeof(int), data, length);
	
	cluster->payload_length += sizeof(int) + length;

	// update the length in the header.
	assert(cluster->payload_length > sizeof(raw_header_t));
	raw = cluster->payload;
	raw->length = htobe32(cluster->payload_length - sizeof(raw_header_t));

	assert(cluster->payload);
	assert(cluster->payload_length <= cluster->payload_max);
}
*/


/*
static void payload_string(cluster_t *cluster, const char *str)
{
	assert(cluster);
	assert(str);
	payload_data(cluster, strlen(str), (void*)str);
}
*/

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



static void reply_ok(cluster_t *cluster, server_t *server, short repcmd, int userid)
{
	assert(cluster);
	assert(server);
	assert(repcmd > 0);
	
	// if we are not connected to this server, then how did we get the message we are replying to?
	assert(server->handle > 0);

	payload_header(cluster, repcmd, REPLY_OK, userid);
	send_reply(cluster, server);
}

/*
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


static void * data_hash(void *data, uint64_t *value)
{
	void *next;
	uint64_t *raw;
	
	assert(data);
	assert(value);
	
	assert(sizeof(uint64_t) == 8);
	
	raw = data;
	value[0] = be64toh(raw[0]);
	next = data + sizeof(uint64_t);
	
	return(next);
}



static void * data_long(void *data, int64_t *value)
{
	void *next;
	int64_t *raw;
	
	assert(data);
	assert(value);
	
	assert(sizeof(int64_t) == 8);
	
	raw = data;
	value[0] = be64toh(raw[0]);
	next = data + sizeof(int64_t);
	
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
*/





// this function will ensure that there is not any pending data on the incoming socket for the 
// server.  Since the server details are not exposed outside of the library, this is an internal 
// function.   Developers will need to call cluster_pending which will process pending data on all 
// the connected servers.
static void pending_server(cluster_t *cluster, server_t *server)
{
	int done = 0;
	int offset = 0;
	int avail;
	int sent;
	raw_header_t *header;
	int length;
	short reply;
	int userid;
	int inner = 0;
	short command;
	
	assert(cluster);
	assert(server);
	assert(server->active);
	
	assert(server->in_length == 0);

	// Outer loop that keeps looping until there are no messages that we are waiting to receive.
	done = 0;
	while (done == 0) {
	
		assert(server->in_buffer);
		assert(server->in_max > 0);
		assert(server->in_length >= 0);

		avail = server->in_max - server->in_length;
		
		// if we have less than a buffer size available, then we need to expand the size of the buffer.
		if (avail < DEFAULT_BUFFER_SIZE) {
			assert(DEFAULT_BUFFER_SIZE > 0);
			server->in_max += DEFAULT_BUFFER_SIZE;
			assert(server->in_max >= (server->in_length + DEFAULT_BUFFER_SIZE));  // will catch if we roll over.
			server->in_buffer = realloc(server->in_buffer, server->in_max);
			assert(server->in_buffer);
			avail = server->in_max - server->in_length;
		}
		
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
					// we dont have enough data yet, so we need to break out of the inner loop.
					inner ++;
				}
				else {
					
					// Offset is used, because this function will process all the pending messages, 
					// loading them into a single buffer that increases in size as needed.  The 
					// messages are not moved around the buffer unnecessarily.   We might need to 
					// have care that the buffer does not get too large.   
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
						
						command = ntohs(header->command);
						reply = ntohs(header->reply);
						userid = ntohl(header->userid);
						assert(userid >= 0);
						
						// if message is a reply, add it to the reply data.
						if (reply > 0) {
							
							assert(cluster->message.id == userid);
							assert(cluster->message.out.command == command);

							if (length > 0) {
								assert(cluster->message.in.max >= 0);
								assert((cluster->message.in.max == 0) || (cluster->message.in.max > 0 && cluster->message.in.payload));
								if (cluster->message.in.max < length) {
									cluster->message.in.payload = realloc(cluster->message.in.payload, length);
									cluster->message.in.max = length;
								}
								
								assert(cluster->message.in.max > 0);
								assert(cluster->message.in.payload);
								memcpy(cluster->message.in.payload, ptr, length);
							}
							cluster->message.in.result = reply;
							
// 							printf("Received reply (%d) from command (%d), length=%d\n", msg->in.result, repcmd, length);

						}
						else {
							// it is not a reply, it is a command.  We need to process that as well.
					
// 							printf("Command Received: %d\n", command); 
							switch (command) {
		
// 								case COMMAND_HASHMASK:    process_hashmask(cluster, server, userid, length, ptr);  break;
			
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
}


static int check_server_active(cluster_t *cluster, server_t *server)
{
	assert(cluster);
	assert(server);
	return(server->active);
}

static void log_data(int handle, char *tag, unsigned char *data, int length)
{
	int i;
	int col;
	char buffer[512];	// line buffer.
	int len;  			// buffer length;
	int start;
	
	assert(tag);
	assert(data);
	assert(length > 0);

	i = 0;
	while (i < length) {

		start = i;
		
		// first put the tag in the buffer.
		strncpy(buffer, tag, sizeof(buffer));
		len = strlen(tag);
		
		// now put the line count.
		len += sprintf(buffer+len, "[%d] %04X: ", handle, i);
		
		// now display the columns of text.
		for (col=0; col<16; col++) {
			if (i < length && col==7) {
				len += sprintf(buffer+len, "%02x-", data[i]);
			}
			else if (i < length) {
				len += sprintf(buffer+len, "%02x ", data[i]);
			}
			else {
				len += sprintf(buffer+len, "   ");
			}
			
			i++;
		}
		
		// add a seperator
		len += sprintf(buffer+len, ": ");
		
		// now we display the plain text.
		assert(start >= 0);
		for (col=0; col<16; col++) {
			if (start < length) {
				if (isprint(data[start])) {
					len += sprintf(buffer+len, "%c", data[start]);
				}
				else {
					len += sprintf(buffer+len, ".");
				}
			}
			else {
				len += sprintf(buffer+len, " ");
			}
			
			start++;
		}

		assert(i == start);
		printf("%s\n", buffer);
	}
}



//--------------------------------------------------------------------------------------------------
// the included message has the details for the reply, so we need to just send the data, and then 
// wait for the replies to come in (if we are waiting for them).
static int send_request(cluster_t *cluster)
{
	ssize_t sent, datasent;
	int avail;
	int status;
	server_t *server;
	int server_entry;

	assert(cluster);

	assert(cluster->message.out.length >= sizeof(raw_header_t));
	assert(cluster->message.out.max >= cluster->message.out.length);
	assert(cluster->message.out.data);
	assert(cluster->message.in.result == 0);
	
	status = 0;
	for (server_entry = 0; status == 0 && server_entry < cluster->server_count; server_entry ++) {
		server = cluster->servers[server_entry];
		if (server) {

			if (check_server_active(cluster, server)) {
				assert(server->handle > 0);
			
				log_data(0, "SEND ", cluster->message.out.data, cluster->message.out.length);

				
				// send the data
				datasent = 0;
				status = 0;
				while (datasent < cluster->message.out.length && server->handle > 0) {
					avail = cluster->message.out.length - datasent;
					assert(avail > 0);
					sent = send(server->handle, cluster->message.out.data + datasent, avail, 0);
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
				
				assert(datasent == cluster->message.out.length);
				
				// if we are going to be waiting for the data....
				while (cluster->message.in.result == 0  && server->handle > 0) {
					pending_server(cluster, server);
				}
			}
		}
	}
	
	return(0);
}



static void message_new(cluster_t *cluster, short int command)
{
	raw_header_t *header;
	
	assert(cluster);
	assert(cluster->message.out.command == 0);
	cluster->message.out.command = command;

	
	if (cluster->message.out.data == NULL) {
		assert(OPENCLUSTER_DEFAULT_BUFFSIZE >= sizeof(raw_header_t));
		cluster->message.out.data = malloc(OPENCLUSTER_DEFAULT_BUFFSIZE);
		cluster->message.out.max = OPENCLUSTER_DEFAULT_BUFFSIZE;
		assert(cluster->message.out.data);
	}
	
	cluster->message.out.length = sizeof(raw_header_t);
	
	assert(cluster->message.out.data);
	header = cluster->message.out.data;

	header->command = htobe16(command);
	header->reply   = 0;
	header->userid  = 0;
	header->length  = htobe32(0);

	assert(cluster->message.in.result == 0);
}

static void message_done(cluster_t *cluster)
{
	assert(cluster);
	assert(cluster->message.out.command > 0);
	cluster->message.out.command = 0;
	cluster->message.in.result = 0;
}



static void msg_setstr(cluster_t *cluster, const char *str) 
{
	raw_header_t *header;
	int *ptr;
	char * sptr;
	int slen;
	
	assert(cluster);

	if (str) { slen = strlen(str); }
	else { slen = 0; }
	
	// make sure there is enough space in the buffer.
	while ((cluster->message.out.length + sizeof(int) + slen) > cluster->message.out.max) {
		cluster->message.out.data = realloc(cluster->message.out.data, cluster->message.out.max + DEFAULT_BUFFER_SIZE);
		cluster->message.out.max += DEFAULT_BUFFER_SIZE;
	}

	ptr = ((void*)cluster->message.out.data + cluster->message.out.length);
	ptr[0] = htonl(slen);

	if (slen > 0) {
		sptr = ((void*)cluster->message.out.data + cluster->message.out.length + sizeof(int));
		memcpy(sptr, str, slen);
	}
	
	cluster->message.out.length += (sizeof(int) + slen);
	
	// add the msg details to the outgoing payload buffer.
	assert(cluster->message.out.max >= sizeof(raw_header_t));
	assert(cluster->message.out.data);
	
	header = cluster->message.out.data;
	header->length = htonl(cluster->message.out.length - sizeof(raw_header_t));
}



static int server_connect(cluster_t *cluster, server_t *server)
{
	int res=-1;
	
	assert(cluster);
	assert(server);
	
	// if server has a handle, then we exit, because we are already connected to it.
	if (server->handle < 0) {
		
		assert(server->active == 0);
		assert(server->closing == 0);
		assert(server->shutdown == 0);
		
		assert(server->conninfo);
		const char *remote_addr = conninfo_remoteaddr(server->conninfo);
		assert(remote_addr);
		
		server->handle = sock_connect(remote_addr);
		assert(res < 0);
		
		if (server->handle < 0) {
			res = -1;
			assert(server->active == 0);
		}
		else {
			assert(res < 0);

			printf("sending HELLO(%X)\n", COMMAND_HELLO);
			message_new(cluster, COMMAND_HELLO);
			msg_setstr(cluster, NULL);
			
			printf("New message.  length = %d\n", cluster->message.out.length);

			log_data(0, "output ", cluster->message.out.data, cluster->message.out.length);

			// temporarily set the server as active.
			server->active = 1;

			
			// send the request and receive the reply.
			assert(cluster->message.out.length > 0);
			if (send_request(cluster) != 0) {
				// some error occured.  SHould handle this some how.
				server->active = 0;

				assert(0);
			}
			else {
				// check the result.
				
				// need to mark the server as active.
				assert(server->active == 1);
				
				res = 0;
			}
			
			message_done(cluster);
		}
	}

	return(res);
}


// This function is used to make sure there is no pending commands in any of the server connections.
void cluster_pending(OPENCLUSTER cluster_ptr) 
{
	cluster_t *cluster = cluster_ptr;
	int i;
	
	assert(cluster);
	assert(cluster->server_count > 0);
	assert(cluster->servers);
	
	for (i=0; i < cluster->server_count; i++) {
		if (cluster->servers[i] != NULL) {
			pending_server(cluster, cluster->servers[i]);
		}
	}
}


// do nothing if we are already connected.  If we are not connected, then 
// connect to the first server in the list.  Since we are setup for blocking 
// activity, we will wait until the connect succeeds or fails.
int cluster_connect(OPENCLUSTER cluster_ptr)
{
	cluster_t *cluster = cluster_ptr;
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


static void server_disconnect(cluster_t *cluster, server_t *server)
{
	assert(cluster);
	assert(server);
	
	// if we are connected to the server, send the GOODBYE request and receive the reply.
	if (server->handle >= 0) {
		// send a GOODBYE command  first.
		message_new(cluster, COMMAND_GOODBYE);
		
		assert(cluster->message.in.result == 0);
		assert(cluster->message.out.length > 0);
		if (send_request(cluster) != 0) {
			assert(0);
			
			// what?
		}
		
		message_done(cluster);
		
		// close the connection.
		assert(server->handle > 0);
		close(server->handle);
		server->handle = -1;
	}
}

void cluster_disconnect(OPENCLUSTER cluster_ptr)
{
	cluster_t *cluster = cluster_ptr;
	server_t *server;
	int try = 0;

	assert(cluster);
	assert(cluster->servers);
	assert(cluster->server_count > 0);
	
	assert(cluster->disconnecting == 0);
	cluster->disconnecting = 1;
	
	for (try=0; try < cluster->server_count; try++) {
	
		server = cluster->servers[try];
		if (server) {
			server_disconnect(cluster, server);
		}
	}
}

#define FNV_BASE_LONG   14695981039346656037llu
#define FNV_PRIME_LONG  1099511628211llu                      



// FNV hash.  Public domain function converted from public domain Java version.
// TODO: modify this to a macro to improve performance a little.
hash_t cluster_hash_bin(const char *str, const int length)
{
	register int i;
	register hash_t hash = FNV_BASE_LONG;

	for (i=0; i<length; i++)  {
		hash ^= (unsigned int)str[i];
		hash *= FNV_PRIME_LONG;
	}

	return(hash);
}


hash_t cluster_hash_str(const char *str)
{
	return(cluster_hash_bin(str, strlen(str)));
}


hash_t cluster_hash_int(const int key)
{
	register int i;
	register hash_t hash = FNV_BASE_LONG;
	union {
		int nkey;
		char str[sizeof(int)];
	} match;

	
	assert(sizeof(key) == 4);
	assert(sizeof(match) == sizeof(key));
	
	match.nkey = htobe32(key);

	for (i=0; i<sizeof(key); i++)  {
		hash ^= (unsigned int)match.str[i];
		hash *= FNV_PRIME_LONG;
	}

	return(hash);
}


hash_t cluster_hash_long(const long long key)
{
	register int i;
	register hash_t hash = FNV_BASE_LONG;
	union {
		long long nkey;
		char str[sizeof(long long)];
	} match;

	assert(sizeof(key) == 8);
	assert(sizeof(match) == sizeof(key));
	
	match.nkey = htobe64(key);

	for (i=0; i<sizeof(key); i++)  {
		hash ^= (unsigned int)match.str[i];
		hash *= FNV_PRIME_LONG;
	}

	return(hash);
}



static void msg_setint(cluster_t *cluster, const int value) 
{
	raw_header_t *header;
	int *ptr;
	
	assert(cluster);
	
	// make sure there is enough space in the buffer.
	if ((cluster->message.out.length + sizeof(int)) > cluster->message.out.max) {
		cluster->message.out.data = realloc(cluster->message.out.data, cluster->message.out.max + DEFAULT_BUFFER_SIZE);
		cluster->message.out.max += DEFAULT_BUFFER_SIZE;
	}

	ptr = ((void*)cluster->message.out.data + cluster->message.out.length);
	ptr[0] = htonl(value);
	cluster->message.out.length += sizeof(int);
	
	// add the msg details to the outgoing payload buffer.
	assert(cluster->message.out.max >= sizeof(raw_header_t));
	assert(cluster->message.out.data);
	
	header = cluster->message.out.data;
	header->length = htonl(cluster->message.out.length - sizeof(raw_header_t));
}


static void msg_sethash(cluster_t *cluster, hash_t value) 
{
	raw_header_t *header;
	uint64_t *ptr;
	
	assert(cluster);
	assert(sizeof(hash_t) == sizeof(uint64_t));
	
	// make sure there is enough space in the buffer.
	if ((cluster->message.out.length + sizeof(value)) > cluster->message.out.max) {
		cluster->message.out.data = realloc(cluster->message.out.data, cluster->message.out.max + DEFAULT_BUFFER_SIZE);
		cluster->message.out.max += DEFAULT_BUFFER_SIZE;
	}

	ptr = ((void*)cluster->message.out.data + cluster->message.out.length);
	ptr[0] = htobe64(value);
	cluster->message.out.length += sizeof(value);
	
	// add the msg details to the outgoing payload buffer.
	assert(cluster->message.out.max >= sizeof(raw_header_t));
	assert(cluster->message.out.data);
	
	header = cluster->message.out.data;
	header->length = htobe32(cluster->message.out.length - sizeof(raw_header_t));
}


static void msg_setlong(cluster_t *cluster, long long value) 
{
	raw_header_t *header;
	long long *ptr;
	
	assert(cluster);
	assert(sizeof(long long) == sizeof(uint64_t));
	
	// make sure there is enough space in the buffer.
	if ((cluster->message.out.length + sizeof(value)) > cluster->message.out.max) {
		cluster->message.out.data = realloc(cluster->message.out.data, cluster->message.out.max + DEFAULT_BUFFER_SIZE);
		cluster->message.out.max += DEFAULT_BUFFER_SIZE;
	}

	ptr = ((void*)cluster->message.out.data + cluster->message.out.length);
	ptr[0] = htobe64(value);
	cluster->message.out.length += sizeof(value);
	
	// add the msg details to the outgoing payload buffer.
	assert(cluster->message.out.max >= sizeof(raw_header_t));
	assert(cluster->message.out.data);
	
	header = cluster->message.out.data;
	header->length = htobe32(cluster->message.out.length - sizeof(raw_header_t));
}








int cluster_setint(OPENCLUSTER cluster_ptr, hash_t map_hash, hash_t key_hash, const int value, const int expires)
{
	cluster_t *cluster = cluster_ptr;
	int res = 0;
	
	assert(cluster);
	assert(expires >= 0);
	
	// build the message and send it off.
	message_new(cluster, COMMAND_SET_INT);
	msg_sethash(cluster, map_hash);
	msg_sethash(cluster, key_hash);
	msg_setint(cluster,  expires);
	msg_setlong(cluster, value);

	assert(cluster->message.in.result == 0);
	assert(cluster->message.out.length > 0);
	send_request(cluster);		// this will wait for the reply, so we should have the result.

	// now we've got a reply, we free the message, because there is no 
	assert(cluster->message.in.result == REPLY_OK);
	message_done(cluster);
	
	return(res);
}



int cluster_setstr(OPENCLUSTER cluster_ptr, hash_t map_hash, hash_t key_hash, const char *value, const int expires)
{
	cluster_t *cluster = cluster_ptr;
	int res = 0;
	
	assert(cluster);
	assert(expires >= 0);
	
	// build the message and send it off.
	message_new(cluster, COMMAND_SET_STRING);
	msg_sethash(cluster, map_hash);
	msg_sethash(cluster, key_hash);
	msg_setint(cluster, expires);
	msg_setstr(cluster, value);

	assert(cluster->message.out.length > 0);
	send_request(cluster);
	
	// now we've got a reply, we free the message, because there is no 
	assert(cluster->message.in.result == REPLY_OK);
	
	message_done(cluster);
	
	return(res);
}



static void msg_getint(cluster_t *cluster, int *value)
{
	int *ptr;
	
	assert(cluster);
	assert(value);
	assert(cluster->message.in.offset >= 0);
	
	ptr = ((void*)(cluster->message.in.payload) + cluster->message.in.offset);
	*value = ntohl(*ptr);

	cluster->message.in.offset += sizeof(uint32_t);
}


static void msg_gethash(cluster_t *cluster, hash_t *value)
{
	uint64_t *ptr;
	
	assert(cluster);
	assert(value);
	assert(cluster->message.in.offset >= 0);
	assert(sizeof(hash_t) == sizeof(uint64_t));
	
	ptr = ((void*)(cluster->message.in.payload) + cluster->message.in.offset);
	*value = be64toh(*ptr);

	cluster->message.in.offset += sizeof(uint64_t);
}


static void msg_getlong(cluster_t *cluster, long long *value)
{
	uint64_t *ptr;
	
	assert(cluster);
	assert(value);
	assert(cluster->message.in.offset >= 0);
	assert(sizeof(long long) == sizeof(uint64_t));
	
	ptr = ((void*)(cluster->message.in.payload) + cluster->message.in.offset);
	*value = be64toh(*ptr);

	cluster->message.in.offset += sizeof(uint64_t);
}




static void msg_getstr(cluster_t *cluster, char **value, int *length)
{
	int *ptr_len;
	char *ptr_str;
	int len;
	char *str;

	assert(cluster);
	assert(value);
	assert(length);
	assert(cluster->message.in.offset >= 0);
	
	ptr_len = ((void*)(cluster->message.in.payload) + cluster->message.in.offset);
	len = ntohl(*ptr_len);
	cluster->message.in.offset += sizeof(uint32_t);
	
	ptr_str = ((void*)(cluster->message.in.payload) + cluster->message.in.offset);
	assert(len > 0);
	str = malloc(len + 1);
	memcpy(str, ptr_str, len);
	str[len] = 0;
	
	*value = str;
	*length = len;
}



int cluster_getint(OPENCLUSTER cluster_ptr, hash_t map_hash, hash_t key_hash)
{
	cluster_t *cluster = cluster_ptr;
	int value=0;
	
	assert(cluster);
	assert(value);
	
	// build the message and send it off.
	message_new(cluster, COMMAND_GET_INT);
	msg_sethash(cluster, map_hash);
	msg_sethash(cluster, key_hash);
	
	assert(cluster->message.in.result == 0);
	assert(cluster->message.out.length > 0);
	send_request(cluster);
	
	// now we've got a reply, we free the message, because there is no 
	if (cluster->message.in.result == REPLY_DATA_INT) {
		
		hash_t in_keyhash;
		hash_t in_maphash;
		hash_t in_valuehash;
		long long in_value;
		
		msg_gethash(cluster, &in_maphash);
		msg_gethash(cluster, &in_keyhash);
		msg_gethash(cluster, &in_valuehash);
		msg_getlong(cluster, &in_value);
		assert(in_maphash == map_hash);
		assert(in_keyhash == key_hash);
		
		value = (int) in_value;
	}
	else {
		// need to do something else... since we didnt get the data.
		assert(0);
	}

	message_done(cluster);

	return(value);
}


char * cluster_getstr(OPENCLUSTER cluster_ptr, hash_t map_hash, hash_t key_hash)
{
	char *str = NULL;
	int str_len;
	cluster_t *cluster = cluster_ptr;
	
	assert(cluster);
	
	// build the message and send it off.
	message_new(cluster, COMMAND_GET_STRING);
	msg_sethash(cluster, map_hash);
	msg_sethash(cluster, key_hash);
	msg_setint(cluster, 0);			// unlimited string size.  //** This needs to be done differntly.
	
	assert(cluster->message.in.result == 0);
	assert(cluster->message.out.length > 0);
	send_request(cluster);
		
	// now we've got a reply, we free the message, because there is no 
	if(cluster->message.in.result == REPLY_DATA_STRING) {
		
		hash_t in_maphash;
		hash_t in_keyhash;
		hash_t in_valuehash;
		
		msg_gethash(cluster, &in_maphash);
		msg_gethash(cluster, &in_keyhash);
		msg_gethash(cluster, &in_valuehash);
		msg_getstr(cluster, &str, &str_len);

		assert(in_maphash == map_hash);
		assert(in_keyhash == key_hash);
		
		assert(str);
		assert(str_len > 0);
		
		if (cluster->debug) {
			printf("=== map_hash=%#llx, key_hash=%#llx\n", (long long unsigned) map_hash, (long long unsigned) key_hash);
		}
	}
	else {
		// the data was not available so we return a fail result.
		assert(0);
	}
	
	message_done(cluster);
	
	return(str);
}


// Return the number of active servers in the cluster.
int cluster_servercount(OPENCLUSTER cluster_ptr)
{
	int count = 0;
	int i;
	server_t *server;
	cluster_t *cluster = cluster_ptr;
	
	assert(cluster);
	assert(cluster->server_count >= 0);
	
	for (i=0; i<cluster->server_count; i++) {
		assert(cluster->servers);
		if (cluster->servers[i]) {
			server = cluster->servers[i];
			if (server->active > 0) {
				count ++;
			}
		}
	}
	
	return(count);
}


