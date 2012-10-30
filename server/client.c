// client.c

#define __CLIENT_C
#include "client.h"
#undef __CLIENT_C


#include "commands.h"
#include "constants.h"
#include "globals.h"
#include "header.h"
#include "logging.h"
#include "node.h"
#include "process.h"
#include "protocol.h"
#include "push.h"
#include "stats.h"
#include "timeout.h"

#include <assert.h>
#include <ctype.h>
#include <endian.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>


// external variables.
extern int _conncount;		// server.c




client_t **_clients = NULL;
int _client_count = 0;


struct timeval _standard_timeout = {5,0};



static void read_handler(int fd, short int flags, void *arg);
static void write_handler(int fd, short int flags, void *arg);


void **_commands = NULL;
int _command_max = 0;

void client_init_commands(int max)
{
	assert(_commands == NULL);
	assert(_command_max == 0);
	assert(max > 0);
	
	_commands = calloc(max, sizeof(void *));
	_command_max = max;
	
	cmd_init();
	process_init();
}

void client_add_cmd(int cmd, void *fn)
{
	int new_max;
	
	assert(_commands);
	assert(_command_max > 0);
	
	if (cmd >= _command_max) {
		new_max = cmd + 512;
		_commands = realloc(_commands, sizeof(void *) * new_max);
		for ( ; _command_max < new_max; _command_max++) {
			_commands[_command_max] = NULL;
		}
	}
	
	assert(cmd < _command_max);
	assert(_commands[cmd] == NULL);
	_commands[cmd] = fn;
}


client_t * client_new(void)
{
	client_t *client;
	
	client = calloc(1, sizeof(client_t));
	assert(client);
	
	client->handle = INVALID_HANDLE;
	
	client->read_event = NULL;
	client->write_event = NULL;
	client->shutdown_event = NULL;
	
	client->out.buffer = NULL;
	client->out.offset = 0;
	client->out.length = 0;
	client->out.max = 0;
	client->out.total = 0;
	
	client->in.buffer = NULL;
	client->in.offset = 0;
	client->in.length = 0;
	client->in.max = 0;
	client->in.total = 0;
	
	client->nextid = 0;
	
	client->timeout_limit = CLIENT_TIMEOUT_LIMIT;
	client->timeout = 0;
	client->tries = 0;
	
	client->closing = 0;

	// add the new client to the clients list.
	if (_client_count > 0) {
		assert(_clients != NULL);
		if (_clients[_client_count-1] == NULL) {
			_clients[_client_count-1] = client;
		}
		else {
			_clients = realloc(_clients, sizeof(void*)*(_client_count+1));
			assert(_clients);
			_clients[_client_count] = client;
			_client_count ++;
		}
	}	
	else {
		assert(_clients == NULL);
		_clients = malloc(sizeof(void*));
		_clients[0] = client;
		_client_count = 1;
	}
	assert(_clients && _client_count > 0);
	
	assert(client->transfer_bucket == NULL);
	
	assert(client);
	return(client);
}







//--------------------------------------------------------------------------------------------------
// Initialise the client structure.
// If 'server' is NULL, then this is probably a connection to another NODE in the cluster.
void client_accept(client_t *client, evutil_socket_t handle, struct sockaddr *address, int socklen)
{
	assert(client);

	assert(handle > 0);
	assert(client->handle < 0);
	client->handle = handle;
	
	logger(LOG_INFO, "New client - handle=%d", handle);

	assert(_evbase);
	assert(client->handle > 0);
	client->read_event = event_new( _evbase, client->handle, EV_READ|EV_PERSIST, read_handler, client);
	assert(client->read_event);
	int s = event_add(client->read_event, &_standard_timeout);
	assert(s == 0);
	
	_conncount ++;
}





//--------------------------------------------------------------------------------------------------
// Free the resources used by the client object.
void client_free(client_t *client)
{
	char found=0, resize=0;
	int i;
	
	assert(client);
	assert(client->transfer_bucket == NULL);
	
	logger(LOG_INFO, "client_free: handle=%d", client->handle);

	if (client->node) {
		node_detach_client(client->node);
	}

	assert(client->out.length == 0);
	assert(client->out.offset == 0);
	if (client->out.buffer) {
		free(client->out.buffer);
		client->out.buffer = NULL;
		client->out.max = 0;
	}

	assert(client->in.length == 0);
	assert(client->in.offset == 0);
	if (client->in.buffer) {
		free(client->in.buffer);
		client->in.buffer = NULL;
		client->in.max = 0;
	}

	if (client->read_event) {
		event_free(client->read_event);
		client->read_event = NULL;
	}
	
	if (client->write_event) {
		event_free(client->write_event);
		client->write_event = NULL;
	}
	
	assert(client->shutdown_event == NULL);

	if (client->handle != INVALID_HANDLE) {
		logger(LOG_DEBUG, "client_free: closing socket %d", client->handle);
		EVUTIL_CLOSESOCKET(client->handle);
		client->handle = INVALID_HANDLE;
	}
	
	// remove the client from the main list.
	assert(_clients);
	assert(_client_count > 0);
	assert(found == 0);
	assert(resize == 0);
	for (i=0; i < _client_count && found == 0; i++) {
		if (_clients[i] == client) {
			found ++;
			_clients[i] = NULL;
			if (i == (_client_count - 1)) {
				// client was at the end of the list, so we need to shorten it.
				_client_count --;
				assert(_client_count >= 0);
				resize ++;
	}	}	}
	
	logger(LOG_DEBUG, "found:%d, client_count:%d", found, _client_count);
	
	assert(found == 1);
	
	if (_client_count > 2) {
		// if the first client entry is null, but the last one isnt, then move the last one to the front.
		if (_clients[0] == NULL) {
			if (_clients[_client_count-1] != NULL) {
				_clients[0] = _clients[_client_count-1];
				_clients[_client_count-1] = NULL;
				_client_count --;
				resize ++;
	}	}	}

	if (resize > 0) {
		_clients = realloc(_clients, sizeof(void*)*_client_count);
	}
	
	_conncount --;
	
	assert(client);
	free(client);
	
	assert(_client_count >= 0);
}




// Process the messages received.  Because of the way the protocol was designed, almost every 
// operation and reply can be treated as a singular thing.  Enough information is included in a 
// reply for it to be handled correctly without having to know the details of the original command. 
static int process_data(client_t *client) 
{
	int processed = 0;
	int stopped = 0;
	char *ptr;
	header_t header;
	raw_header_t *raw;

	void (*func)(client_t *client, header_t *header, char *payload);
	
	assert(sizeof(char) == 1);
	assert(sizeof(short int) == 2);
	assert(sizeof(int) == 4);
	assert(sizeof(long long) == 8);
	
	assert(client);
	assert(client->handle > 0);

	while (stopped == 0) {
			
		assert(client->in.buffer);
		assert((client->in.length + client->in.offset) <= client->in.max);
		
		// if we dont have 10 characters, then we dont have enough to build a message.  Messages are 
		// at least that.
		if (client->in.length < HEADER_SIZE) {
			// we didn't have enough, even for the header, so we are stopping.
            logger(LOG_DEBUG, "[process_data] There wasn't enough data to build anything so not processing the buffer. in.length=%d", client->in.length);
			stopped = 1;
		}
		else {
			
			// keeping in mind the offset, get the 4 params, and determine what we need to do with 
			// them.
			
			// *** performance tuning.  We should only parse the header once.  It should be saved in 
			//     the client object and only done once.
			
			raw = (void *) (client->in.buffer + client->in.offset);

			header.command = be16toh(raw->command);
			header.repcmd  = be16toh(raw->repcmd);
			header.userid  = be32toh(raw->userid);
			header.length  = be32toh(raw->length);
			
			logger(LOG_DEBUG, "New telegram: Command=%d, repcmd=%d, userid=%d, length=%d, buffer_length=%d", 
					header.command, header.repcmd, 
					header.userid, header.length, client->in.length);
			
			if ((client->in.length-HEADER_SIZE) < header.length) {
				// we dont have enough data yet.
				stopped = 1;
			}
			else {
				
				// get a pointer to the payload
				ptr = client->in.buffer + client->in.offset + HEADER_SIZE;
				assert(ptr);

				if (header.command >= _command_max || _commands[header.command] == NULL) {

					if (header.repcmd != 0) {
						logger(LOG_ERROR, "Unknown reply: Reply=%d, Command=%d, userid=%d, length=%d", 
									header.repcmd, header.command, header.userid, header.length);
					}
					else {
						logger(LOG_ERROR, "Unknown command received: Command=%d, userid=%d, length=%d", header.command, header.userid, header.length);
						client_send_message(client, &header, REPLY_UNKNOWN, 0, NULL);
					}

					#ifndef NDEBUG
					assert(0);
					#endif							
				}
				else {
					assert(_commands[header.command]);
					func = _commands[header.command];
					(*func)(client, &header, ptr);
				}
				
				// need to adjust the details of the incoming buffer.
				client->in.length -= (header.length + HEADER_SIZE);
				assert(client->in.length >= 0);
				if (client->in.length == 0) {
					client->in.offset = 0;
					stopped = 1;
				}
				else {
					client->in.offset += (header.length + HEADER_SIZE);
				}
				assert( ( client->in.length + client->in.offset ) <= client->in.max);
			}	
		}	
	}
	
	assert(stopped != 0);
	
	return(processed);
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
		logger(LOG_EXTRA, "%s", buffer);
	}
}






// This function is called when data is available on the socket.  We need to 
// read the data from the socket, and process as much of it as we can.  We 
// need to remember that we might possibly have leftover data from previous 
// reads, so we will need to append the new data in that case.
static void read_handler(int fd, short int flags, void *arg)
{
	client_t *client = (client_t *) arg;
	int avail;
	int res;
	int processed;
	
	assert(fd >= 0);
	assert(flags != 0);
	assert(client);
	assert(client->handle == fd);

	if (flags & EV_TIMEOUT) {
		
		assert(client->timeout >= 0 && client->timeout < CLIENT_TIMEOUT_LIMIT);

		client->timeout ++;
		if (client->timeout >= client->timeout_limit) {
		
			// we timed out, so we should kill the client.
			logger(LOG_ERROR, "client timed out. handle=%d", fd);
			
			// because the client has timed out, we need to clear out any data that we currently 
			// have for it.
			client->in.offset = 0;
			client->in.length = 0;
			
			client_free(client);
			client = NULL;
		}
		else {
			// if the client is a node, then // we send a ping.
			if (client->node) {
				push_ping(client);
			}
		}
	}
	else {
		// Make sure we have room in our inbuffer.
		assert((client->in.length + client->in.offset) <= client->in.max);
		avail = client->in.max - client->in.length - client->in.offset;
		if (avail < DEFAULT_BUFSIZE) {
			// we want to increase buffer size, so we'll add another DEFAULT_BUFSIZE to the max.  
			// This should keep it in multiples of DEFAULT_BUFSIZE, regardless of how much is 
			// available for each read.
			
			client->in.buffer = realloc(client->in.buffer, client->in.max + DEFAULT_BUFSIZE);
			client->in.max += DEFAULT_BUFSIZE;
			avail += DEFAULT_BUFSIZE;
		}
		assert(avail >= DEFAULT_BUFSIZE);
		
		// read data from the socket.
		assert(client->in.buffer);
		res = read(fd, client->in.buffer + client->in.offset, avail);
		if (res > 0) {
			
			client->timeout = 0;
			
			stats_bytes_in(res);
			client->in.total += res;
			
			if (log_getlevel() >= LOG_EXTRA) {
				log_data(fd, "IN: ", (unsigned char *)client->in.buffer + client->in.offset, res);
			}

			// got some data.
			assert(res <= avail);
			client->in.length += res;
			
			processed = process_data(client);
			if (processed < 0) {
				// something failed while processing.  We need to close the client connection.
				assert(0);
			}
		}
		else {
			// the connection was closed, or there was an error.
			logger(LOG_ERROR, "socket %d closed. res=%d, errno=%d,'%s'", fd, res, errno, strerror(errno));
			
			// free the client resources.
			if (client->node) {
				// this client is actually a node connection.  We need to create an event to wait 
				// and then try connecting again.
				node_retry(client->node);
			}

			client_free(client);
			client = NULL;
}	}	}




// add a reply to the clients outgoing buffer.  If a 'write' event isnt 
// already set, then set one so that it can begin sending out the data.
void client_send_message(client_t *client, header_t *header, short command, int length, void *payload)
{
	raw_header_t raw;
	char *ptr;
	
	assert(client);
	assert(command > 0);
	assert((length == 0 && payload == NULL) || (length > 0 && payload));
	
	assert(sizeof(raw_header_t) == HEADER_SIZE);

	// build the raw header.
	raw.command = htobe16(command);
	if (header) {
		raw.repcmd = htobe16(header->command);
		raw.userid = htobe32(header->userid);
	}
	else {
		// this is a command, not a reply, so we need to give it a new unique id.
		assert(client->nextid >= 0);
		raw.repcmd = 0;
		raw.userid = htobe32(client->nextid);
		client->nextid++;
		if (client->nextid < 0) client->nextid = 0;
	}
	raw.length = htobe32(length);
	
	// make sure the clients out_buffer is big enough for the message.
	while (client->out.max < client->out.length + client->out.offset + sizeof(raw_header_t) + length) {
		client->out.buffer = realloc(client->out.buffer, client->out.max + DEFAULT_BUFSIZE);
		client->out.max += DEFAULT_BUFSIZE;
	}
	assert(client->out.buffer);
	
	// add the header and the payload to the clients out_buffer, a
	ptr = (client->out.buffer + client->out.offset + client->out.length);
	
	memcpy(ptr, &raw, sizeof(raw_header_t));
	ptr += sizeof(raw_header_t);
	memcpy(ptr, payload, length);
	client->out.length += (sizeof(raw_header_t) + length);
	
	// if the clients write-event is not set, then set it.
	assert(client->out.length > 0);
	if (client->write_event == NULL) {
		assert(_evbase);
		assert(client->handle > 0);
		client->write_event = event_new( _evbase, client->handle, EV_WRITE | EV_PERSIST, write_handler, (void *)client); 
		assert(client->write_event);
		event_add(client->write_event, NULL);
	}
}



//-----------------------------------------------------------------------------
// when the write event fires, we will try to write everything to the socket.
// If everything has been sent, then we will remove the write_event, and the
// out_buffer.
static void write_handler(int fd, short int flags, void *arg)
{
	client_t *client;
	int res;
	
	assert(fd > 0);
	assert(arg);

	client = arg;

	// PERF: if a performance issue is found with sending large chunks of data 
	//       that dont fit in single send, we might be wasting time by purging 
	//       sent data from the buffer which results in moving data in memory.
	
	assert(client->write_event);
	assert(client->out.buffer);
	assert(client->out.length > 0);
	assert( ( client->out.offset + client->out.length ) <= client->out.max);
	
	assert(client->handle > 0);
	
	res = send(client->handle, client->out.buffer + client->out.offset, client->out.length, 0);
	if (res > 0) {
		
		stats_bytes_out(res);
		client->out.total += res;
		
		if (log_getlevel() >= LOG_EXTRA) {
			log_data(client->handle, "OUT: ", (unsigned char *)client->out.buffer + client->out.offset, res);
		}
		
		assert(res <= client->out.length);
		if (res == client->out.length) {
			client->out.offset = 0;
			client->out.length = 0;
		}
		else {
			client->out.offset += res;
			client->out.length -= res;
			
			if (client->out.length == 0) {
				client->out.offset = 0;
			}
		}
		
		assert(client->out.length >= 0);
		if (client->out.length == 0) {
			// all data has been sent, so we clear the write event.
			assert(client->write_event);
			event_free(client->write_event);
			client->write_event = NULL;
			assert(client->read_event);
		}
	}
	else if (res == 0 || (errno != EAGAIN && errno != EWOULDBLOCK)) {
		// the connection has closed, so we need to clean up.
		client_free(client);
		client = NULL;
	}

	
	if (client) {
		if (client->closing > 0 && client->write_event == NULL) {
			// we can now close the connection because we have sent everything.
			logger(LOG_INFO, "Closing connection to client [%d]", client->handle);
			client_free(client);
			client = NULL;
		}
	}
}





// This function will return a pointer to the internal data.  It will also 
// update the length variable to indicate the length of the string.  It will 
// increase the 'next' to point to the next potential field in the payload.  
// If there is no more data in the payload, you will need to check that yourself
char * data_string(char **data, int *length)
{
	char *str;
	int *ptr;
	
	assert(data);
	assert(*data);
	assert(length);
	
	ptr = (void*) *data;
	*length = be32toh(ptr[0]);
	str = *data + sizeof(int);
	*data += (sizeof(int) + *length);

	return(str);
}


char * data_string_copy(char **data) 
{
	char *s;
	char *sal;
	int length = 0;
	
	assert(data);
	assert(*data);
	
	s = data_string(data, &length);
	assert(s);
	assert(length >= 0);
	
	if (length == 0) return NULL;
	else {
		sal = malloc(length + 1);
		memcpy(sal, s, length);
		sal[length] = 0;
		return (sal);
	}
}


// This function will return a pointer to the internal data.  It will also update the length 
// variable to indicate the length of the string.  It will increase the 'next' to point to the next 
// potential field in the payload.  If there is no more data in the payload, you will need to check 
// that yourself
int data_int(char **data)
{
	int *ptr;
	int value;
	
	assert(data);
	assert(*data);
	
	ptr = (void*) *data;
	value = be32toh(ptr[0]);

	*data += sizeof(int);
	
	return(value);
}




// This function will return a pointer to the internal data.  It will also update the length 
// variable to indicate the length of the string.  It will increase the 'next' to point to the next 
// potential field in the payload.  If there is no more data in the payload, you will need to check 
// that yourself
long long data_long(char **data)
{
	long long *ptr;
	long long value;
	
	assert(data);
	assert(*data);
	assert(sizeof(long long) == 8);
	
	ptr = (void*) *data;
	value = be64toh(ptr[0]);

	*data += sizeof(long long);
	
	return(value);
}





void client_attach_node(client_t *client, void *node, int fd)
{
	assert(client);
	assert(node);
	assert(fd >= 0);

	assert(client->node == NULL);
	assert(client->handle < 0);
	
	client->node = node;
	client->handle = fd;
			
	assert(_evbase);
	assert(client->handle > 0);
	assert(client->read_event == NULL);
	client->read_event = event_new( _evbase, fd, EV_READ|EV_PERSIST, read_handler, client);
	assert(client->read_event);
	int s = event_add(client->read_event, &_timeout_client);
	assert(s == 0);
}









static void client_shutdown_handler(evutil_socket_t fd, short what, void *arg) 
{
	client_t *client = arg;
	
	assert(fd == -1);
	assert(arg);

	// check to see if this is a node connection, and there are still buckets, reset the timeout for one_second.
	assert(client);
	if (client->node) {
		if (_primary_buckets > 0 || _secondary_buckets > 0) {
			// this is a node client, and there are still buckets, so we need to keep the connection open.
			assert(client->shutdown_event);
			evtimer_add(client->shutdown_event, &_timeout_shutdown);
		}
		else {
			// this is a node connection, but we dont have any buckets left, so we can close it now.
			event_free(client->shutdown_event);
			client->shutdown_event = NULL;
			client_free(client);
		}
	}
	else {
		// client is not a node, we can shut it down straight away, if there isn't pending data going out to it.
		if (client->in.length == 0 && client->out.length == 0) {
			event_free(client->shutdown_event);
			client->shutdown_event = NULL;
			client_free(client);
		} 
		else {
			printf("waiting to flush data to client:%d\n", client->handle);
			assert(client->shutdown_event);
			evtimer_add(client->shutdown_event, &_timeout_shutdown);
		}
	}
}



// if the client is already shutting down, then do nothing, otherwise we need to initiate the shutdown handler.
void client_shutdown(client_t *client)
{
	assert(client);
	
	if (client->shutdown_event == NULL) {
		assert(_evbase);
		client->shutdown_event = evtimer_new(_evbase, client_shutdown_handler, client);
		assert(client->shutdown_event);
		evtimer_add(client->shutdown_event, &_timeout_now);
	}
}



static void client_dump(client_t *client)
{
	assert(client);
	
	stat_dumpstr("    [%d] Node=%s, Data Received=%ld, Data Sent=%ld", 
				 client->handle,
				 client->node ? "yes" : "no",
				 client->in.total,
				 client->out.total
				);
}


void clients_dump(void)
{
	int i;
	
	stat_dumpstr("CLIENTS");
	
	if (_client_count > 0) {
		stat_dumpstr(NULL);
		stat_dumpstr("  Client List:");
		
		for (i=0; i<_client_count; i++) {
			if (_clients[i]) {
				client_dump(_clients[i]);
			}
		}
	}
	stat_dumpstr(NULL);
}


// mark the client to indicate that the socket needs to be closed as soon as all outgoing data has 
// been sent.  Since we will be sending an ACK back to the client, then we cant close now, we must 
// wait until the data has been sent.
void client_closing(client_t *client)
{
	assert(client);
	assert(client->closing == 0);

	client->closing = 1;
}
