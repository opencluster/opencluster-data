// client.c

#define __CLIENT_C
#include "client.h"
#undef __CLIENT_C


#include "commands.h"
#include "constants.h"
#include "header.h"
#include "logging.h"
#include "messages.h"
#include "node.h"
#include "process.h"
#include "protocol.h"
#include "push.h"
#include "server.h"
#include "stats.h"
#include "timeout.h"

#include <assert.h>
#include <ctype.h>
#include <endian.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>



typedef struct {
	int code;
	void *fn;
} handler_info_t;


typedef struct {
	int cmd;
	int max;
	handler_info_t *handlers;
} command_handlers_t;


static client_t **_clients = NULL;
static int _client_count = 0;


// char *_connectinfo = NULL;


static void read_handler(int fd, short int flags, void *arg);
static void write_handler(int fd, short int flags, void *arg);


static command_handlers_t **_commands = NULL;
static int _command_max = 0;

static handler_info_t *_specials = NULL;
static int _special_max = 0;

// we will keep track of the evbase clients will be using here, rather than using an extern 
// variable.  Gives more flexibility if we want to have seperate evbases.
static struct event_base *_evbase = NULL;






void client_init_commands(int max)
{
	assert(_commands == NULL);
	assert(_command_max == 0);
	assert(max > 0);
	
	_commands = calloc(max, sizeof(command_handlers_t *));
	_command_max = max;
	
	cmd_init();
	process_init();
}

void client_add_cmd(int cmd, void *fn)
{
	int new_max;
	command_handlers_t *handler;
	
	assert(cmd > 0);
	assert(fn);
	
	assert(_commands);
	assert(_command_max > 0);
	
	if (cmd >= _command_max) {
		new_max = cmd + 512;
		_commands = realloc(_commands, sizeof(command_handlers_t *) * new_max);
		for ( ; _command_max < new_max; _command_max++) {
			_commands[_command_max] = NULL;
		}
	}
	
	
	handler = calloc(1, sizeof(command_handlers_t));
	handler->cmd = cmd;
	handler->max = 1;	
	handler->handlers = calloc(handler->max, sizeof(handler_info_t));
	assert(handler->handlers);
	handler->handlers[0].code = 0;
	handler->handlers[0].fn = fn;
	
	assert(cmd < _command_max);
	assert(_commands[cmd] == NULL);
	_commands[cmd] = handler;
}


void client_add_response(int cmd, int code, void *fn)
{
	int i;
	
	assert(cmd > 0);
	assert(code > 0);
	assert(fn);
	
	assert(_commands);
	assert(_command_max > cmd);
	assert(_commands[cmd]);
	assert(_commands[cmd]->cmd == cmd);
	assert(_commands[cmd]->max > 0);
	assert(_commands[cmd]->handlers);
	assert(_commands[cmd]->handlers[0].code == 0);
	assert(_commands[cmd]->handlers[0].fn);
	
	int found = 0;
	for (i=1; i<_commands[cmd]->max && found == 0; i++) {
		if (_commands[cmd]->handlers[_commands[cmd]->max].code == code) {
			found ++;
		}
	}
	
	if (found == 0) {
		// code was not found already
		_commands[cmd]->handlers = realloc(_commands[cmd]->handlers, ((_commands[cmd]->max+1) * sizeof(handler_info_t)));
		assert(_commands[cmd]->handlers);
		_commands[cmd]->handlers[_commands[cmd]->max].code = code;
		_commands[cmd]->handlers[_commands[cmd]->max].fn = fn;
		_commands[cmd]->max ++;
	}
	else {
		// why adding the same result code more than once?
		assert(0);
	}
}


// add handlers for special responces that might be returned from any command.
void client_add_special(int code, void *fn)
{
	assert(code > 0);
	assert(fn);
	
	assert(_special_max >= 0);
	_specials = realloc(_specials, ((_special_max+1) * sizeof(handler_info_t)));
	_specials[_special_max].code = code;
	_specials[_special_max].fn = fn;
	_special_max ++;
}


void client_cleanup(void)
{
	int i;
	for (i=0; i<_command_max; i++) {
		if (_commands[i]) {
			assert(_commands[i]->handlers);
			free(_commands[i]->handlers);
			_commands[i]->handlers = NULL;
			_commands[i]->max = 0;
			
			free(_commands[i]);
			_commands[i] = NULL;
		}
	}
	
	// there should always be some commands
	assert(_commands);
	free(_commands);
	_commands = NULL;
	_command_max = 0;
	
	// there should always be some special handling.
	assert(_specials);
	free(_specials);
	_specials = NULL;
	_special_max = 0;
	
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
	int s = event_add(client->read_event, &_timeout_accept);
	assert(s == 0);
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
	
	server_conn_closed();
	
	assert(client);
	free(client);
	
	assert(_client_count >= 0);
}




// Process the messages received.  The messages could be new commands, or replies to commands that were sent.
static int process_data(client_t *client) 
{
	int processed = 0;
	int stopped = 0;
	char *ptr;
	header_t header;
	raw_header_t *raw;

	void (*func_cmd)(client_t *client, header_t *header, char *payload);
	void (*func_response)(client_t *client, header_t *header, char *payload, char *data);
	
	assert(sizeof(char) == 1);
	assert(sizeof(short int) == 2);
	assert(sizeof(int) == 4);
	assert(sizeof(long long) == 8);
	
	assert(client);
	assert(client->handle > 0);

	while (stopped == 0) {
			
		assert(client->in.buffer);
		assert((client->in.length + client->in.offset) <= client->in.max);
		
		// if we dont have enough for a header, then we dont have enough to build a message.  Messages are at least that.
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

			header.length  = be32toh(raw->length);
			if ((client->in.length-HEADER_SIZE) < header.length) {
				// we dont have enough data yet.
				stopped = 1;
			}
			else {

				header.command = be16toh(raw->command);
				header.response_code  = be16toh(raw->response_code);
				header.userid  = be32toh(raw->userid);

				logger(LOG_DEBUG, "New telegram: Command=%d, repcmd=%d, userid=%d, length=%d, buffer_length=%d", 
						header.command, header.response_code, 
						header.userid, header.length, client->in.length);
				
				// get a pointer to the payload
				ptr = client->in.buffer + client->in.offset + HEADER_SIZE;
				assert(ptr);

				if (header.command >= _command_max || _commands[header.command] == NULL) {

					if (header.response_code == 0) {
						logger(LOG_ERROR, "Unknown command received: Command=%d, userid=%d, length=%d", header.command, header.userid, header.length);
									client_send_reply(client, &header, RESPONSE_UNKNOWN, 0, NULL);
					}
					else {
						logger(LOG_ERROR, "Unknown reply: Reply=%d, Command=%d, userid=%d, length=%d", 
									header.response_code, header.command, header.userid, header.length);
						
						// if we got a reply we werent expecting, then something bad has happened to the connection and we cant trust it.
						assert(0);
						
					}

					#ifndef NDEBUG
					assert(0);
					#endif							
				}
				else {

					assert(_commands[header.command]);
					assert(_commands[header.command]->max > 0);
					assert(_commands[header.command]->cmd == header.command);
					assert(_commands[header.command]->handlers);

					if (header.response_code == 0) {
						// this is a command, so we simply call the handler.
						
						if (_commands[header.command] == NULL) {
							// this server doesnt understand that command.
							assert(0);
						}
						else {
							assert(_commands[header.command]->handlers[0].code == 0);
							assert(_commands[header.command]->handlers[0].fn);
							
							func_cmd = _commands[header.command]->handlers[0].fn;
							(*func_cmd)(client, &header, ptr);
						}
					}
					else {
					
						// this is a response code.   We need to lookup the UserID for this client to find the stored data for this message.
						void *data = message_get(client, header.userid, header.command);
						
						assert(func_response == NULL);
						int i;
						for (i=0; i<_commands[header.command]->max && func_response==NULL; i++) {
							assert(_commands[header.command]->handlers[i].code >= 0);
							if (_commands[header.command]->handlers[i].code == header.response_code) {
								func_response = _commands[header.command]->handlers[i].fn;
								assert(func_response);
								(*func_response)(client, &header, ptr, data);
							}
						}
						
						if (func_response == NULL) {
							// no function was found, but if there are special responses, such as 'tryelsewhere' and 'unknown'... these can be returned from any command.
							assert(0);
							
							assert(func_response == NULL);
							int i;
							for (i=0; i<_special_max && func_response == NULL; i++) {
								if (_specials[i].code == header.response_code) {
									assert(_specials[i].fn);
									func_response = _specials[i].fn;
									(*func_response)(client, &header, ptr, data);
								}
							}
							
							// we should have handled the specials.  If we still didnt find something to handle it, then our implementation of the protocol is broken.
							assert(func_response);
						}
					}
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
			
			// if we received partial data from the socket before it closed, we need to clear it.
			if (client->in.length > 0) {
				client->in.length = 0;
			}
			
			// if we have data pending to send, we might as well clear that out too, as we cant send it now.
			if (client->out.length > 0) {
				client->out.length = 0;
			}

			client_free(client);
			client = NULL;
		}	
	}
}

static void send_data(client_t *client, raw_header_t *rawheader, int length, void *payload)
{
	char *ptr;
	
	assert(client);
	assert(rawheader);
	assert((length == 0 && payload == NULL) || (length > 0 && payload));

	assert(sizeof(raw_header_t) == HEADER_SIZE);

	// make sure the clients out_buffer is big enough for the message.
	while (client->out.max < client->out.length + client->out.offset + sizeof(raw_header_t) + length) {
		client->out.buffer = realloc(client->out.buffer, client->out.max + DEFAULT_BUFSIZE);
		client->out.max += DEFAULT_BUFSIZE;
	}
	assert(client->out.buffer);
	
	// add the header and the payload to the clients out_buffer, a
	ptr = (client->out.buffer + client->out.offset + client->out.length);
	
	memcpy(ptr, rawheader, sizeof(raw_header_t));
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


void client_send_message(client_t *client, short command, int length, void *payload, void *data)
{
	raw_header_t raw;
	int userid;
	
	assert(client);
	assert(command > 0);
	assert((length == 0 && payload == NULL) || (length > 0 && payload));
	
	// when sending a message, we need to save a copy of the specifics.  we will also take a void ptr that we will keep till the response is received.
	userid = message_set(client, command, data);
	assert(userid >= 0);
	
	raw.command = htobe16(command);
	raw.response_code = 0;
	raw.userid = htobe32(userid);
	raw.length = htobe32(length);

	send_data(client, &raw, length, payload);
}


// add a reply to the clients outgoing buffer.  If a 'write' event isnt 
// already set, then set one so that it can begin sending out the data.
void client_send_reply(client_t *client, header_t *header, short code, int length, void *payload)
{
	raw_header_t raw;
	
	assert(client);
	assert(header);
	assert(code > 0);
	assert((length == 0 && payload == NULL) || (length > 0 && payload));
	
	assert(sizeof(raw_header_t) == HEADER_SIZE);

	// build the raw header.
	raw.command = htobe16(header->command);
	raw.response_code = htobe16(code);
	raw.userid = htobe32(header->userid);
	raw.length = htobe32(length);

	send_data(client, &raw, length, payload);
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

	int message_count = messages_count(client);
	if (message_count > 0) {
		// there are still messages waiting for replies for this client, so we need to keep waiting.
		assert(client->shutdown_event);
		evtimer_add(client->shutdown_event, &_timeout_shutdown);
	}
	else {
		
		
		// check to see if this is a node connection, and there are still buckets, reset the timeout for one_second.
		assert(client);
		if (client->node) {
			if (buckets_get_primary_count() > 0 || buckets_get_secondary_count() > 0) {
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


// push the hashmask update to all the clients that we have connections with.
void client_update_hashmasks(hash_t mask, hash_t hashmask, int level)
{
	int i;
	
	if (_clients) {
		for (i=0; i<_client_count; i++) {
			if (_clients[i]) {
				if (_clients[i]->handle >= 0) {
					// we have a client, that seems to be connected.
					push_hashmask(_clients[i], mask, hashmask, level);
				}
			}
		}
	}
}


int client_count(void)
{
	assert(_client_count >= 0);
	return(_client_count);
}



// setup events to shutdown all the clients.  It will shutdown all the client-only connections, and setup events to shutdown the server-node connections after all the buckets have finished migrating. 
void clients_shutdown(void)
{
	assert(0);
}

void clients_set_evbase(struct event_base *evbase)
{
	assert(_evbase == NULL);
	assert(evbase);
	_evbase = evbase;
}
