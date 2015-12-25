// payload.c

#include "payload.h"

#include <arpa/inet.h>
#include <assert.h>
#include <stdlib.h>
#include <string.h>





// We will keep track of payload objects in two lists.  Active and Available.  The active list is 
// for payloads that are currently in use.   The PAYLOAD handle will refer to payloads in the Active 
// list.  As payloads are released, if they are at the end of the list, the 'count' handle will be 
// reduced until it finds a non-empty slot.   This means there could be gaps in the list.  The count 
// for 'active' will not be a true count, but instead indicates how many entries are used, even 
// though some of them might be blank.
// The Available list is for payload objects that are not currently in use, and are ready to be used 
// again. The _avail_next index will point to the next entry on the list available for use.  There 
// should not be any empty slots in this list, it should be treated as a stack.

payload_t **_active_list = NULL;
int _active_max = 0;
int _active_count = 0;

payload_t **_avail_list = NULL;
int _avail_max = 0;
int _avail_count = 0;

#ifndef DEFAULT_BUFSIZE
#define DEFAULT_BUFSIZE 2048
#endif


void payload_init(void)
{
	// nothing to do here currently.
}   


PAYLOAD payload_new(void *client, int command)
{
	PAYLOAD entry = -1;
	payload_t *payload = NULL; 

	assert(_avail_count <= _avail_max);
	
	if (_avail_count == 0) {
		// we dont have any free payloads, so we need to make one.
		payload = calloc(1, sizeof(payload_t));
		assert(payload->used == 0);
		payload->buffer = malloc(DEFAULT_BUFSIZE);
		payload->max = DEFAULT_BUFSIZE;
		assert(payload->length == 0);
	}
	else {
		// we have some available payloads, so we use the next one in the list.
		assert(_avail_count > 0);
		assert(_avail_max > 0);
		
		_avail_count --;
		assert(_avail_count >= 0);
		
		payload = _avail_list[_avail_count];
		assert(payload);
		assert(payload->used == 0);
		assert(payload->buffer);
		assert(payload->max > 0);
	}
	
	// we now have a payload, so we need to add it to the 'active' list and return the handle to it.
	assert(payload);
	if (_active_count == _active_max) {
		// we dont have any space, we need to add a slot.
		entry = _active_count;
		_active_max ++;
		_active_count ++;
		_active_list = realloc(_active_list, sizeof(payload_t *) * _active_max);
		_active_list[entry] = payload;
	}
	else {
		// there is an empty slot.
		entry = _active_count;
		assert(_active_list[entry] == NULL);
		_active_list[entry] = payload;
		_active_count ++;
		assert(_active_count <= _active_max);
	}
	
	assert(payload->used == 0);
	payload->used ++;
	
	assert(entry >= 0);
	assert(entry <_active_count);
	payload->command = command;
	payload->userid = entry;
	payload->client = client;
	
	return(entry);
}


PAYLOAD payload_new_reply(void)
{
	return(payload_new(NULL, 0));
}


// by the time the payload module is being released, there should not be any active payloads in the 
// system.  
void payload_free(void)
{
	if (_active_list) {
		assert(_active_count == 0);
		assert(_active_max > 0);
		free(_active_list);
		_active_list = NULL;
	}
	
	if (_avail_list) {
		assert(_avail_count > 0);
		assert(_avail_max > 0);
		
		while (_avail_max > 0) {
			_avail_max --;
			assert(_avail_list[_avail_max]->used == 0);
			assert(_avail_list[_avail_max]->length == 0);
			assert(_avail_list[_avail_max]->buffer);
			free(_avail_list[_avail_max]->buffer);
			_avail_list[_avail_max]->buffer = NULL;
			_avail_list[_avail_max]->max = 0;
		}
		free(_avail_list);
		_avail_list = NULL;
		_avail_count = 0;
	}
	
	assert(_active_list == NULL);
	assert(_active_count == 0);
	assert(_active_max == 0);
	assert(_avail_list == NULL);
	assert(_avail_count == 0);
	assert(_avail_max == 0); 
}





void payload_int(PAYLOAD entry, int value)
{
	assert(entry >= 0);
	assert(entry < _active_count);
	assert(_active_count <= _active_max);
	assert(_active_list);
	
	payload_t *payload = _active_list[entry];
	assert(payload);
	assert(payload->used > 0);
	
	assert(payload->max >= 0 && payload->length >= 0);
	assert(payload->length <= payload->max);
	assert(DEFAULT_BUFSIZE > sizeof(value));
	assert(sizeof(value) == 4);
	assert(payload->buffer);
	
	int avail = payload->max - payload->length;
	if (avail < sizeof(value)) {
		payload->buffer = realloc(payload->buffer, payload->max + DEFAULT_BUFSIZE);
		assert(payload->buffer);
		payload->max += DEFAULT_BUFSIZE;
		assert(payload->max > 0);
	}
	
	int *ptr = ((void*) payload->buffer + payload->length);
	ptr[0] = htobe32(value);

	// we need to assume that an int is 32 bits.
	assert(sizeof(int) == 4); 
	
	payload->length += sizeof(int);
	
	assert(payload->buffer);
	assert(payload->length <= payload->max);
}



void payload_long(PAYLOAD entry, long long value)
{
	assert(entry >= 0);
	assert(entry < _active_count);
	assert(_active_count <= _active_max);
	assert(_active_list);
	
	payload_t *payload = _active_list[entry];
	assert(payload);
	assert(payload->used > 0);
	
	assert(payload->max >= 0 && payload->length >= 0);
	assert(payload->length <= payload->max);
	assert(DEFAULT_BUFSIZE > sizeof(value));
	assert(sizeof(value) == 8);
	assert(payload->buffer);
	
	int avail = payload->max - payload->length;
	if (avail < sizeof(value)) {
		payload->buffer = realloc(payload->buffer, payload->max + DEFAULT_BUFSIZE);
		assert(payload->buffer);
		payload->max += DEFAULT_BUFSIZE;
		assert(payload->max > 0);
	}
	
	long long *ptr = ((void*) payload->buffer + payload->length);
	ptr[0] = htobe64(value);
	
	payload->length += sizeof(value);
	
	assert(payload->buffer);
	assert(payload->length <= payload->max);
}




void payload_data(PAYLOAD entry, int length, void *data)
{
	assert(entry >= 0);
	assert(entry < _active_count);
	assert(_active_count <= _active_max);
	assert(_active_list);
	
	payload_t *payload = _active_list[entry];
	assert(payload);
	assert(payload->used > 0);
	
	assert(payload->max >= 0 && payload->length >= 0);
	assert(payload->length <= payload->max);
	assert(payload->buffer);
	
	int avail = payload->max - payload->length;
	if (avail < (DEFAULT_BUFSIZE + length)) {
		payload->max += (DEFAULT_BUFSIZE + length);
		payload->buffer = realloc(payload->buffer, payload->max);
		assert(payload->buffer);
		assert(payload->max > 0);
	}

	// we need to assume that an int is 32 bits.
	assert(sizeof(int) == 4);
	assert(sizeof(length) == 4);
	
	// add the length of the string first.
	char *ptr = ((char*) payload->buffer + payload->length);
	ptr[0] = htobe32(length);

	if (length > 0) {
		memcpy(payload->buffer + payload->length + sizeof(int), data, length);
	}
	
	payload->length += (sizeof(int) + length);
	
	assert(payload->buffer);
	assert(payload->length <= payload->max);
}



void payload_string(PAYLOAD entry, const char *str)
{
	if (str == NULL) {
		payload_data(entry, 0, NULL);
	}
	else {
		payload_data(entry, strlen(str), (void*)str);
	}
}


payload_t * payload_get(PAYLOAD entry)
{
	payload_t *payload = NULL;
	
	assert(entry >= 0);
	assert(entry < _active_count);
	
	if (entry >= 0 && entry < _active_count) {
		payload = _active_list[entry];
		assert(payload);
		if (payload) {
			assert(payload->used > 0);
			
			assert(payload->buffer);
			assert(payload->length >= 0);
			assert(payload->max > 0);
		}
	}

	return(payload);
}

payload_t * payload_get_verify(PAYLOAD entry, int command, void *client)
{
	payload_t *payload = payload_get(entry);
	assert(payload);

	if (payload->command != command || payload->client != client) {
		payload = NULL;
	}
	
	return(payload);
}


// The payload has been completed, and is not needed.  This function will simply reduce the 'used' 
// count, and if it reached zero, then put the payload into the 'avail' list.
void payload_release(PAYLOAD entry)
{
	assert(entry >= 0);
	assert(entry < _active_count);
	
	payload_t *payload = _active_list[entry];
	payload->used --;
	assert(payload->used >= 0);

	if (payload->used == 0) {
		// the payload is not needed anymore, so we can put it in the 'avail' list.
		
		assert(_avail_count <= _avail_max);
		if (_avail_count == _avail_max) {
			_avail_max ++;
			_avail_list = realloc(_avail_list, sizeof(payload_t *) * _avail_max);
			_avail_list[_avail_count] = payload;
			_avail_count ++;
		}

		// if this entry was at the end of the list, then move the 'count' to the previous one.
		_active_list[entry] = NULL;
		if (entry > 0 && entry == _active_count) {
			_active_count --;
		}
		assert(_active_count >= 0);
		assert(_active_count <= _active_max);
		
		// reset the contents of the payload ready to be used again.
		payload->userid = NO_PAYLOAD;
		payload->command = 0;
		payload->length = 0;
		payload->client = NULL;
		payload->sent = 0;
	}
}


// returns the number of active 
int payload_client_count(void *client)
{
	int count = 0;
	
	assert(client);
	
	assert(_active_max >= 0);
	assert(_active_count >= 0);
	assert(_active_count <= _active_max);
	assert(_active_list);
	
	int i;
	payload_t *payload;
	for (i=0; i<_active_count; i++) {
		payload = _active_list[i];
		if (payload) {
			assert(payload->used > 0);
			if (payload->client == client) {
				count ++;
			}
		}
	}
	
	return(count);
}

