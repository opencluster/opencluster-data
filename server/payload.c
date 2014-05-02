// payload.c

#include "payload.h"

#include <arpa/inet.h>
#include <assert.h>
#include <stdlib.h>
#include <string.h>

// we will use a default payload buffer to generate the payload for each message sent.  It is not 
// required, but avoids having to allocate memory each time a message is sent.   
// While single-threaded this is all that is needed.  When using threads, would probably have one 
// of these per thread.
char *_payload = NULL;
int _payload_length = 0; 
int _payload_max = 0;


#ifndef DEFAULT_BUFSIZE
#define DEFAULT_BUFSIZE 2048
#endif


void payload_init(void)
{
	assert(_payload == NULL);
	assert(_payload_length == 0);
	assert(_payload_max == 0);
	
	_payload = malloc(DEFAULT_BUFSIZE);
	_payload_max = DEFAULT_BUFSIZE;
	
	assert(_payload);
}

void payload_free(void)
{
	assert(_payload);
	free(_payload);
	_payload_length = 0;
	_payload_max = 0;
}


void payload_int(int value)
{
	int avail;
	int *ptr;

	assert(_payload_max >= 0 && _payload_length >= 0);
	assert(_payload_length <= _payload_max);
	assert(DEFAULT_BUFSIZE > sizeof(value));
	
	avail = _payload_max - _payload_length;
	if (avail < sizeof(value)) {
		_payload = realloc(_payload, _payload_max + DEFAULT_BUFSIZE);
		assert(_payload);
		_payload_max += DEFAULT_BUFSIZE;
	}
	
	ptr = ((void*) _payload + _payload_length);
	ptr[0] = htobe32(value);

        // we need to assume that an int is 32 bits.
        assert(sizeof(int) == 4); 

	
	_payload_length += sizeof(int);
	
	assert(_payload);
	assert(_payload_length <= _payload_max);
}



void payload_long(long long value)
{
	int avail;
	long long *ptr;

	assert(_payload_max >= 0 && _payload_length >= 0);
	assert(_payload_length <= _payload_max);
	assert(DEFAULT_BUFSIZE > sizeof(value));
	assert(sizeof(value) == 8);
	
	avail = _payload_max - _payload_length;
	if (avail < sizeof(value)) {
		_payload = realloc(_payload, _payload_max + DEFAULT_BUFSIZE);
		assert(_payload);
		_payload_max += DEFAULT_BUFSIZE;
	}
	
	ptr = ((void*) _payload + _payload_length);
	ptr[0] = htobe64(value);
	
	_payload_length += sizeof(value);
	
	assert(_payload);
	assert(_payload_length <= _payload_max);
}




void payload_data(int length, void *data)
{
	int avail;
	int *ptr;
	
	assert(_payload_max >= 0 && _payload_length >= 0);
	assert(_payload_length <= _payload_max);
	
	avail = _payload_max - _payload_length;
	while (avail < (sizeof(int)+length)) {
		_payload = realloc(_payload, _payload_max + DEFAULT_BUFSIZE);
		assert(_payload);
		_payload_max += DEFAULT_BUFSIZE;
		avail += DEFAULT_BUFSIZE;
	}

	// we need to assume that an int is 32 bits.
	assert(sizeof(int) == 4);
	
	// add the length of the string first.
	ptr = ((void*) _payload + _payload_length);
	ptr[0] = htobe32(length);

	if (length > 0) {
		memcpy(_payload + _payload_length + sizeof(int), data, length);
	}
	
	_payload_length += sizeof(int) + length;
	
	assert(_payload);
	assert(_payload_length <= _payload_max);
}



void payload_string(const char *str)
{
	if (str == NULL) {
		payload_data(0, NULL);
	}
	else {
		payload_data(strlen(str), (void*)str);
	}
}


// return the length of the payload.
int payload_length(void)
{
	assert(_payload);
	assert(_payload_length >= 0);
	assert(_payload_max > 0);

	return(_payload_length);
}


void * payload_ptr(void)
{
	assert(_payload);
	assert(_payload_length >= 0);
	assert(_payload_max > 0);

	return(_payload);
}


void payload_clear(void)
{
	assert(_payload);
	assert(_payload_length >= 0);
	assert(_payload_max > 0);

	_payload_length = 0;
}
