// messages.c

#include "messages.h"

#include <assert.h>
#include <stdlib.h>

/*
 * Each client maintains a list of messages that it is waiting for a reply on, often including a 
 * pointer to data that will be needed when the reply is received.
 * 
 * To facilitate fast lookup of a free message that can be used, a 'next' index is kept.  
 * This 'next' will be set when a messages is received and cleared (which makes it available for use).
 * 
 * When that 'next' entry is used, the 'next' is incremented, if there is room at the end of the list, or set to 0.
 * 
 * When searching the list, it will check the 'next' entry and if it is used, then it will add a new entry to the end of the list, and use that.  When a message is cleared, the 'next' pointer will use that one.  
 * 
 * ***** 
 * ** It may be better to have a modified circular list, with the ability to shrink it.  That way 
 * ** the list can momentarily be increased dramatically, and then reduced when the message cycle 
 * ** goes back to normal.
 * ***** 
 * 
 * 
 */


// The messages are kept when a command is sent to another node (either client or server).  The required information is generally kept until a reply is received.   

typedef struct {
	client_t *client;	// NULL indicates this message is empty and can be used.
	short int command;    
	int userid;
	void *data;
} message_t;


message_t *_list = NULL;
int _max = 0;
int _next = -1;





void messages_init(void)
{
	// create at least the first empty message
	_list = calloc(1, sizeof(message_t));
	_max = 1;
	_next = 0;
	assert(_list[_next].client == NULL);
}


void messages_cleanup(void)
{
	assert(_list);
	assert(_max > 0);
	assert(_next >= 0 && _next < _max);
		
	while (_max > 0) {
		_max --;
		assert(_list[_max].client == NULL);
		assert(_list[_max].command == 0);
		assert(_list[_max].userid == _max);
		assert(_list[_max].data == NULL);
	}
	assert(_max == 0);
	_next = -1;
		
	free(_list);
	_list = NULL;
}


// create a message and store the data, will return a userid that will be used to identify this 
// message.  First we will check if the 'next' entry is available, and if it is, use that.  If it 
// isn't then we check the first entry in the list.  If that is not available, then it will create 
// an entry at the end of the list.   If it uses a 'next' or 'first' entry, the new 'next' entry 
// will be the next one in the list.
int message_set(client_t *client, short int command, void *data)
{
	int userid = -1;

	assert(client);
	assert(command > 0);
	
	assert(_list);
	assert(_max > 0);

	// first check if the 'next' one is available.
	assert(userid < 0);
	assert(_next >= 0 && _next < _max);
	if (_next > 0 && _list[_next].client == NULL) {
		// this entry is available, and can be used.
		userid = _next;
	}
	else if (_list[0].client == NULL) {
		// the first entry in the list is empty, so we will start at the begining.
		userid = 0;
		_next = 0;
	}
	else {
		// we didn't find a free entry at 'next' or from at the begining.  So we need to add one to the end.
		_list = realloc(_list, sizeof(message_t) * (_max+1));
		userid = _max;
		_max ++;
		_list[userid].userid = userid;
		_next = -1;	// after the post-processing, this will end up being 0.
	}	
	
	assert(userid >= 0 && userid < _max);
	assert(_list[userid].userid == userid);
	_list[userid].command = command;
	_list[userid].client = client;
	_list[userid].data = data;
	_next ++;
	if (_next >= _max) { _next = 0; }
	
	assert(_next >= 0 && _next < _max);
	assert(userid < _max);
	assert(userid >= 0);
	return(userid);
}





// get the data that was stored for this message.  This can only be done once, the message is available again for use after this call.
void * message_get(client_t *client, int userid, int command)
{
	void *data = NULL;

	assert(client);
	assert(command > 0);
	assert(userid < _max);
	assert(_list);
	assert(_max > 0);
	assert(userid >= 0);			// in debug mode, we should assert on this,  but we also need to check the values for prod mode, because this information comes from a remote connection.
	if (userid >= 0 && userid < _max) {
		
		_next = userid;
		
		assert(_list[userid].userid == userid);
		assert(_list[userid].client == client);
		assert(_list[userid].command == command);
		if (_list[userid].client == client && _list[userid].command == command) {
			data = _list[userid].data;
		
			// mark this message as available to use again.
			_list[userid].client = NULL;
			_list[userid].command = 0;
			_list[userid].data = NULL;
		}
	}
	
	return(data);
}



// return the number of messages in the list for a particular client.  This will mostly be used to 
// determine if it is safe to disconnect a client connection.
int messages_count(client_t *client)
{
	int total = 0;
	int i;
	
	for (i=0; i<_max; i++) {
		if (_list[i].client == client) {
			total ++;
		}
	}
	
	
	return(total);
}



