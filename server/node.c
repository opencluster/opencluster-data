// node.c

#define __NODE_C
#include "node.h"
#undef __NODE_C


#include "event-compat.h"
#include "globals.h"
#include "push.h"
#include "timeout.h"

#include <assert.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>


node_t **_nodes = NULL;
int _node_count = 0;

int _active_nodes = 0;



// pre-declare our handlers where necessary.
static void node_wait_handler(int fd, short int flags, void *arg);




node_t * node_new(const char *name) 
{
	node_t *node;
	
	node = malloc(sizeof(node_t));
	assert(node);
	node->name = strdup(name);
	node->client = NULL;
	node->connect_event = NULL;
	node->loadlevel_event = NULL;
	node->wait_event = NULL;
	node->shutdown_event = NULL;

	node->connect_attempts = 0;
	
	return(node);
}



// we have a node that contains a pointer to the client object.  For whatever reason, we need to 
// clear that.
void node_detach_client(node_t *node)
{

	// if we have a loadlevel event set for this node, then we need to cancel it.
	if (node->loadlevel_event) {
		event_free(node->loadlevel_event);
		node->loadlevel_event = NULL;
	}
	
	node->client = NULL;
	_active_nodes --;
	assert(_active_nodes >= 0);

}


static void node_connect_handler(int fd, short int flags, void *arg)
{
	node_t *node = (node_t *) arg;
	int error;
	
	assert(fd >= 0 && flags != 0 && node);
	if (_verbose) printf("[%d] CONNECT: handle=%d\n", _seconds, fd);

	if (flags & EV_TIMEOUT) {
		// timeout on the connect.  Need to handle that somehow.
		
		if (_verbose) 
			printf("[%d] Timeout connecting to: %s\n", 
				   _seconds, node->name);
		
		assert(0);
	}
	else {

		// remove the connect event
		assert(node->connect_event);
		event_free(node->connect_event);
		node->connect_event = NULL;

		// check to see if we really are connected.
		socklen_t foo = sizeof(error);
		int error;
		getsockopt(fd, SOL_SOCKET, SO_ERROR, &error, &foo);
		if (error == ECONNREFUSED) {

			if (_verbose) printf("Unable to connect to: %s\n", node->name);

			// close the socket that didn't connect.
			close(fd);

			//set the action so that we can attempt to reconnect.
			assert(node->connect_event == NULL);
			assert(node->wait_event == NULL);
			assert(_evbase);
			node->wait_event = evtimer_new(_evbase, node_wait_handler, (void *) node);
			evtimer_add(node->wait_event, &_timeout_node_wait);
		}
		else {
			if (_verbose) printf("Connected to node: %s\n", node->name);
			
			// we've connected to another server.... 
			// TODO: we still dont know if its a valid connection, but we can delay the _settling event.

			assert(node->connect_event == NULL);
			assert(node->wait_event == NULL);
			
			node->client = client_new();
			client_attach_node(node->client, node, fd);
			
			// set an event to start asking for load levels.
			// --- 
			assert(node->loadlevel_event == NULL);
			assert(_evbase);
			node->loadlevel_event = evtimer_new(_evbase, node_loadlevel_handler, (void *) node);
			assert(node->loadlevel_event);
			evtimer_add(node->loadlevel_event, &_timeout_node_loadlevel);

			// should send a SERVERHELLO command to the server we've connected to.
			push_serverhello(node->client);
}	}	}




// if the name is passed it, then it is assumed that this is the first time a connect is attempted.  
// If a name is not supplied (ie, it is NULL), then we simply re-use the existing data in the client 
// object.
static void node_connect(node_t *node) 
{
	int len;
	int sock;
	struct sockaddr saddr;

	// create standard network socket.
	sock = socket(AF_INET,SOCK_STREAM,0);
	assert(sock >= 0);
											
	// Before we attempt to connect, set the socket to non-blocking mode.
	evutil_make_socket_nonblocking(sock);

	assert(node);
	assert(node->name);
	
	// resolve the address.
	len = sizeof(saddr);
	if (evutil_parse_sockaddr_port(node->name, &saddr, &len) != 0) {
		// if we cant parse the socket, then we should probably remove it from the nodes list.
		assert(0);
	}

	// attempt the connect.
	int result = connect(sock, &saddr, sizeof(struct sockaddr));
	assert(result < 0);
	assert(errno == EINPROGRESS);

		
	if (_verbose) printf("attempting to connect to node: %s\n", node->name);
	
	// set the connect event with a timeout.
	assert(node->connect_event == NULL);
	assert(_evbase);
	node->connect_event = event_new(_evbase, sock, EV_WRITE, node_connect_handler, node);
	event_add(node->connect_event, &_timeout_connect);

	assert(node->wait_event == NULL);
	assert(node->connect_event);
}



// the wait handler is used to wait before retrying to connect to a node.  When this event fires, 
// then we attempt to connect again.
static void node_wait_handler(int fd, short int flags, void *arg)
{
	node_t *node = arg;

	assert(fd == -1);
	assert((flags & EV_TIMEOUT) == EV_TIMEOUT);
	assert(arg);
	
	assert(node->name);
	if (_verbose) printf("WAIT: node:'%s'\n", node->name);
	
	assert(node->connect_event == NULL);
	assert(node->wait_event);
	
	event_free(node->wait_event);
	node->wait_event = NULL;

	node_connect(node);
}



// this function must assume that the client object has been destroyed because the connection was 
// lost, and we need to setup a wait event to try and connect again later.
void node_retry(node_t *node)
{
	assert(node);
	assert(_evbase);
	
	node->client = NULL;
	
	assert(node->connect_event == NULL);
	assert(node->wait_event == NULL);
	node->wait_event = evtimer_new(_evbase, node_wait_handler, (void *) node);
	evtimer_add(node->wait_event, &_timeout_node_wait);
}






node_t * node_find(char *name)
{
	int i;
	node_t *node = NULL;
	
	assert(name);
	
	for (i=0; i<_node_count && node == NULL; i++) {
		
		// at this point, every 'node' should have a client object attached.
		if (_nodes[i]) {
			assert(_nodes[i]);
			assert(_nodes[i]->client);
			assert(_nodes[i]->name);
			
			if (strcmp(_nodes[i]->name, name) == 0) {
				node = _nodes[i];
			}
		}		
	}
	
	return(node);
}




node_t * node_add(client_t *client, char *name)
{
	node_t *node = NULL;

	assert(client);
	assert(name);
	
	assert((_nodes == NULL && _node_count == 0) || (_nodes && _node_count >= 0));
	_nodes = realloc(_nodes, sizeof(node_t *) * (_node_count + 1));
	assert(_nodes);
	_nodes[_node_count] = node_new(name);
	_nodes[_node_count]->client = client;
	
	node = _nodes[_node_count];
	
	assert(client->node == NULL);
	client->node = _nodes[_node_count];
	_node_count ++;
		
	_active_nodes ++;
	assert(_active_nodes > 0);

	assert(node);
	return(node);
}




void node_loadlevel_handler(int fd, short int flags, void *arg)
{
	node_t *node = arg;
	
	assert(fd == -1);
	assert((flags & EV_TIMEOUT) == EV_TIMEOUT);
	assert(arg);

	assert(node);
	assert(node->client);
	
	push_loadlevels(node->client);

	// add the timeout event back in.
	assert(node->loadlevel_event);
	assert(_evbase);
	evtimer_add(node->loadlevel_event, &_timeout_node_loadlevel);
}



int node_active_inc(void)
{
	assert(_active_nodes >= 0);
	_active_nodes++;
	assert(_active_nodes > 0);
	return(_active_nodes);
}


int node_active_dec(void)
{
	assert(_active_nodes > 0);
	_active_nodes--;
	assert(_active_nodes >= 0);
	return(_active_nodes);
}

int node_active_count(void)
{
	assert(_active_nodes >= 0);
	return(_active_nodes);
}



// the nodes array should already be initialised, and there should already be an entry for each node 
// in the command-line params, we need to initiate a connection attempt (which will then need to be 
// handled through the event system)
void node_connect_all(void)
{
	int i;
	
	for (i=0; i<_node_count; i++) {
		assert(_nodes);
		assert(_nodes[i]);
		assert(_nodes[i]->name);
		if (_nodes[i]->client == NULL) {
			node_connect(_nodes[i]);
		}
	}
}