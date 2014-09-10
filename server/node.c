// node.c

#include "node.h"

#include "event-compat.h"
#include "logging.h"
#include "push.h"
#include "stats.h"
#include "timeout.h"

#include <assert.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>


typedef enum {
	INIT
} startup_state;

static startup_state _startup = INIT;

static node_t **_nodes = NULL;
static int _node_count = 0;

static int _active_nodes = 0;

static struct event_base *_evbase = NULL;

// pre-declare our handlers where necessary.
static void node_wait_handler(int fd, short int flags, void *arg);
static void node_loadlevel_handler(int fd, short int flags, void *arg);



// get a new empty node object.
static void node_alloc(node_t *node)
{
	int entry = -1;

	assert(node);
	assert(node->state == INITIALIZED);
	
	// go through the nodes list to find an empty slot.
	int i;
	for (i=0; entry < 0 && i<_node_count; i++) {
		if (_nodes[i] == NULL) { 
			entry = i;
		} 
	}
	
	if (entry < 0) {
		// if no empty slots, make a new one.
		assert((_nodes == NULL && _node_count == 0) || (_nodes && _node_count >= 0));
		_nodes = realloc(_nodes, sizeof(node_t *) * (_node_count + 1));
		assert(_nodes);
		entry = _node_count;
		_node_count ++;
	}
	assert(entry >= 0 && entry < _node_count);

	_nodes[entry] = node;
}


static node_t * node_new(conninfo_t *conninfo) 
{
	assert(conninfo);
	
	node_t *node = calloc(1, sizeof(node_t));
	assert(node);
	node->nodehash = 0;

	node->client = NULL;
	node->connect_event = NULL;
	node->loadlevel_event = NULL;
	node->wait_event = NULL;
	node->shutdown_event = NULL;
	node->connect_attempts = 0;
	node->conninfo = conninfo;
	node->state = INITIALIZED;

	// add the node to the list of nodes, so that we can iterate through them when we need to.
	node_alloc(node);

	return(node);
}



node_t * node_new_file(const char *filename)
{
	node_t *node = NULL;
	conninfo_t *conninfo;
	
	assert(filename);
	
	conninfo = conninfo_load(filename);
	if (conninfo) {
		node = node_new(conninfo);
		assert(node);
		assert(node->client == NULL);
		logger(LOG_INFO, "Added new node from file: %s", conninfo_name(conninfo));
	}
	else {
		logger(LOG_ERROR, "Unable to add new node from file: %s", filename);
	}

	assert(node->state == INITIALIZED);
	
	assert((conninfo && node) || (conninfo==NULL && node==NULL));
	return(node);
}


node_t * node_add(client_t *client, char *connect_info)
{
	node_t *node = NULL;

	assert(client);
	assert(connect_info);
	
	// first need to go through the list to find out if thise node is already there.
	node = node_find(connect_info);
	if (node == NULL) {
		
		conninfo_t *conninfo = conninfo_parse(connect_info);
		if (conninfo) {
		
			node = node_new(conninfo);
			assert(node);
			assert(node->client == NULL);
			node->client = client;
			
			assert(client->node == NULL);
			client->node = node;
				
			_active_nodes ++;
			assert(_active_nodes > 0);
			
			logger(LOG_INFO, "Added new node from client: %s", conninfo_name(conninfo));
		}
		else {
			logger(LOG_ERROR, "Unable to add new node from client: %s", connect_info);
		}
	}

	assert(node);
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
	
	assert(node->client);
	node->client = NULL;
	assert(_active_nodes > 0);
	_active_nodes --;
	assert(_active_nodes >= 0);

}



static void node_connect_handler(int fd, short int flags, void *arg)
{
	node_t *node = (node_t *) arg;
	int error;
	
	assert(node->state == CONNECTING);
	
	assert(fd >= 0 && flags != 0 && node);
	logger(LOG_INFO, "CONNECT: handle=%d", fd);

	const char *name = node_name(node);
	assert(name);
	
	if (flags & EV_TIMEOUT) {
		// timeout on the connect.  Need to handle that somehow.
		logger(LOG_WARN, "NODE: Timeout connecting to: %s", name);
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

			logger(LOG_ERROR, "Unable to connect to: %s", name);

			// close the socket that didn't connect.
			close(fd);

			//set the action so that we can attempt to reconnect.
			assert(node->connect_event == NULL);
			assert(node->wait_event == NULL);
			assert(_evbase);
			node->wait_event = evtimer_new(_evbase, node_wait_handler, (void *) node);
			evtimer_add(node->wait_event, &_timeout_node_wait);
			
			assert(node->client == NULL);
			
			node->state = UNKNOWN;
		}
		else {
			logger(LOG_INFO, "Connected to node: %s", name);
			
			// we've connected to another server.... 
			// NOTE: we still dont know if its a valid connection.

			assert(node->connect_event == NULL);
			assert(node->wait_event == NULL);
			
			assert(node->client == NULL);
			node->client = client_new();
			assert(node->client);
			client_attach_node(node->client, node, fd);
			
			// set an event to start asking for load levels.
			assert(node->loadlevel_event == NULL);
			assert(_evbase);
			node->loadlevel_event = evtimer_new(_evbase, node_loadlevel_handler, (void *) node);
			assert(node->loadlevel_event);
			evtimer_add(node->loadlevel_event, &_timeout_node_loadlevel);

			assert(node->state == CONNECTING);
			node->state = AUTHENTICATING;
			
			// should send a SERVERHELLO command to the server we've connected to.
			assert(node->conninfo);
			push_serverhello(node->client, conninfo_str(node->conninfo));
		}	
	}	
}




// if the name is passed it, then it is assumed that this is the first time a connect is attempted.  
// If a name is not supplied (ie, it is NULL), then we simply re-use the existing data in the client 
// object.
static void node_connect(node_t *node) 
{
	int len;
	int sock;
	struct sockaddr saddr;

	assert(node);
	
	assert(node->state == INITIALIZED);
	
	// create standard network socket.
	sock = socket(AF_INET,SOCK_STREAM,0);
	assert(sock >= 0);
											
	// Before we attempt to connect, set the socket to non-blocking mode.
	evutil_make_socket_nonblocking(sock);

	
	// get the remote address from the conninfo stored for the node.   
	// It is possible to return a NULL, but for now we will require a simple remote_addr.
	assert(node);
	assert(node->conninfo);
	const char *remote_addr = conninfo_remoteaddr(node->conninfo);
	assert(remote_addr);
	
	// resolve the address.
	len = sizeof(saddr);
	if (evutil_parse_sockaddr_port(remote_addr, &saddr, &len) != 0) {
		// if we cant parse the socket, then we should probably remove it from the nodes list.
		assert(0);
	}

	// attempt the connect.
	int result = connect(sock, &saddr, sizeof(struct sockaddr));
	assert(result < 0);
	assert(errno == EINPROGRESS);

		
	logger(LOG_INFO, "Attempting to connect to node: %s", node_name(node));
	
	// set the connect event with a timeout.
	assert(node->connect_event == NULL);
	assert(_evbase);
	node->connect_event = event_new(_evbase, sock, EV_WRITE, node_connect_handler, node);
	event_add(node->connect_event, &_timeout_connect);

	assert(node->wait_event == NULL);
	assert(node->connect_event);
	
	node->state = CONNECTING;
}



// the wait handler is used to wait before retrying to connect to a node.  When this event fires, 
// then we attempt to connect again.
static void node_wait_handler(int fd, short int flags, void *arg)
{
	node_t *node = arg;

	assert(fd == -1);
	assert((flags & EV_TIMEOUT) == EV_TIMEOUT);
	assert(arg);
	
	logger(LOG_INFO, "WAIT: node:'%s'", node_name(node));
	
	assert(node->connect_event == NULL);
	assert(node->wait_event);
	
	// reset the node to merely an INITIALIZED state, since it didn't connect.
	node->state = INITIALIZED;
	
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






node_t * node_find(const char *conninfo_str)
{
	int i;
	node_t *node = NULL;
	
	assert(conninfo_str);
	
	for (i=0; i<_node_count && node == NULL; i++) {
		
		// at this point, every 'node' should have a client object attached.
		// ** no it shouldn't.
		if (_nodes[i]) {
			assert(_nodes[i]);
			assert(_nodes[i]->client);
			assert(_nodes[i]->conninfo);
			
			// when conninfo string is loaded it needs to be compressed to remove unnecessary 
			// elements, and then the resulting string needs to be hashed, and that value is what 
			// needs to be compared.
			if (conninfo_compare_str(_nodes[i]->conninfo, conninfo_str) == 0) {
				node = _nodes[i];
			}
		}
	}
	
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



// the nodes array should already be initialised, and there may already be some nodes in the 
// list, from the command-line params, we need to initiate a connection attempt (which will then 
// need to be handled through the event system)
void node_connect_start(void)
{
	int i;
	
	assert(_startup == INIT);
	
	assert(_nodes);
	assert(_node_count >= 0);
	for (i=0; i<_node_count; i++) {
		logger(LOG_DEBUG, "Starting connect for entry: %d/%d", i, _node_count);
		assert(_nodes);
		if (_nodes[i]) {
			logger(LOG_DEBUG, "node exists: %d", i);
			assert(_nodes[i]->conninfo);
			assert(_nodes[i]->client == NULL);
			assert(_nodes[i]->state == INITIALIZED);
			logger(LOG_DEBUG, "Attempting connect: %d", i);
			node_connect(_nodes[i]);
		}
	}
}



static void node_free(node_t *node)
{
	assert(node);
	assert(node->conninfo);
	
	assert(node->client == NULL);
	assert(node->connect_event == NULL);
	assert(node->loadlevel_event == NULL);
	assert(node->wait_event == NULL);
	assert(node->shutdown_event == NULL);

	// remove the node from the list.
	int i;
	for (i=0; i<_node_count; i++) {
		if (_nodes[i] == node) {
			_nodes[i] = NULL;
			break;
		}
	}
	
	if (node->conninfo) {
		conninfo_free(node->conninfo);
		node->conninfo = NULL;
	}
	
	free(node);
}






static void node_shutdown_handler(evutil_socket_t fd, short what, void *arg) 
{
	node_t *node = arg;
	int i;
	
	assert(fd == -1 && arg);
	assert(node);
	
	// if the node is connecting, we have to wait for it to time-out.
	if (node->connect_event) {
		assert(node->shutdown_event);
		evtimer_add(node->shutdown_event, &_timeout_shutdown);
	}
	else {
	
		// if the node is waiting... cancel it.
		if (node->wait_event) {
			assert(0);
		}
		
		// if we can, remove the node from the nodes list.
		if (node->client) {
			// the client is still connected.  We need to wait for it to disconnect.
		}
		else {
			node_free(node);
		}
	}
}



void node_shutdown(node_t *node)
{
	assert(node);
	
	if (node->shutdown_event == NULL) {
		
		assert(_evbase);
		node->shutdown_event = evtimer_new(_evbase, node_shutdown_handler, node);
		assert(node->shutdown_event);
		evtimer_add(node->shutdown_event, &_timeout_now);
	}
}


static void node_dump(int index, node_t *node)
{
	assert(node);

	stat_dumpstr("    [%02d] '%s', Connected=%s, Connect_attempts=%d", 
				 index, 
				 node_name(node), 
				 node->client ? "yes" : "no", 
				 node->connect_attempts);
}


// dump to the stats logger, all the information that we have about the nodes;
void nodes_dump(void)
{
	int i;
	
	stat_dumpstr("NODES");
	stat_dumpstr("  Active Nodes: %d", _active_nodes);
	
	if (_node_count > 0) {
		stat_dumpstr(NULL);
		stat_dumpstr("  Node List:");
		
		for (i=0; i<_node_count; i++) {
			if (_nodes[i]) {
				node_dump(i, _nodes[i]);
			}
		}
	}
	stat_dumpstr(NULL);
}


const char * node_name(node_t *node)
{
	assert(node);
	assert(node->conninfo);
	return(conninfo_name(node->conninfo));
}

// shutdown all the nodes.   Actually, what it will do is fire up an event that will wait until it is safe to shutdown the nodes (ie, all the buckets have been migrated off, or otherwise being discarded.)
void nodes_shutdown(void)
{
	assert(0);
}


void nodes_set_evbase(struct event_base *evbase)
{
	assert(_evbase == NULL);
	assert(evbase);
	_evbase = evbase;
}



int node_count(void)
{
	assert(_node_count >= 0);
	return(_node_count);
}


