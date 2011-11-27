//-----------------------------------------------------------------------------
// ocd - Open Cluster Daemon
//	Enhanced hash-map storage cluster.
//-----------------------------------------------------------------------------


#include "event-compat.h"

// includes

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <pwd.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <glib.h>



#define PACKAGE 						"ocd"
#define VERSION 						"0.10"


#define DEFAULT_BUFSIZE 4096

#ifndef INVALID_HANDLE
#define INVALID_HANDLE -1
#endif


#define HEADER_SIZE 12

#define STARTING_MASK  0x0F


// commands and replies
#define REPLY_ACK        1
#define REPLY_UNKNOWN    9
#define CMD_HELLO        10
#define CMD_SHUTTINGDOWN 15
#define CMD_PING         30
#define CMD_SERVERHELLO  50
#define CMD_SERVERLIST   100
#define CMD_HASHMASKS    110
#define CMD_HASHMASK     120
#define CMD_SET_INT      2000
#define CMD_GET_INT      2100
#define REPLY_DATA_INT   2105


#define CLIENT_TIMEOUT_LIMIT	6


//-----------------------------------------------------------------------------
// common structures.


typedef int hash_t;


typedef struct {
	struct evconnlistener *listener;
} server_t;



typedef struct {
	void *node;	// node_t;
	
	evutil_socket_t handle;
	struct event *read_event;
	struct event *write_event;
	struct event *shutdown_event;

	struct {
		char *buffer;
		int offset;
		int length;
		int max;
	} in, out;
	
	int nextid;

	int timeout_limit;
	int timeout;
	int tries;
	
	int closing;
} client_t;



#define VALUE_SHORT  1
#define VALUE_INT    2
#define VALUE_LONG   3
#define VALUE_STRING 4
typedef struct {
	int type;
	union {
		int i;			// integer
		long l;			// long
		struct {
			int length;
			char *data;
		} s;			// string
	} data;
} value_t;


typedef struct {
	short command;
	short repcmd;
	int userid;
	int length;
} header_t;

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


typedef struct {
	char *name;
	client_t *client;
	struct event *connect_event;
	struct event *loadlevel_event;
	struct event *wait_event;
	struct event *shutdown_event;
	int connect_attempts;
} node_t;


typedef struct {

	hash_t hash;
	
	//  0 indicates primary
	//  1 or more indicate which level of indirection the backups go.
	int level;	
	
	// NULL if not hosted here.
	GTree *tree;
	
	// if this bucket is not hosted here, then this is the server that it is hosted.
	// NULL if it is hosted here.
	char *target_host;
	char *backup_host;
	char *logging_host;
	
	node_t *target_node;
	node_t *backup_node;
	node_t *logging_node;

	struct event *shutdown_event;
	struct event *transfer_event;

} bucket_t;




typedef struct {
	GTree *mapstree;
	hash_t key;
} maplist_t;


typedef struct {
	hash_t key;
	hash_t map_key;
	int expires;
	char *name;
	value_t *value;
} item_t;


//-----------------------------------------------------------------------------
// Global Variables.    
// If we ever add threads then we need to be careful when accessing these 
// global variables.


// the mask is used to determine which bucket a hash belongs to.
unsigned int _mask = 0;

// the list of buckets that this server is handling.  '_mask' indicates how many entries in the array there is, but 
// the ones that are not handled by this server, will have a NULL entry.
bucket_t ** _buckets = NULL;

// we will keep track of the number of buckets we are actually supporting.  This is so that we dont 
// have to go through the list to determine it.
int _bucket_count = 0;


// event base for listening on the sockets, and timeout events.
struct event_base *_evbase = NULL;

// list of listening sockets (servers). 
server_t *_server = NULL;

client_t **_clients = NULL;
int _client_count;

node_t **_nodes = NULL;
int _node_count = 0;

int _active_nodes = 0;



// we will use a default payload buffer to generate the payload for each message sent.  It is not 
// required, but avoids having to allocate memory each time a message is sent.   
// While single-threaded this is all that is needed.  When using threads, would probably have one 
// of these per thread.
char *_payload = NULL;
int _payload_length = 0; 
int _payload_max = 0;


// number of connections that we are currently listening for activity from.
int _conncount = 0;

// startup settings.
const char *_interface = "127.0.0.1:13600";
int _maxconns = 1024;
int _verbose = 0;
int _daemonize = 0;
const char *_username = NULL;
const char *_pid_file = NULL;


// signal catchers that are used to clean up, and store final data before 
// shutting down.
struct event *_sigint_event = NULL;
struct event *_sighup_event = NULL;

// When the service first starts up, it attempts to connect to another node in the cluster, but it 
// doesn't know if it is the first member of the cluster or not.  So it wait a few seconds and then 
// create all the buckets.  This event is a one-time timout to trigger this process.  If it does 
// connect to another node, then this event will end up doing nothing.
struct event *_settle_event = NULL;

// The 'seconds' event fires several times a second checking the current time.  When the time ticks 
// over, it updates the internal _current_time variable which is used for tracking expiries and 
// lifetime of stored data.
struct event *_seconds_event = NULL;
struct timeval _current_time = {0,0};
struct timeval _start_time = {0,0};

// The stats event fires every second, and it collates the stats it has and logs some statistics 
// (if there were any).
struct event *_stats_event = NULL;
long _stat_counter = 0;

// This event is started when the system is shutting down, and monitors the events that are left to 
// finish up.  When everything is stopped, it stops the final events that have been ticking over 
// (like the seconds and stats events), which will allow the event-loop to exit.
struct event *_shutdown_event = NULL;

// Standard timeout values for the various events.  Note that common-timeouts should probably be 
// used instead which can increase performance when there are a lot of events that have the same 
// timeout value.
struct timeval _now_timeout = {0,0};
struct timeval _standard_timeout = {5,0};
struct timeval _shutdown_timeout = {0,500000};
struct timeval _settle_timeout = {.tv_sec = 5, .tv_usec = 0};
struct timeval _seconds_timeout = {.tv_sec = 0, .tv_usec = 100000};
struct timeval _stats_timeout = {.tv_sec = 1, .tv_usec = 0};
struct timeval _loadlevel_timeout = {.tv_sec = 30, .tv_usec = 0};
struct timeval _connect_timeout = {.tv_sec = 30, .tv_usec = 0};
struct timeval _node_wait_timeout = {.tv_sec = 5, .tv_usec = 0};

// This will be 0 when the node has either connected to other nodes in the cluster, or it has 
// assumed that it is the first node in the cluster.
int _settling = 1;


// _shutdown will indicate the state of the service.  Under startup conditions it will be -1.  
// When the system has been started up, it will be set to 0.  But you need to be aware of the 
// _settling variable to indicate if it has either joined an existing cluster, or decided it is the 
// first member of one.  When the system is attempting to shutdown, _shutdown will be set to >0 
// (which will indicate how many subsystems it knows are remaining).  When it has finally determined 
// that everything is shutdown, then it will be 0, and the final event will be stopped.
// NOTE: When determining how many things need to be closed, it is important to get this right, or 
// the system will stop before it has really completed.
int _shutdown = -1;




//-----------------------------------------------------------------------------
// Pre-declare our handlers, because often they need to interact with functions
// that are used to invoke them.
static void accept_conn_cb(struct evconnlistener *listener, evutil_socket_t fd, struct sockaddr *address, int socklen, void *ctx);
static void read_handler(int fd, short int flags, void *arg);
static void write_handler(int fd, short int flags, void *arg);
static void node_wait_handler(int fd, short int flags, void *arg);
static void node_connect_handler(int fd, short int flags, void *arg);




//-----------------------------------------------------------------------------
// Listen for socket connections on a particular interface.
static void server_listen(void)
{
	struct sockaddr_in sin;
	int len;
	
	assert(_server == NULL);
	assert(_interface);
	assert(_maxconns > 0);
	assert(_conncount == 0);
	
	_server = calloc(1, sizeof(*_server));
	assert(_server);

	_server->listener = NULL;

	memset(&sin, 0, sizeof(sin));
	// 	sin.sin_family = AF_INET;
	len = sizeof(sin);
	
	assert(sizeof(struct sockaddr_in) == sizeof(struct sockaddr));
	if (evutil_parse_sockaddr_port(_interface, (struct sockaddr *)&sin, &len) != 0) {
		assert(0);
	}
	else {
		
		assert(_server->listener == NULL);
		assert(_evbase);
		
		if (_verbose) printf("listen: %s\n", _interface);

		_server->listener = evconnlistener_new_bind(
								_evbase,
								accept_conn_cb,
								_server,
								LEV_OPT_CLOSE_ON_FREE|LEV_OPT_REUSEABLE,
								-1,
								(struct sockaddr*)&sin,
								sizeof(sin)
							);
		assert(_server->listener);
}	}



static client_t * client_new(void)
{
	client_t *client;
	
	client = calloc(1, sizeof(client_t));
	assert(client);
	
	client->handle = -1;
	
	client->read_event = NULL;
	client->write_event = NULL;
	client->shutdown_event = NULL;
	
	client->out.buffer = NULL;
	client->out.offset = 0;
	client->out.length = 0;
	client->out.max = 0;
	
	client->in.buffer = NULL;
	client->in.offset = 0;
	client->in.length = 0;
	client->in.max = 0;
	
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
	
	assert(client);
	return(client);
}



//-----------------------------------------------------------------------------
// Initialise the client structure.
// If 'server' is NULL, then this is probably a connection to another NODE in the cluster.
static void client_accept(client_t *client, evutil_socket_t handle, struct sockaddr *address, int socklen)
{
	assert(client);

	assert(handle > 0);
	assert(client->handle < 0);
	client->handle = handle;
	
	if (_verbose) printf("[%d] New client - handle=%d\n", (int)(_current_time.tv_sec-_start_time.tv_sec), handle);

	assert(_evbase);
	assert(client->handle > 0);
	client->read_event = event_new( _evbase, client->handle, EV_READ|EV_PERSIST, read_handler, client);
	assert(client->read_event);
	int s = event_add(client->read_event, &_standard_timeout);
	assert(s == 0);
	
	_conncount ++;
}


//-----------------------------------------------------------------------------
// accept an http connection.  Create the client object, and then attach the
// file handle to it.
static void accept_conn_cb(
	struct evconnlistener *listener,
	evutil_socket_t fd,
	struct sockaddr *address,
	int socklen,
	void *ctx)
{
	client_t *client;
	server_t *server = (server_t *) ctx;

	assert(listener);
	assert(fd > 0);
	assert(address && socklen > 0);
	assert(ctx);
	assert(server);

	// create client object.
	// TODO: We should be pulling these client objects out of a mempool.
	client = client_new();
	client_accept(client, fd, address, socklen);
}


//-----------------------------------------------------------------------------
// Free the resources used by the client object.
static void client_free(client_t *client)
{
	char found=0, resize=0;
	int i;
	node_t *node;
	
	assert(client);
	
	if (_verbose >= 2) printf("[%d] client_free: handle=%d\n", (int)(_current_time.tv_sec-_start_time.tv_sec), client->handle);

	if (client->node) {
		node = client->node;
		node->client = NULL;
		_active_nodes --;
		assert(_active_nodes >= 0);
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
	
	printf("[%d] found:%d, client_count:%d\n", (int)(_current_time.tv_sec-_start_time.tv_sec), found, _client_count);
	
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





static void server_shutdown()
{
	assert(_server);

	// need to close the listener socket.
	if (_server->listener) {
		evconnlistener_free(_server->listener);
		_server->listener = NULL;
		printf("Stopping listening on: %s\n", _interface);
	}
	
	free(_server);
	_server = NULL;
}






// add a reply to the clients outgoing buffer.  If a 'write' event isnt 
// already set, then set one so that it can begin sending out the data.
static void send_message(client_t *client, header_t *header, short command, int length, void *payload)
{
	raw_header_t raw;
	char *ptr;
	
	assert(client);
	assert(command > 0);
	assert((length == 0 && payload == NULL) || (length > 0 && payload));
	
	assert(sizeof(raw_header_t) == HEADER_SIZE);

	// build the raw header.
	raw.command = htons(command);
	if (header) {
		raw.repcmd = htons(header->command);
		raw.userid = htonl(header->userid);
	}
	else {
		// this is a command, not a reply, so we need to give it a new unique id.
		assert(client->nextid >= 0);
		raw.repcmd = 0;
		raw.userid = htonl(client->nextid);
		client->nextid++;
		if (client->nextid < 0) client->nextid = 0;
	}
	raw.length = htonl(length);
	
	
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



static void push_shuttingdown(client_t *client)
{
	assert(client->handle > 0);
	send_message(client, NULL, CMD_SHUTTINGDOWN, 0, NULL);
}


gboolean traverse_hash_fn(gpointer key, gpointer value, void *data)
{
	// data points to the variable that we will be using.
	hash_t **aa = data;

	aa[0] = value;
	return(TRUE);
}

// delete the contents of the bucket.  Note, that the bucket becomes empty, but the bucket itself is not destroyed.
static void bucket_destroy(bucket_t *bucket)
{
	maplist_t *maplist;
	int total, total_items;
	int i, j;
	item_t *item;
	
	assert(bucket);
	
	// while going through the tree, cannot modify the tree, so we will continuously go through the 
	// list, getting an item one at a time, clearing it and removing it from the tree.  Continue the 
	// process until there is no more items in the tree.
	assert(bucket->tree);
	total = g_tree_nnodes(bucket->tree);
	assert(total >= 0);
	
	for (i=0; i<total; i++) {
		maplist = NULL;
		g_tree_foreach(bucket->tree, traverse_hash_fn, &maplist);
		assert(maplist);
		
		assert(maplist->mapstree);
		
		total_items = g_tree_nnodes(maplist->mapstree);
		assert(total_items >= 0);
		
		for (j=0; j<total_items; j++) {
			item = NULL;
			g_tree_foreach(maplist->mapstree, traverse_hash_fn, &item);
			assert(item);
			
			assert(item->key == maplist->key);
			
			assert(item->name);
			free(item->name);
			item->name = NULL;

			assert(item->value);
			if (item->value->type == VALUE_STRING) {
				assert(item->value->data.s.data);
				free(item->value->data.s.data);
				item->value->data.s.data = NULL;
			}
			free(item->value);
			item->value = NULL;

			// now remove hte reference from the binary tree.
			gboolean found = g_tree_remove(maplist->mapstree, &item->map_key);
			assert(found == TRUE);
			
			free(item);
		}
		
		total_items = g_tree_nnodes(maplist->mapstree);
		assert(total_items == 0);
		
		g_tree_destroy(maplist->mapstree);
		maplist->mapstree = NULL;
		
		g_tree_remove(bucket->tree, &maplist->key);
		free(maplist);
	}
	
	// the tree should be empty now.
	total = g_tree_nnodes(bucket->tree);
	assert(total == 0);
	
	g_tree_destroy(bucket->tree);
	bucket->tree = NULL;
}




//-----------------------------------------------------------------------------
static void bucket_transfer_handler(evutil_socket_t fd, short what, void *arg)
{
	int waiting = 0;
	bucket_t *bucket = arg;
	
	assert(fd == -1);
	assert(what & EV_TIMEOUT);
	assert(bucket);

	if (_verbose) printf("Bucket transfer handler\n");

	// check the state of the bucket.  If it is a backup bucket, then we can deleete it.
	if (bucket->level > 0) {
		bucket_destroy(bucket);
	}
	
	// if it is a primary bucket, and does not have any backup buckets, and there are no nodes, then delete the bucket.
	assert(0);
	
	if (waiting > 0) {
		if (_verbose) printf("WAITING FOR BUCKET (%d).\n", bucket->hash);
		evtimer_add(_shutdown_event, &_seconds_timeout);
	}
	else {
		// the bucket is done.
		assert(bucket->transfer_event);
		event_free(bucket->transfer_event);
		bucket->transfer_event = NULL;
		
		
	}
}


static void client_shutdown_handler(evutil_socket_t fd, short what, void *arg) 
{
	client_t *client = arg;
	
	assert(fd == -1);
	assert(arg);

	// check to see if this is a node connection, and there are still buckets, reset the timeout for one_second.
	assert(client);
	if (client->node) {
		if (_bucket_count > 0) {
			// this is a node client, and there are still buckets, so we need to keep the connection open.
			assert(client->shutdown_event);
			evtimer_add(client->shutdown_event, &_shutdown_timeout);
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
			evtimer_add(client->shutdown_event, &_shutdown_timeout);
		}
	}
}


// I assume that this is supposed to push out the details of an adjusted hash to all the clients 
// (and therefore nodes).
static void all_push_hashmask(hash_t hash)
{
	int i;
	
	assert(_mask > 0);

	for (i=0; i<_client_count; i++) {
		if (_clients[i]) {
			assert(0);
		}
	}
}



static void push_promote(client_t *client, hash_t hash)
{
	assert(client);
	
	// send a message to the client, telling it that it is now the primary node for the specified bucket hash.
	assert(0);
	
}

static void node_free(node_t *node)
{
	assert(node);
	assert(node->name);
	
	assert(node->client == NULL);
	assert(node->connect_event == NULL);
	assert(node->loadlevel_event == NULL);
	assert(node->wait_event == NULL);
	assert(node->shutdown_event == NULL);
	
	free(node->name);
	node->name = NULL;
	
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
		evtimer_add(node->shutdown_event, &_shutdown_timeout);
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
			
			for (i=0; i<_node_count; i++) {
				if (_nodes[i] == node) {
					_nodes[i] = NULL;
					break;
				}
			}
			
			node_free(node);
		}
	}
}



static void bucket_shutdown_handler(evutil_socket_t fd, short what, void *arg) 
{
	bucket_t *bucket = arg;
	int done = 0;
	
	assert(fd == -1);
	assert(arg);
	assert(bucket);
	assert(bucket->shutdown_event);
	
	// if the bucket is a backup bucket, we can simply destroy it, and send out a message to clients that it is no longer the backup for the bucket.
	if (bucket->level > 0) {
		done ++;
	}
	else {
		assert(bucket->level == 0);

		// if the bucket is primary, but there are no nodes to send it to, then we destroy it.
		if (_node_count == 0) {
			done ++;
		}
		else {
			assert(_node_count > 0);
			
			// If the backup node is connected, then we will tell that node, that it has been 
			// promoted to be primary for the bucket.
			if (bucket->backup_node && bucket->backup_node->client) {
				push_promote(bucket->backup_node->client, bucket->hash);

				done ++;
			}
			else {
			
				// at this point, we are the primary and there is no backup.  There are other nodes connected, so we need to try and transfer this bucket to another node.
				assert(0);
				
				assert(done == 0);
				
// 				assert(_buckets[i]->transfer_event == NULL);
// 				_buckets[i]->transfer_event = evtimer_new(_evbase, bucket_transfer_handler, _buckets[i]);
// 				assert(_buckets[i]->transfer_event);
			}
		}
	}
	
	if (done > 0) {
		// we are done with the bucket.
	
		bucket_destroy(bucket);
		all_push_hashmask(bucket->hash);
				
		event_free(bucket->shutdown_event);
		bucket->shutdown_event = NULL;

		assert(_buckets[bucket->hash] == bucket);
		_buckets[bucket->hash] = NULL;
		free(bucket);
		bucket = NULL;
	}
	else {
		// we are not done yet, so we need to schedule the event again.
		assert(bucket->shutdown_event);
		evtimer_add(bucket->shutdown_event, &_shutdown_timeout);
	}		
}



//-----------------------------------------------------------------------------
static void shutdown_handler(evutil_socket_t fd, short what, void *arg)
{
	int waiting = 0;
	int i;
	
	assert(fd == -1);
	assert(what & EV_TIMEOUT);
	assert(arg == NULL);

	if (_verbose) printf("SHUTDOWN handler\n");

	// setup a shutdown event for all the nodes.
	for (i=0; i<_node_count; i++) {
		if (_nodes[i]) {
			waiting++;
			if (_nodes[i]->shutdown_event == NULL) {
				
				assert(_evbase);
				_nodes[i]->shutdown_event = evtimer_new(_evbase, node_shutdown_handler, _nodes[i]);
				assert(_nodes[i]->shutdown_event);
				evtimer_add(_nodes[i]->shutdown_event, &_now_timeout);
	}	}	}
	
	assert(_node_count >= 0);
	if (_node_count > 0) {
		while (_nodes[_node_count - 1] == NULL) {
			_node_count --;
	}	}
	
	if (_node_count == 0) {
		if (_nodes) {
			free(_nodes);
			_nodes = NULL;
		}
	}
	else {
		assert(waiting > 0);
	}

	// need to send a message to each node telling them that we are shutting down.
	for (i=0; i<_client_count; i++) {
		if (_clients[i]) {
			waiting ++;
			if (_clients[i]->shutdown_event == NULL) {
				
				assert(_evbase);
				_clients[i]->shutdown_event = evtimer_new(_evbase, client_shutdown_handler, _clients[i]);
				assert(_clients[i]->shutdown_event);
				evtimer_add(_clients[i]->shutdown_event, &_now_timeout);
	}	}	}
	
	assert(_client_count >= 0);
	if (_client_count > 0) {
		while (_clients[_client_count - 1] == NULL) {
			_client_count --;
	}	}
	
	if (_client_count == 0) {
		if (_clients) {
			free(_clients);
			_clients = NULL;
		}
	}
	else {
		assert(waiting > 0);
	}
	
	
	// start a timeout event for each bucket, to attempt to send it to other nodes.
	if (_buckets) {
		assert(_mask > 0);
		for (i=0; i<=_mask; i++) {
			if (_buckets[i]) {
				waiting ++;
				if (_buckets[i]->shutdown_event == NULL) {
					printf("Bucket shutdown initiated: %d\n", i);

					assert(_evbase);
					_buckets[i]->shutdown_event = evtimer_new(_evbase, bucket_shutdown_handler, _buckets[i]);
					assert(_buckets[i]->shutdown_event);
					evtimer_add(_buckets[i]->shutdown_event, &_now_timeout);
	}	}	}	}
	
	if (_server) {
		if (_verbose) printf("Shutting down server interface: %s\n", _interface);
		server_shutdown();
	}
	
	
	if (waiting > 0) {
		if (_verbose) printf("WAITING FOR SHUTDOWN.  nodes=%d, clients=%d, buckets=%d\n", _node_count, _client_count, _bucket_count);
		evtimer_add(_shutdown_event, &_shutdown_timeout);
	}
	else {
		assert(_seconds_event);
		event_free(_seconds_event);
		_seconds_event = NULL;
		
		assert(_stats_event);
		event_free(_stats_event);
		_stats_event = NULL;
	}
}











//-----------------------------------------------------------------------------
// Since this is the interrupt handler, we need to do as little as possible here, and just start up an event to take care of it.
static void sigint_handler(evutil_socket_t fd, short what, void *arg)
{
	assert(arg == NULL);
	assert(_shutdown <= 0);

 	if (_verbose > 0) printf("\nSIGINT received.\n\n");

	// delete the signal events.
	assert(_sigint_event);
	event_free(_sigint_event);
	_sigint_event = NULL;

	assert(_sighup_event);
	event_free(_sighup_event);
	_sighup_event = NULL;
	
	// start the shutdown event.  This timeout event will just keep ticking over until the _shutdown 
	// value is back down to 0, then it will stop resetting the event, and the loop can exit.... 
	// therefore shutting down the service completely.
	_shutdown_event = evtimer_new(_evbase, shutdown_handler, NULL);
	assert(_shutdown_event);
	evtimer_add(_shutdown_event, &_now_timeout);
	
// 	printf("SIGINT complete\n");
}


//-----------------------------------------------------------------------------
// When SIGHUP is received, we need to re-load the config database.  At the
// same time, we should flush all caches and buffers to reduce the system's
// memory footprint.   It should be as close to a complete app reset as
// possible.
static void sighup_handler(evutil_socket_t fd, short what, void *arg)
{
	assert(arg == NULL);

	// clear out all cached objects.
	assert(0);

	// reload the config database file.
	assert(0);

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
	//       sent data from hte buffer which results in moving data in memory.
	
	assert(client->write_event);
	assert(client->out.buffer);
	assert(client->out.length > 0);
	assert( ( client->out.offset + client->out.length ) <= client->out.max);
	
	assert(client->handle > 0);
	
	res = send(client->handle, client->out.buffer + client->out.offset, client->out.length, 0);
	if (res > 0) {
		
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
}





static void payload_int(int value)
{
	int avail;
	int *ptr;
	
	avail = _payload_max - _payload_length;
	if (avail < sizeof(int)) {
		_payload = realloc(_payload, _payload_max + DEFAULT_BUFSIZE);
		assert(_payload);
		_payload_max += DEFAULT_BUFSIZE;
	}
	
	ptr = ((void*) _payload + _payload_length);
	ptr[0] = htonl(value);
	
	_payload_length += sizeof(int);
	
	assert(_payload);
	assert(_payload_length <= _payload_max);
}



static void payload_data(int length, void *data)
{
	int avail;
	int *ptr;
	
	avail = _payload_max - _payload_length;
	while (avail < (sizeof(int)+length)) {
		_payload = realloc(_payload, _payload_max + DEFAULT_BUFSIZE);
		assert(_payload);
		_payload_max += DEFAULT_BUFSIZE;
		avail += DEFAULT_BUFSIZE;
	}

	// add the length of the string first.
	ptr = ((void*) _payload + _payload_length);
	ptr[0] = htonl(length);
	
	memcpy(_payload + _payload_length + sizeof(int), data, length);
	
	_payload_length += sizeof(int) + length;
	
	assert(_payload);
	assert(_payload_length <= _payload_max);
}



static void payload_string(const char *str)
{
	payload_data(strlen(str), (void*)str);
}




// Pushes out a command to the specified client (which should be a cluster node), informing it that we are a cluster node also, that our interface is such and such.
static void push_serverhello(client_t *client)
{
	assert(client);
	assert(client->handle > 0);
	
	assert(_payload_length == 0);
	payload_string(_interface);
	
	printf("[%d] SERVERHELLO: Interface:'%s', length=%d, payload=%d\n", (int)(_current_time.tv_sec-_start_time.tv_sec), _interface, (int)strlen(_interface), _payload_length);
	
	send_message(client, NULL, CMD_SERVERHELLO, _payload_length, _payload);
	_payload_length = 0;
}

// Pushes out a command to the specified client (which should be a cluster node), informing it that 
// we are a cluster node also, that our interface is such and such.   We do not send PINGS to 
// regular clients.
static void push_ping(client_t *client)
{
	assert(client);
	assert(client->handle > 0);
	assert(client->node);
	
	assert(_payload_length == 0);
	
	send_message(client, NULL, CMD_PING, 0, NULL);
	assert(_payload_length == 0);
}



// Pushes out a command to the specified client, with the list of servers taht are maintained.
static void push_serverlist(client_t *client)
{
	int i;
	
	assert(client);
	assert(client->handle > 0);
	assert(_payload_length == 0);
	assert(_node_count >= 0);
	
	payload_int(_node_count);
	for (i=0; i<_node_count; i++) {
		payload_string(_nodes[i]->name);
	}
	
	send_message(client, NULL, CMD_SERVERLIST, _payload_length, _payload);
	
	_payload_length = 0;
}


// send the hashmasks list to the client.
static void push_hashmasks(client_t *client)
{
	int i;
	int check;
	
	assert(client);
	assert(client->handle > 0);
	
	assert(_payload_length == 0);

	assert(_mask >= 0);
	
	// first we set the number of buckets this server contains.
	payload_int(_mask);
	payload_int(_bucket_count);
	
	check = 0;
	for (i=0; i<=_mask; i++) {

		if (_buckets[i]->level == 0) {
			
			// we have the bucket, and we are the 'primary for it'
			
			assert(_buckets[i]->tree);
			assert(_buckets[i]->target_host == NULL);
			assert(_buckets[i]->target_node == NULL);
			
			payload_int(i);
			payload_int(0);
			
			check++;
		}
		else if (_buckets[i]->level > 0) {
			
			// we have the bucket, but we are a backup for it. 
			
			assert(_buckets[i]->tree);
			assert(_buckets[i]->target_host);
			assert(_buckets[i]->target_node);

			payload_int(i);
			payload_int(_buckets[i]->level);
			
			check++;
		}
		else  {
			
			// we dont have the bucket at all, so we dont include it in the list.
			
			assert(_buckets[i]->level == -1);
			assert(_buckets[i]->tree == NULL);
			assert(_buckets[i]->backup_host == NULL);
			assert(_buckets[i]->backup_node == NULL);
			
		}
	}

	assert(_bucket_count == 0 || (check) == _bucket_count);

	send_message(client, NULL, CMD_HASHMASKS, _payload_length, _payload);

	_payload_length = 0;
}


// This function will return a pointer to the internal data.  It will also update the length variable to indicate the length of the string.  It will increase the 'next' to point to the next potential field in the payload.  If there is no more data in the payload, you will need to check that yourself
static char * data_string(char **data, int *length)
{
	char *str;
	int *ptr;
	
	assert(data);
	assert(*data);
	assert(length);
	
	
	ptr = (void*) *data;
	*length = ntohl(ptr[0]);

	str = *data + sizeof(int);

	*data += (sizeof(int) + *length);

	return(str);
}


// This function will return a pointer to the internal data.  It will also update the length variable to indicate the length of the string.  It will increase the 'next' to point to the next potential field in the payload.  If there is no more data in the payload, you will need to check that yourself
static int data_int(char **data)
{
	int *ptr;
	int value;
	
	assert(data);
	assert(*data);
	
	ptr = (void*) *data;
	value = ntohl(ptr[0]);

	*data += sizeof(int);
	
	return(value);
}




// the hello command does not require a payload, and simply does a reply.   
// However, it triggers a servermap, and a hashmasks command to follow it.
static void cmd_hello(client_t *client, header_t *header)
{
	assert(client);
	assert(header);

	// there is no payload required for an ack.
	
	// send the ACK reply.
	send_message(client, header, REPLY_ACK, 0, NULL);
	
	// send a servermap command to the client.
	push_serverlist(client);
	
	// send a hashmasks command to the client.
	push_hashmasks(client);
}


static void cmd_ping(client_t *client, header_t *header)
{
	assert(client);
	assert(header);

	// there is no payload required for an ack.
	
	// send the ACK reply.
	send_message(client, header, REPLY_ACK, 0, NULL);
}


static node_t * node_new(const char *name) 
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





// the hello command does not require a payload, and simply does a reply.   
// However, it triggers a servermap, and a hashmasks command to follow it.
static void cmd_serverhello(client_t *client, header_t *header, char *payload)
{
	int length;
	char *next;
	char *raw_name;
	char *name;
	int used = 0;
	int i;
	int found;
	
	assert(client);
	assert(header);
	assert(payload);

	next = payload;
	
	assert(client->node == NULL);
	
	// the only parameter is a string indicating the servers connection data.  
	// Normally an IP and port.
	raw_name = data_string(&next, &length);
	assert(length > 0);
	assert(raw_name);
	used += sizeof(int) + length;
	
	// we need to mark this client as a node connection, we do this by settings its node name.
	assert(length > 0);
	name = malloc(length +1);
	strncpy(name, raw_name, length);
	name[length] = 0;
	
	
	// we have a server name.  We need to check it against out node list.  If it is there, then we 
	// dont do anything.  If it is not there, then we need to add it.
	found = -1;
	for (i=0; i<_node_count && found < 0; i++) {
		
		// at this point, every 'node' should have a client object attached.
		assert(_nodes[i]);
		assert(_nodes[i]->client);
		assert(_nodes[i]->name);
		
		if (strcmp(_nodes[i]->name, name) == 0) {
			found = i;
		}
	}
	
	if (found < 0) {
		// the node was not found in the list.  We need to add it to the list, and add this client 
		// to the node.
		
		_nodes = realloc(_nodes, sizeof(node_t *) * (_node_count + 1));
		assert(_nodes);
		_nodes[_node_count] = node_new(name);
		_nodes[_node_count]->client = client;
		client->node = _nodes[_node_count];
		_node_count ++;
		
		_active_nodes ++;
		
		printf("Adding '%s' as a New Node.\n", name);
	}
	else {
		
		// the node was found in the list.  Now we need to remove the existing one, and replace it 
		// with this new one.
		assert(_nodes[found]->client);
		if (_nodes[found]->client == client) {
			// is this even possible?
			assert(0);
		}
		else {
			
			if (_nodes[found]->client->read_event) {
				// this client object already exists and is reading data.  We need to close this.
				assert(0);
			}
			else {
				if (_nodes[found]->connect_event) {
					assert(_nodes[found]->client->handle > 0);
					assert(_nodes[found]->wait_event == NULL);
					
					// this client is in the middle of connecting, and we've received a connection at the same time.
					assert(0);
				}
				else {
					assert(_nodes[found]->connect_event == NULL);
					assert(_nodes[found]->wait_event);
					assert(_nodes[found]->client->read_event == NULL);
					assert(_nodes[found]->client->handle == -1);
					
					// the client is waiting to connect, so we can break it down.
					client_free(_nodes[found]->client);
					assert(_nodes[found]->client == NULL);
					client->node = _nodes[found];
	}	}	}	}
	
	// send the ACK reply.
	send_message(client, header, REPLY_ACK, 0, NULL);
}


static gint key_compare_fn(gconstpointer a, gconstpointer b)
{
	const register unsigned int *aa, *bb;
	
	aa = a;
	bb = b;
	
	return((*aa) - (*bb));
}


static void value_free(value_t *value)
{
	assert(value);
	if (value->type == VALUE_STRING) {
		assert(value->data.s.data);
		free(value->data.s.data);
	}
	free(value);
}

static int store_value(int map_hash, int key_hash, char *name, int expires, value_t *value) 
{
	int bucket_index;
	bucket_t *bucket;
	maplist_t *list;
	item_t *item;
	int result = -1;

		// calculate the bucket that this item belongs in.
	bucket_index = _mask & key_hash;
	assert(bucket_index >= 0);
	assert(bucket_index < _mask);
	bucket = _buckets[bucket_index];
	assert(bucket->hash == bucket_index);

	// if we have a record for this bucket, then we are either a primary or a backup for it.
	if (bucket) {
	
		// make sure that this server is 'primary' for this bucket.
		if (bucket->level == 0) {
			
			
			// search the btree in the bucket for this key.
			assert(bucket->tree);
			list = g_tree_lookup(bucket->tree, &key_hash);
			if (list == NULL) {
				// the key is not in the btree at all.  Need to create a new maplist and add it to the btree.
				list = malloc(sizeof(maplist_t));
				assert(list);
				list->key = key_hash;
				list->mapstree = g_tree_new(key_compare_fn);
				assert(list->mapstree);

				g_tree_insert(bucket->tree, &list->key, list);
			}
			
			assert(list);
			
			// search the list of maps for a match.
			assert(list->mapstree);
			item = g_tree_lookup(list->mapstree, &map_hash);
			if (item == NULL) {
				// item was not found.  We need to create a new item, and add it to the maps tree.
				item = malloc(sizeof(item_t));
				assert(item);

				item->key = key_hash;
				item->map_key = map_hash;
				item->name = name;
				item->value = value;
				
				g_tree_insert(list->mapstree, &item->map_key, item);
			}
			else {
				// if item is found, replace the data.
				assert(item->name);
				free(item->name);
				item->name = name;
				
				value_free(item->value);
				item->value = value;
			}
			
			if (expires == 0) { item->expires = 0; }
			else { item->expires = _current_time.tv_sec + expires; }
			
			assert(result < 0);
			result = 0;
		}
		else {
			// we need to reply with an indication of which server is actually responsible for this bucket.
			assert(result < 0);
			assert(0);
		}
	}
	
	return(result);
}


static int get_value(int map_hash, int key_hash, value_t **value) 
{
	int bucket_index;
	bucket_t *bucket;
	maplist_t *list;
	item_t *item;
	int result = -1;

		// calculate the bucket that this item belongs in.
	bucket_index = _mask & key_hash;
	assert(bucket_index >= 0);
	assert(bucket_index < _mask);
	bucket = _buckets[bucket_index];
	assert(bucket->hash == bucket_index);

	// if we have a record for this bucket, then we are either a primary or a backup for it.
	if (bucket) {

		// make sure that this server is 'primary' for this bucket.
		if (bucket->level != 0) {
			// we need to reply with an indication of which server is actually responsible for this bucket.
			assert(result < 0);
			assert(0);
		}
		else {
			
			// search the btree in the bucket for this key.
			assert(bucket->tree);
			
			list = g_tree_lookup(bucket->tree, &key_hash);
			if (list == NULL) {
				assert(result < 0);
			}
			else {
			
				// search the list of maps for a match.
				assert(list->mapstree);
				item = g_tree_lookup(list->mapstree, &map_hash);
				if (item == NULL) {
					assert(result < 0);
				}
				else {
					// item is found, return with the data.
					assert(item->name);
					assert(item->value);
					
					if (item->expires > 0 && item->expires < _current_time.tv_sec) {
						// item has expired.   We need to remove it from the map list.
						assert(result < 0);
						assert(0);
					}
					else {
						*value = item->value;

						assert(result < 0);
						result = 0;
	}	}	}	}	}
	
	return(result);
}





// Set a value into the hash storage.
static void cmd_set_int(client_t *client, header_t *header, char *payload)
{
	char *next;
	int map_hash;
	int key_hash;
	int expires;
	int fullwait;
	value_t *value;
	char *str;
	char *name;
	int name_len;
	int result;
	
	assert(client);
	assert(header);
	assert(payload);

	// create a new value.
	value = calloc(1, sizeof(value_t));
	assert(value);
	
	next = payload;
	
	map_hash = data_int(&next);
	key_hash = data_int(&next);
	expires = data_int(&next);
	fullwait = data_int(&next);
	str = data_string(&next, &name_len);
	value->type = VALUE_INT;
	value->data.i = data_int(&next);
	
	assert(str && name_len > 0);
	
	name = malloc(name_len + 1);
	memcpy(name, str, name_len);
	name[name_len] = 0;
	
	if (_verbose > 2) printf("[%d] CMD: set (integer): [%d/%d]'%s'=%d\n\n", (int)(_current_time.tv_sec-_start_time.tv_sec), map_hash, key_hash, name, value->data.i);

	// eventually we will add the ability to wait until the data has been competely distributed 
	// before returning an ack.
	assert(fullwait == 0);

	// store the value into the trees.  If a value already exists, it will get released and this one 
	// will replace it, so control of this value is given to the tree structure.
	// NOTE: value is controlled by the tree after this function call.
	// NOTE: name is controlled by the tree after this function call.
	result = store_value(map_hash, key_hash, name, expires, value);
	value = NULL;
	name = NULL;
	
	// send the ACK reply.
	if (result == 0) {
		send_message(client, header, REPLY_ACK, 0, NULL);
		_payload_length = 0;
	}
	else {
		assert(0);
	}
}


// Get a value from storage.
static void cmd_get_int(client_t *client, header_t *header, char *payload)
{
	char *next;
	int map_hash;
	int key_hash;
	value_t *value;
	int result;
	
	assert(client);
	assert(header);
	assert(payload);

	next = payload;
	
	map_hash = data_int(&next);
	key_hash = data_int(&next);
	
	
	if (_verbose > 2) printf("[%d] CMD: get (integer)\n\n", (int)(_current_time.tv_sec-_start_time.tv_sec));


	// store the value into the trees.  If a value already exists, it will get released and this one 
	// will replace it, so control of this value is given to the tree structure.
	// NOTE: value is controlled by the tree after this function call.
	// NOTE: name is controlled by the tree after this function call.
	value = NULL;
	result = get_value(map_hash, key_hash, &value);
	
	// send the ACK reply.
	if (result == 0) {
		assert(value);
		
		if (value->type != VALUE_INT) {
			// need to indicate stored value is a different type.
			assert(0);
		}
		else {
		
			// build the reply.
			assert(_payload);
			assert(_payload_length == 0);
			assert(_payload_max > 0);
			
			payload_int(map_hash);
			payload_int(key_hash);
			payload_int(value->data.i);
		}
		
		assert(_payload_length > 0);
		send_message(client, header, REPLY_DATA_INT, _payload_length, _payload);
		_payload_length = 0;
	}
	else {
		assert(0);
	}
}



static void process_ack(client_t *client, header_t *header)
{
	assert(client);
	assert(header);
	
	// if there are any special repcmds that we need to process, then we would add them here.  

	if (header->repcmd == CMD_SERVERHELLO) {
		_active_nodes ++;
		if (_verbose) printf("Active cluster node connections: %d\n", _active_nodes);
	}
	
}


static void process_unknown(client_t *client, header_t *header)
{
	assert(client);
	assert(header);

	// we sent a command, and the client didn't know what to do with it.  If we had certain modes 
	// we could enable for compatible capabilities (for this client), we would do it here.
	assert(0);
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
	
	assert(sizeof(char) == 1);
	assert(sizeof(short int) == 2);
	assert(sizeof(int) == 4);
	
	assert(client);
	assert(client->handle > 0);

	while (stopped == 0) {
			
		assert(client->in.buffer);
		assert((client->in.length + client->in.offset) <= client->in.max);
		
		// if we dont have 10 characters, then we dont have enough to build a message.  Messages are at
		// least that.
		if (client->in.length < HEADER_SIZE) {
			// we didn't have enough, even for the header, so we are stopping.
			stopped = 1;
		}
		else {
			
			// keeping in mind the offset, get the 4 params, and determine what we need to do with them.
			
			// *** performance tuning.  We should only parse the header once.  It should be saved in the client object and only done once.
			
			raw = (void *) (client->in.buffer + client->in.offset);

			header.command = ntohs(raw->command);
			header.repcmd = ntohs(raw->repcmd);
			header.userid = ntohl(raw->userid);
			header.length = ntohl(raw->length);
			
			if (_verbose > 4) {
				printf("[%d] New telegram: Command=%d, repcmd=%d, userid=%d, length=%d, buffer_length=%d\n", (int)(_current_time.tv_sec-_start_time.tv_sec), header.command, header.repcmd, header.userid, header.length, client->in.length);
			}
			
			if ((client->in.length-HEADER_SIZE) < header.length) {
				// we dont have enough data yet.
				stopped = 1;
			}
			else {
				
				// get a pointer to the payload
				ptr = client->in.buffer + client->in.offset + HEADER_SIZE;
				assert(ptr);
				
				if (header.repcmd != 0) {
					// we have received a reply to a command we issued.  So we need to process that reply.
					// we have enough data, so we need to pass it on to the functions that handle it.
					switch (header.command) {
						case REPLY_ACK:     process_ack(client, &header); 		break;
						case REPLY_UNKNOWN: process_unknown(client, &header); 	break;
						default:
							if (_verbose > 1) {
								printf("[%d] Unknown reply: Reply=%d, Command=%d, userid=%d, length=%d\n", 
									(int)(_current_time.tv_sec-_start_time.tv_sec), header.repcmd, header.command, header.userid, header.length);
							}
							break;
					}
				}
				else {
				
					// we have enough data, so we need to pass it on to the functions that handle it.
					switch (header.command) {
						case CMD_HELLO: 		cmd_hello(client, &header); 			break;
						case CMD_SERVERHELLO: 	cmd_serverhello(client, &header, ptr); 	break;
						case CMD_PING: 	        cmd_ping(client, &header); 				break;
						case CMD_SET_INT: 		cmd_set_int(client, &header, ptr); 		break;
						case CMD_GET_INT: 		cmd_get_int(client, &header, ptr); 		break;
						default:
							// got an invalid command, so we need to reply with an 'unknown' reply.
							// since we have the raw command still in our buffer, we can use that 
							// without having to build a normal reply.
							if (_verbose > 1) { printf("[%d] Unknown command received: Command=%d, userid=%d, length=%d\n", (int)(_current_time.tv_sec-_start_time.tv_sec), header.command, header.userid, header.length); }
							send_message(client, &header, REPLY_UNKNOWN, 0, NULL);
							break;
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
	}	}	}
	
	assert(stopped != 0);
	
	return(processed);
}


// if the name is passed it, then it is assumed that this is the first time a connect is attempted.  If a name is not supplied (ie, it is NULL), then we simply re-use the existing data in the client object.
static void node_connect(node_t *node) 
{
	int len;
	int sock;
	struct sockaddr saddr;

	
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
	event_add(node->connect_event, &_connect_timeout);

	assert(node->wait_event == NULL);
	assert(node->connect_event);
}



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

	if (_shutdown <= 0) {
		node_connect(node);
	}
}



// this function must assume that the client object has been destroyed because the connection was 
// lost, and we need to setup a wait event to try and connect again later.
static void node_retry(node_t *node)
{
	assert(node);
	assert(_evbase);
	
	node->client = NULL;
	
	
	assert(node->connect_event == NULL);
	assert(node->wait_event == NULL);
	node->wait_event = evtimer_new(_evbase, node_wait_handler, (void *) node);
	evtimer_add(node->wait_event, &_node_wait_timeout);
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
			if (_verbose > 2) {
				printf("[%d] client timed out. handle=%d\n", (int)(_current_time.tv_sec-_start_time.tv_sec), fd);
			}
			
			// because the client has timed out, we need to clear out any data that we currently have for it.
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
			/* we want to increase buffer size, so we'll add another DEFAULT_BUFSIZE to the 
			max.  This should keep it in multiples of DEFAULT_BUFSIZE, regardless of how 
			much is available for each read.
			*/
			
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
			if (_verbose > 2)  printf("socket %d closed. res=%d, errno=%d,'%s'\n", fd, res, errno, strerror(errno));
			
			// free the client resources.
			if (client->node) {
				// this client is actually a node connection.  We need to create an event to wait 
				// and then try connecting again.

				_active_nodes --;
				assert(_active_nodes >= 0);

				node_retry(client->node);
			}

			client_free(client);
			client = NULL;
}	}	}




//-----------------------------------------------------------------------------
// print some info to the user, so that they can know what the parameters do.
static void usage(void) {
	printf(PACKAGE " " VERSION "\n");
	printf("-l <ip_addr:port>  interface to listen on, default is localhost:13600\n");
	printf("-c <num>           max simultaneous connections, default is 1024\n");
	printf("-m <mb>            mb of RAM to allocate to the cluster.\n");
	printf("-n <node>          Other cluster node to connect to. Can be specified more than once.\n");
	printf("\n");
	printf("-d                 run as a daemon\n");
	printf("-P <file>          save PID in <file>, only used with -d option\n");
	printf("-u <username>      assume identity of <username> (only when run as root)\n");
	printf("\n");
	printf("-v                 verbose (print errors/warnings while in event loop)\n");
	printf("-h                 print this help and exit\n");
	return;
}


static void parse_params(int argc, char **argv)
{
	int c;
	
	assert(argc >= 0);
	assert(argv);
	
	// process arguments
	/// Need to check the options in here, there're possibly ones that we dont need.
	while ((c = getopt(argc, argv, 
		"c:"    /* max connections. */
		"h"     /* help */
		"v"     /* verbosity */
		"d"     /* daemon */
		"u:"    /* user to run as */
		"P:"    /* PID file */
		"l:"    /* interfaces to listen on */
		"n:"    /* other node to connect to */
		)) != -1) {
		switch (c) {
			case 'c':
				_maxconns = atoi(optarg);
				assert(_maxconns > 0);
				break;
			case 'h':
				usage();
				exit(EXIT_SUCCESS);
			case 'v':
				_verbose++;
				break;
			case 'd':
				assert(_daemonize == 0);
				_daemonize = 1;
				break;
			case 'u':
				assert(_username == NULL);
				_username = strdup(optarg);
				assert(_username != NULL);
				assert(_username[0] != 0);
				break;
			case 'P':
				assert(_pid_file == NULL);
				_pid_file = strdup(optarg);
				assert(_pid_file != NULL);
				assert(_pid_file[0] != 0);
				break;
			case 'l':
				_interface = optarg;
				assert(_interface != NULL);
				assert(_interface[0] != 0);
				break;
			case 'n':
				assert((_node_count == 0 && _nodes == NULL) || (_node_count > 0 && _nodes));
				_nodes = realloc(_nodes, sizeof(node_t *) * (_node_count + 1));
				assert(_nodes);
				_nodes[_node_count] = node_new(optarg);
				_node_count ++;
				break;
				
			default:
				fprintf(stderr, "Illegal argument \"%c\"\n", c);
				return;
				
}	}	}


void daemonize(const char *username, const char *pidfile, const int noclose)
{
	struct passwd *pw;
	struct sigaction sa;
	int fd;
	FILE *fp;
	
	if (getuid() == 0 || geteuid() == 0) {
		if (username == 0 || *username == '\0') {
			fprintf(stderr, "can't run as root without the -u switch\n");
			exit(EXIT_FAILURE);
		}
		assert(username);
		pw = getpwnam((const char *)username);
		if (pw == NULL) {
			fprintf(stderr, "can't find the user %s to switch to\n", username);
			exit(EXIT_FAILURE);
		}
		if (setgid(pw->pw_gid) < 0 || setuid(pw->pw_uid) < 0) {
			fprintf(stderr, "failed to assume identity of user %s\n", username);
			exit(EXIT_FAILURE);
		}
	}
	
	sa.sa_handler = SIG_IGN;
	sa.sa_flags = 0;
	if (sigemptyset(&sa.sa_mask) == -1 || sigaction(SIGPIPE, &sa, 0) == -1) {
		perror("failed to ignore SIGPIPE; sigaction");
		exit(EXIT_FAILURE);
	}
	
	switch (fork()) {
		case -1:
			exit(EXIT_FAILURE);
		case 0:
			break;
		default:
			_exit(EXIT_SUCCESS);
	}
	
	if (setsid() == -1)
		exit(EXIT_FAILURE);
	
	(void)chdir("/");
	
	if (noclose == 0 && (fd = open("/dev/null", O_RDWR, 0)) != -1) {
		(void)dup2(fd, STDIN_FILENO);
		(void)dup2(fd, STDOUT_FILENO);
		(void)dup2(fd, STDERR_FILENO);
		if (fd > STDERR_FILENO)
			(void)close(fd);
	}
	
	// save the PID in if we're a daemon, do this after thread_init due to a
	// file descriptor handling bug somewhere in libevent
	if (pidfile != NULL) {
		if ((fp = fopen(pidfile, "w")) == NULL) {
			fprintf(stderr, "Could not open the pid file %s for writing\n", pidfile);
			exit(EXIT_FAILURE);
		}
		
		fprintf(fp,"%ld\n", (long)getpid());
		if (fclose(fp) == -1) {
			fprintf(stderr, "Could not close the pid file %s.\n", pidfile);
			exit(EXIT_FAILURE);
		}
	}
}





static void node_connect_handler(int fd, short int flags, void *arg)
{
	node_t *node = (node_t *) arg;
	int error;
	
	assert(fd >= 0 && flags != 0 && node);
	if (_verbose) printf("[%d] CONNECT: handle=%d\n", (int)(_current_time.tv_sec-_start_time.tv_sec), fd);

	if (flags & EV_TIMEOUT) {
		// timeout on the connect.  Need to handle that somehow.
		
		if (_verbose) printf("[%d] Timeout connecting to: %s\n", (int)(_current_time.tv_sec-_start_time.tv_sec), node->name);
		
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
			evtimer_add(node->wait_event, &_node_wait_timeout);
		}
		else {
			if (_verbose) printf("Connected to node: %s\n", node->name);
			
			// we've connected to another server.... 
			// TODO: we still dont know if its a valid connection, but we can delay the _settling event.

			assert(node->connect_event == NULL);
			assert(node->wait_event == NULL);
			
			node->client = client_new();
			node->client->node = node;
			node->client->handle = fd;
			
			assert(_evbase);
			assert(node->client->handle > 0);
			assert(node->client->read_event == NULL);
			node->client->read_event = event_new( _evbase, fd, EV_READ|EV_PERSIST, read_handler, node->client);
			assert(node->client->read_event);
			int s = event_add(node->client->read_event, &_standard_timeout);
			assert(s == 0);
			
			// should send a SERVERHELLO command to the server we've connected to.
			push_serverhello(node->client);
}	}	}








// the nodes array should already be initialised, and there should already be an entry for each node 
// in the command-line params, we need to initiate a connection attempt (which will then need to be 
// handled through the event system)
void connect_nodes(void)
{
	int i;
	
	for (i=0; i<_node_count; i++) {
		assert(_nodes);
		assert(_nodes[i]);
		assert(_nodes[i]->name);
		if (_nodes[i]->client == NULL) {
			node_connect(_nodes[i]);
}	}	}


// send a message to all connected clients informing them of the new hashmask info.
static void all_hashmask(unsigned int hashmask, int level)
{
	int j;
	
	assert(level >= 0);
	assert(hashmask <= _mask);
	
	assert(_payload_length == 0);
	
	assert(_mask > 0);
	assert(_bucket_count > 0);
	
	// build the message first.
	// first comes the mask.
	payload_int(_mask);
	payload_int(hashmask);
	payload_int(level);
	
	
	// for each client
	for (j=0; j<_client_count; j++) {
		assert(_clients);
		if (_clients[j]) {
		
			// check if there is a read-event setup for the client.  Because that is a good 
			// indicater that the client is connected.
			if (_clients[j]->read_event) {
				// send the message to the client.
				send_message(_clients[j], NULL, CMD_HASHMASK, _payload_length, _payload);	
			}
		}
	}

	_payload_length = 0;
}


static bucket_t * bucket_new(hash_t hash)
{
	bucket_t *bucket;
	
	assert(_buckets[hash] == NULL);
	
	bucket = calloc(1, sizeof(bucket_t));
	bucket->hash = hash;
	assert(bucket->level == 0);
			
	assert(bucket->backup_host == NULL);
	assert(bucket->backup_node == NULL);
	assert(bucket->target_host == NULL);
	assert(bucket->target_node == NULL);
	assert(bucket->logging_host == NULL);
	assert(bucket->logging_node == NULL);
	assert(bucket->transfer_event == NULL);
	assert(bucket->shutdown_event == NULL);
			
	bucket->tree = g_tree_new(key_compare_fn);
	assert(bucket->tree);
	
	return(bucket);
}

// this handler is fired 5 seconds after the daemon starts up.  It checks to see if it has connected 
// to other nodes, if it hasn't then it initialises the cluster as if it is the first node in the 
// cluster.
static void settle_handler(int fd, short int flags, void *arg)
{
	unsigned int i;
	
	assert(fd == -1);
	assert(flags & EV_TIMEOUT);
	assert(arg == NULL);

	if (_verbose) printf("SETTLE: handle=%d\n", fd);
	
	if (_active_nodes == 0) {
		if (_verbose) printf("Settle timeout.  No Node connections.  Setting up cluster.\n");
		
		assert(_mask == 0);
		_mask = STARTING_MASK;
		assert(_mask > 0);
		
		_buckets = malloc(sizeof(bucket_t *) * (_mask + 1));
		assert(_buckets);
		
		_bucket_count = _mask + 1;
		
		// for starters we will need to create a bucket for each hash.
		for (i=0; i<=_mask; i++) {
			_buckets[i] = bucket_new(i);

			// send out a message to all connected clients, to let them know that the buckets have changed.
			all_hashmask(i, 0);
		}
		
		_settling = 0;
		
		if (_verbose) printf("Current buckets: %d\n", _bucket_count);
	}
}


// several times a second, this handler will fire and get the current time.  Normally we only care 
// about seconds, so as long as we check several times a second it will be accurate enough.
static void seconds_handler(int fd, short int flags, void *arg)
{
	assert(fd == -1);
	assert(flags & EV_TIMEOUT);
	assert(arg == NULL);

	gettimeofday(&_current_time, NULL);

	evtimer_add(_seconds_event, &_seconds_timeout);
}


// this handler fires, and it collates the stats collected, and possibly outputs to the log.
// If we have migrating stats, then we need to migrate to the next slot.
static void stats_handler(int fd, short int flags, void *arg)
{
	assert(fd == -1);
	assert(flags & EV_TIMEOUT);
	assert(arg == NULL);

// 	if (_verbose) printf("Stats. Nodes:%d\n", _active_nodes);
	
	evtimer_add(_stats_event, &_stats_timeout);
}




//-----------------------------------------------------------------------------
// Main... process command line parameters, and then setup our listening 
// sockets and event loop.
int main(int argc, char **argv) 
{

	assert(sizeof(char) == 1);
	assert(sizeof(short) == 2);
	assert(sizeof(int) == 4); 
	assert(sizeof(long) == 8);
	
///============================================================================
/// Initialization.
///============================================================================

	parse_params(argc, argv);

	// daemonize
	if (_daemonize) {
		daemonize(_username, _pid_file, _verbose);
	}


	gettimeofday(&_start_time, NULL);
	gettimeofday(&_current_time, NULL);

	
	// create our event base which will be the pivot point for pretty much everything.
#if ( _EVENT_NUMERIC_VERSION >= 0x02000000 )
	assert(event_get_version_number() == LIBEVENT_VERSION_NUMBER);
#endif
	
	_evbase = event_base_new();
	assert(_evbase);

	// initialise signal handlers.
	assert(_evbase);
	_sigint_event = evsignal_new(_evbase, SIGINT, sigint_handler, NULL);
	_sighup_event = evsignal_new(_evbase, SIGHUP, sighup_handler, NULL);
	assert(_sigint_event);
	assert(_sighup_event);
	event_add(_sigint_event, NULL);
	event_add(_sighup_event, NULL);
	
	
	// we need to set a timer to fire in 5 seconds to setup the cluster if no connections were made.
	_settle_event = evtimer_new(_evbase, settle_handler, NULL);
	assert(_settle_event);
	evtimer_add(_settle_event, &_settle_timeout);
	

	// we need to set a timer to fire in 5 seconds to setup the cluster if no connections were made.
	_seconds_event = evtimer_new(_evbase, seconds_handler, NULL);
	assert(_seconds_event);
	evtimer_add(_seconds_event, &_seconds_timeout);
	// we need to set a timer to fire in 5 seconds to setup the cluster if no connections were made.
	_stats_event = evtimer_new(_evbase, stats_handler, NULL);
	assert(_stats_event);
	evtimer_add(_stats_event, &_stats_timeout);


	// initialise the servers that we listen on.
	server_listen();

	// attempt to connect to the other known nodes in the cluster.
	connect_nodes();
	

///============================================================================
/// Main Event Loop.
///============================================================================

	// enter the processing loop.  This function will not return until there is
	// nothing more to do and the service has shutdown.  Therefore everything
	// needs to be setup and running before this point.  
	if (_verbose > 0) { printf("Starting main loop.\n"); }
	assert(_evbase);
	event_base_dispatch(_evbase);

///============================================================================
/// Shutdown
///============================================================================

	if (_verbose > 0) { printf("Freeing the event base.\n"); }
	assert(_evbase);
	event_base_free(_evbase);
	_evbase = NULL;

	// server interface should be shutdown.
	assert(_server == NULL);
	assert(_node_count == 0);
	assert(_client_count == 0);


	// make sure signal handlers have been cleared.
	assert(_sigint_event == NULL);
	assert(_sighup_event == NULL);

	
	// we are done, cleanup what is left in the control structure.
	if (_verbose > 0) { printf("Final cleanup.\n"); }
	
	_interface = NULL;
	
	if (_username) { free((void*)_username); _username = NULL; }
	if (_pid_file) { free((void*)_pid_file); _pid_file = NULL; }
	
	assert(_conncount == 0);
	assert(_sigint_event == NULL);
	assert(_sighup_event == NULL);
	assert(_evbase == NULL);
	
	return 0;
}


