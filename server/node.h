// node.h


#ifndef __NODE_H
#define __NODE_H

/*
 * Nodes are now handled slightly differently than it was before.  
 * Before a node was defined by its IP:Port.   But for a flexible enterprise product, we need more 
 * connectivity options than that.  So now nodes are identified by a public connection string.
 * See: https://github.com/hyper/opencluster/wiki/Connection-String
 * 
 * Internally, the string is hashed, and the hash is used to identify it quickly in an array, and 
 * use the connection string to connect to the node.
 * 
 */


#include "client.h"
#include "conninfo.h"
#include "hash.h"

typedef enum {
	UNKNOWN,
	INITIALIZED,
	CONNECTING,
	AUTHENTICATING,
	AUTHENTICATED,
	READY
} node_state_e;


typedef struct {
	hash_t nodehash;
	conninfo_t *conninfo;
	client_t *client;
	struct event *connect_event;
	struct event *loadlevel_event;
	struct event *wait_event;
	struct event *shutdown_event;
	int connect_attempts;
	node_state_e state;
} node_t;

void nodes_set_evbase(struct event_base *evbase);

// create new nodes... from different sources.
node_t * node_new(conninfo_t *conninfo);
node_t * node_new_file(const char *filename);
node_t * node_add(client_t *client, char *connect_info);

void node_connect_start(void);
void node_detach_client(node_t *node);
void node_retry(node_t *node);
void node_shutdown(node_t *node);

node_t * node_find(const char *conninfo_str);

int node_active_inc(void);
int node_active_dec(void);
int node_active_count(void);

int node_count(void);

void nodes_dump(void);

const char * node_name(node_t *node);

void nodes_shutdown(void);


#endif
