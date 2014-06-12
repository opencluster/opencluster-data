// node.h


#ifndef __NODE_H
#define __NODE_H


/*
 * Nodes are now handled slightly differently than it was before.  Before a node was defined by its IP:Port.   But for a flexible enterprise product, we need more connectivity options than that.  So now nodes are identified by a public connection string.
 * See: https://github.com/hyper/opencluster/wiki/Connection-String
 * 
 * Internally, the string is hashed, and the hash is used to identify it quickly in an array, and use the connection string to connect to the node.
 * 
 */





#include "client.h"
#include "hash.h"


typedef struct {
	hash_t nodehash;
	
	struct {
		char *name;
		char *connectinfo;
		char *remote_addr;
	} details;
	
	client_t *client;
	struct event *connect_event;
	struct event *loadlevel_event;
	struct event *wait_event;
	struct event *shutdown_event;
	int connect_attempts;
} node_t;


void node_connect_all(void);
void node_detach_client(node_t *node);
void node_retry(node_t *node);
node_t * node_find(char *connect_info);
node_t * node_add(client_t *client, char *connect_info);
void node_shutdown(node_t *node);


int node_active_inc(void);
int node_active_dec(void);
int node_active_count(void);

void nodes_dump(void);

char * node_getname(node_t *node);


// if this header is not being included by node.c, then we set the externs.
#ifndef __NODE_C
// 	extern node_t **_nodes;
	extern int _node_count;
// 	extern int _active_nodes;
#endif


#endif
