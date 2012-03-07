// node.h


#ifndef __NODE_H
#define __NODE_H

#include "client.h"


typedef struct {
	char *name;
	client_t *client;
	struct event *connect_event;
	struct event *loadlevel_event;
	struct event *wait_event;
	struct event *shutdown_event;
	int connect_attempts;
} node_t;



node_t * node_new(const char *name);
void node_connect_all(void);
void node_detach_client(node_t *node);
void node_retry(node_t *node);
node_t * node_find(char *name);
node_t * node_add(client_t *client, char *name);

void node_loadlevel_handler(int fd, short int flags, void *arg);

int node_active_inc(void);
int node_active_dec(void);
int node_active_count(void);




// if this header is not being included by node.c, then we set the externs.
#ifndef __NODE_C
	extern node_t **_nodes;
	extern int _node_count;
	extern int _active_nodes;
#endif


#endif