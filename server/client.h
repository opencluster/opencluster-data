// client.h

#ifndef __CLIENT_H
#define __CLIENT_H

#include "event-compat.h"
#include "header.h"

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
		long long total;
	} in, out;
	
	int nextid;

	int timeout_limit;
	int timeout;
	int tries;
	
	int closing;

	void *transfer_bucket;

} client_t;



client_t * client_new(void);
void client_free(client_t *client);
void client_accept(client_t *client, evutil_socket_t handle, struct sockaddr *address, int socklen);
void client_send_message(client_t *client, header_t *header, short command, int length, void *payload);
void client_attach_node(client_t *client, void *node, int fd);
void client_shutdown(client_t *client);
void client_closing(client_t *client);

void clients_dump(void);

void client_add_cmd(int cmd, void *fn);
void client_init_commands(int max);



// functions that are used to pull data out of the communication channel.  Since both 'commands' and 
// 'process' needs to use this, we will put them in 'client' because they are both subsets of the client operation.
int data_int(char **data);
long long data_long(char **data);
char * data_string_copy(char **data);
char * data_string(char **data, int *length);



#ifndef __CLIENT_C
	extern client_t **_clients;
	extern int _client_count;
#endif


#endif