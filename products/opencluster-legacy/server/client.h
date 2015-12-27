// client.h

#ifndef __CLIENT_H
#define __CLIENT_H

#include "event-compat.h"
#include "hash.h"
#include "header.h"
#include "payload.h"




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
	
	int timeout_limit;
	int timeout;
	int tries;
	
	int pending;
	
	int closing;

	void *transfer_bucket;
} client_t;

void clients_init(struct event_base *evbase);

client_t * client_new(void);
void client_free(client_t *client);
void client_accept(client_t *client, evutil_socket_t handle, struct sockaddr *address, int socklen);
void client_send_message(PAYLOAD payload_id);
void client_send_reply(client_t *client, header_t *header, short code, PAYLOAD payload_id);
void client_attach_node(client_t *client, void *node, int fd);
void client_shutdown(client_t *client);
void client_closing(client_t *client);
void client_fail(client_t *client);

void clients_dump(void);

void client_add_cmd(int cmd, void *fn);
void client_add_response(int cmd, int code, void *fn);
void client_add_special(int code, void *fn);

void client_update_hashmasks(hash_t mask, hash_t hashmask, int level);


void clients_init_commands(int max);
void clients_cleanup(void);

int client_count(void);

void clients_shutdown(void);







#endif