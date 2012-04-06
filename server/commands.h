// commands.h

#ifndef __COMMANDS_H
#define __COMMANDS_H

#include "client.h"
#include "header.h"

void cmd_get_int(client_t *client, header_t *header, char *payload);
void cmd_get_str(client_t *client, header_t *header, char *payload);
void cmd_set_str(client_t *client, header_t *header, char *payload);
void cmd_loadlevels(client_t *client, header_t *header);
void cmd_accept_bucket(client_t *client, header_t *header, char *payload);
void cmd_set_int(client_t *client, header_t *header, char *payload);
void cmd_ping(client_t *client, header_t *header);
void cmd_serverhello(client_t *client, header_t *header, char *payload);
void cmd_hashmask(client_t *client, header_t *header, char *payload);
void cmd_control_bucket(client_t *client, header_t *header, char *payload);
void cmd_hello(client_t *client, header_t *header);
void cmd_sync_string(client_t *client, header_t *header, char *payload);
void cmd_sync_int(client_t *client, header_t *header, char *payload);
void cmd_sync_name(client_t *client, header_t *header, char *payload);
void cmd_goodbye(client_t *client, header_t *header);



#endif

