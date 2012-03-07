// process.h

#ifndef __PROCESS_H
#define __PROCESS_H

#include "client.h"
#include "header.h"


void process_ack(client_t *client, header_t *header);
void process_loadlevels(client_t *client, header_t *header, void *ptr);
void process_unknown(client_t *client, header_t *header);
void process_control_bucket_complete(client_t *client, header_t *header, void *ptr);
void process_accept_bucket(client_t *client, header_t *header, void *ptr);


#endif