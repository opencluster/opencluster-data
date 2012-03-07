// push.h

#ifndef __PUSH_H
#define __PUSH_H

#include "bucket.h"
#include "client.h"


void push_ping(client_t *client);
void push_shuttingdown(client_t *client);
void push_hashmask_update(bucket_t *bucket);
void push_hashmasks(client_t *client);
void push_serverlist(client_t *client);
void push_serverhello(client_t *client);
void push_loadlevels(client_t *client);
void push_accept_bucket(client_t *client, hash_t key);
void push_promote(client_t *client, hash_t hash);
void push_control_bucket(client_t *client, bucket_t *bucket, int level);


#endif

