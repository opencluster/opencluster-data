// push.h

#ifndef __PUSH_H
#define __PUSH_H

#include "client.h"
#include "item.h"


void push_ping(client_t *client);
void push_shuttingdown(client_t *client);
void push_hashmask(client_t *client, hash_t mask, hash_t hashmask, int level);
void push_serverlist(client_t *client);
void push_serverhello(client_t *client, const char *conninfo_str, const char *server_auth);
void push_loadlevels(client_t *client);
void push_accept_bucket(client_t *client, hash_t mask, hash_t hashmask);
void push_promote(client_t *client, hash_t hash);
void push_control_bucket(client_t *client, hash_t mask, hash_t hashmask, int level);
void push_sync_item(client_t *client, item_t *item);
void push_sync_keyvalue(client_t *client, hash_t keyhash, int length, char *keyvalue);
void push_sync_keyvalue_int(client_t *client, hash_t key, long long int_key);
void push_finalise_migration(client_t *client, hash_t mask, hash_t hashmask, const char *conninfo, int level);
void push_all_newserver(char *name, client_t *source_client);


#endif

