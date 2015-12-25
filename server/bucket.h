// bucket.h

#ifndef __BUCKET_H
#define __BUCKET_H

#include "bucket_data.h"
#include "hash.h"
#include "node.h"
#include "value.h"

#include "event-compat.h"
#include <glib.h>





typedef struct {

	hash_t hashmask;
	
	//  0 indicates primary
	//  1 or more indicate which level of indirection the backups go.
	// -1 indicates that the bucket is not hosted here.
	int level;	
	
	// tree that the maps containing the actual data is stored.
	bucket_data_t *data;
	
	node_t *primary_node;		// NULL for hosted locally.
	node_t *secondary_node;
	
	// if this bucket is not hosted here, then source is the server that it is hosted.
	// NULL if it is hosted here.
	node_t *source_node;	
	node_t *backup_node;  	// next node in the chain to send data to.

	// special 'logger' nodes can be added to the cluster.  They do not serve data, but instead 
	// record changes to a transaction log which can be used to recover data.
	node_t *logging_node;

	// client we are transferring this bucket to.  if this is not null, then a transfer is in progress.
	client_t *transfer_client;
	int transfer_mode_special;
	
	struct event *shutdown_event;
	struct event *transfer_event;

	// an event will fire every second to pull out an item from the tree and put it in its proper 
	// tree.  If there are 10,000 items in the tree, this could take a while.  However, every time a 
	// lookup on an item is made, and it is found in one of these trees, it is removed and put in 
	// the proper tree so active items should get moved pretty quickly.
	struct event *oldbucket_event;
	
	// indicate that the bucket is attempting to promote a bucket on another node.  We keep the 
	// bucket until it is confirmed that the other node has promoted.
	enum {
		NOT_PROMOTING=0,
		PROMOTING=1
	} promoting;

} bucket_t;


value_t * buckets_get_value(hash_t map_hash, hash_t key_hash);
int buckets_store_value(hash_t map_hash, hash_t key_hash, int expires, value_t *value);
void buckets_split_mask(hash_t current_mask, hash_t new_mask);
void buckets_init(hash_t mask, struct event_base *evbase);
int buckets_store_keyvalue(hash_t key_hash, char *name, int expires);
const char * buckets_get_keyvalue(hash_t hash_hash);


bucket_t * bucket_new(hash_t hash);
int buckets_shutdown(void);

int buckets_nobackup_count(void);
void bucket_destroy_contents(bucket_t *bucket);

node_t * buckets_get_primary_node(hash_t key_hash);
int buckets_get_migrate_sync(void);

void buckets_dump(void);

int buckets_get_primary_count(void);
int buckets_get_secondary_count(void);

int buckets_transferring(void);
int buckets_send_bucket(client_t *client, hash_t mask, hash_t hashmask);
int buckets_accept_bucket(client_t *client, hash_t mask, hash_t hashmask);
void buckets_control_bucket(client_t *client, hash_t mask, hash_t key_hash, int level);

void buckets_hashmasks_update(node_t *node, hash_t hashmask, int level);

hash_t buckets_mask(void);
bucket_t * buckets_find_switchable(node_t *node);
bucket_t * buckets_nobackup_bucket(void);

void buckets_finalize_migration(client_t *client, hash_t hashmask, int level, conninfo_t *conninfo);
bucket_t * buckets_check_loadlevels(client_t *client, int primary, int backups);

void buckets_set_transferring(bucket_t *bucket, client_t *client);
void buckets_clear_transferring(bucket_t *bucket);
int buckets_transfer_items(client_t *client);
bucket_t *buckets_current_transfer(void);



#endif

