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

	hash_t hash;
	
	//  0 indicates primary
	//  1 or more indicate which level of indirection the backups go.
	int level;	
	
	// tree that the maps containing the actual data is stored.
	bucket_data_t *data;
	
	// if this bucket is not hosted here, then this is the server that it is hosted.
	// NULL if it is hosted here.
	node_t *target_node;
	node_t *backup_node;
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


} bucket_t;


typedef struct {
	int local;
	char *primary;
	char *secondary;
} hashmask_t;







value_t * buckets_get_value(int map_hash, int key_hash);
int buckets_store_value(int map_hash, int key_hash, char *name, int name_int, int expires, value_t *value);
void buckets_split_mask(int mask);
void buckets_init(void);
int buckets_store_name(hash_t key_hash, char *name, int int_key);

bucket_t * bucket_new(hash_t hash);
void bucket_shutdown(bucket_t *bucket);

int buckets_nobackup_count(void);
void bucket_destroy(bucket_t *bucket);


void buckets_dump(void);
void hashmasks_dump(void);


#ifndef __BUCKET_C
	extern bucket_t ** _buckets;
	extern int _migrate_sync;
	extern int _primary_buckets;
	extern int _secondary_buckets;
	extern hashmask_t ** _hashmasks;
#endif



#endif

