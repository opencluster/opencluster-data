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
	
	// if this bucket is not hosted here, then source is the server that it is hosted.
	// NULL if it is hosted here.
	node_t *source_node;
	
	// next node in the chain to send data to.
	node_t *backup_node;
	
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


typedef struct {
	// string of the primary node for this hashmask
	char *primary;
	
	// string of the secondary node for this hashmask.
	char *secondary;
} hashmask_t;







value_t * buckets_get_value(hash_t map_hash, hash_t key_hash);
int buckets_store_value(hash_t map_hash, hash_t key_hash, int expires, value_t *value);
void buckets_split_mask(hash_t mask);
void buckets_init(void);
int buckets_store_keyvalue_int(hash_t key_hash, long long int_key);
int buckets_store_keyvalue_str(hash_t key_hash, int length, char *name);

bucket_t * bucket_new(hash_t hash);
void bucket_shutdown(bucket_t *bucket);

int buckets_nobackup_count(void);
void bucket_destroy_contents(bucket_t *bucket);

const char * buckets_get_primary(hash_t key_hash);

void buckets_dump(void);
void hashmasks_dump(void);

void hashmask_switch(hash_t hash);


#ifndef __BUCKET_C
	extern bucket_t ** _buckets;
	extern int _migrate_sync;
	extern int _primary_buckets;
	extern int _secondary_buckets;
	extern hashmask_t ** _hashmasks;
	extern int _nobackup_buckets;
	extern hash_t _mask;
	extern int _bucket_transfer;
#endif



#endif

