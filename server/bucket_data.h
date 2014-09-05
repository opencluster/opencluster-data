#ifndef __BUCKET_DATA_H
#define __BUCKET_DATA_H

#include "client.h"
#include "hash.h"
#include "item.h"
#include "value.h"

#include <glib.h>




typedef struct __bucket_data_t {

	hash_t mask;
	hash_t hashmask;
	
	// keep track of the number of buckets that are referencing this object.  When it gets to zero, 
	// it can be cleaned up and deallocated.
	int ref;
	
	// the value tree.
	GTree *tree;
	
	// if we already have an 'oldtree' and we need to split again, then we put the existing old tree 
	// inside this new one.   When the data is eventually moved out of it, it can be deleted.
	struct __bucket_data_t *next;
	
	long long item_count;
	long long data_size;

} bucket_data_t;


typedef struct {
	GTree *mapstree;
	hash_t item_key;
	char *keyvalue;
	long keyvalue_expires;
	
	// indicates that something inside this maps tree has not been migrated yet.  
	// keyvalue is last to be migrated, so if nothing else needs to go, keyvalue does.
	int migrate;
} maplist_t;



bucket_data_t * data_new(hash_t mask, hash_t hashmask);
void data_free(bucket_data_t *data);
void data_destroy(bucket_data_t *data, hash_t mask, hash_t hashmask);

value_t * data_get_value(hash_t map_hash, hash_t key_hash, bucket_data_t *ddata);
void data_set_value(hash_t map_hash, hash_t key_hash, bucket_data_t *ddata, value_t *value, int expires, client_t *backup_client);
const char * data_get_keyvalue(hash_t key_hash, bucket_data_t *data);
void data_set_keyvalue(hash_t key_hash, bucket_data_t *data, char *keyvalue, int expires);
int data_migrate_items(client_t *client, bucket_data_t *data, hash_t hash, int limit);
int data_in_transit(void);
void data_in_transit_dec(void);
void data_migrated(bucket_data_t *data, hash_t map, hash_t hash);

void data_dump(bucket_data_t *data);


#endif