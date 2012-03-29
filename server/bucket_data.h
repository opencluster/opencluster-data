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
	
	// the map tree.
	GTree *tree;

	// if we already have an 'oldtree' and we need to split again, then we put the existing old tree 
	// inside this new one.   When the data is eventually moved out of it, it can be deleted.
	struct __bucket_data_t *next;

} bucket_data_t;


typedef struct {
	GTree *mapstree;
	hash_t item_key;
	int migrate;		// indicates that something inside this maps tree has not been migrated yet.
	int migrate_name;	// indicates that the name of the item has not been migrated (if less than _sync_migrate)
	char *name;			// user specified name of the item, that is used to generate the hash-key.  May be NULL.
	int int_key;		// user specified integer of the key, if specified.  Only valid if 'name' is NULL.
} maplist_t;



bucket_data_t * data_new(hash_t hashmask);
void data_free(bucket_data_t *data);
void data_destroy(bucket_data_t *data, hash_t hash);

value_t * data_get_value(int map_hash, int key_hash, bucket_data_t *ddata);
void data_set_value(int map_hash, int key_hash, bucket_data_t *ddata, char *name, int name_int, value_t *value, int expires, client_t *backup_client);
void data_set_name(hash_t key_hash, bucket_data_t *data, char *name, int name_int);
int data_migrate_items(client_t *client, bucket_data_t *data, hash_t hash, int limit);
int data_in_transit(void);
void data_in_transit_dec(void);
void data_migrated(bucket_data_t *data, hash_t map, hash_t hash);



#endif