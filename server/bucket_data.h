#ifndef __BUCKET_DATA_H
#define __BUCKET_DATA_H

#include "hash.h"
#include "item.h"
#include "value.h"

#include <glib.h>



typedef struct __bucket_data_t {
	
	// keep track of the number of buckets that are referencing this object.  When it gets to zero, 
	// it can be cleaned up and deallocated.
	int ref;
	
	// the map tree.
	GTree *tree;

	// if we already have an 'oldtree' and we need to split again, then we put the existing old tree 
	// inside this new one.   When the data is eventually moved out of it, it can be deleted.
	struct __bucket_data_t *next;

} bucket_data_t;



bucket_data_t * data_new(void);
void data_free(bucket_data_t *data);
void data_destroy(bucket_data_t *data, hash_t hash);

value_t * data_get_value(int map_hash, int key_hash, bucket_data_t *ddata);
int data_set_value(int map_hash, int key_hash, bucket_data_t *ddata, char *name, value_t *value, int expires);
item_t ** data_get_migrate_items(bucket_data_t *data, hash_t hash, int limit);



#endif