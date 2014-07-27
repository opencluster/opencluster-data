// bucket_data.c

#include "bucket_data.h"
#include "bucket.h"
#include "client.h"
#include "constants.h"
#include "hash.h"
#include "item.h"
#include "logging.h"
#include "push.h"
#include "seconds.h"
#include "stats.h"

#include <assert.h>
#include <stdlib.h>
#include <string.h>



typedef struct {
	hash_t search_hash;
	hash_t search_mask;
	maplist_t *map;
	item_t *item;
	int items_count;
	int limit;
	client_t *client;
} trav_t;


// the number of items that have been sent to the transfer client, but have not been ack'd yet.  
// Since only one bucket can be migrating at a time, this should be only in use by one bucket at a 
// time.  No need to keep seperate values per bucket.
int _in_transit = 0;




int data_in_transit(void)
{
	assert(_in_transit >= 0);
	return(_in_transit);
}


void data_in_transit_dec(void)
{
	assert(_in_transit > 0);
	_in_transit --;
}

static gint key_compare_fn(gconstpointer a, gconstpointer b)
{
	const register hash_t *aa, *bb;
	
	aa = a;
	bb = b;
	
	return((*aa) - (*bb));
}



bucket_data_t * data_new(hash_t mask, hash_t hashmask)
{
	bucket_data_t *data;
	
	assert(mask > 0);
	assert(hashmask <= mask);
	assert(hashmask >= 0);
		
	data = malloc(sizeof(bucket_data_t));
	assert(data);
	data->ref = 1;
	data->tree = g_tree_new(key_compare_fn);
	assert(data->tree);
	data->next = NULL;
	
	data->keyvalues = g_tree_new(key_compare_fn);
	assert(data->keyvalues);
	
	data->item_count = 0;
	data->data_size = 0;
	
	assert(mask > 0);
	assert(hashmask >= 0 && hashmask <= mask);
	
	data->mask = mask;
	data->hashmask = hashmask;
	
	return(data);
}



gboolean traverse_map_fn(gpointer p_key, gpointer p_value, void *p_data)
{
	trav_t *data = p_data;
	
	assert(p_key);
	assert(p_value);
	assert(p_data);
	assert(data->map);
	assert(data->item == NULL);
	assert(data->search_mask > 0);

	data->item = p_value;
	return(TRUE);
}




gboolean traverse_hash_fn(gpointer p_key, gpointer p_value, void *p_data)
{
	trav_t *data = p_data;
	hash_t *key = p_key;
	
	assert(p_key);
	assert(p_value);
	assert(p_data);
	assert(data->map == NULL);
	assert(data->search_mask > 0);
	
	if ((*key & data->search_mask) == data->search_hash) {
		data->map = p_value;
		return(TRUE);
	}
	else {
		// this item isn't one that we care about.
		return(FALSE);
	}
}

void data_destroy(bucket_data_t *data, hash_t mask, hash_t hashmask) 
{
	int total;
	trav_t trav;
	int finished = 0;
	bucket_data_t *current;
	
	assert(mask > 0);
	assert(hashmask >= 0);
	assert(hashmask <= mask);

	trav.search_hash = hashmask;
	trav.search_mask = mask;
	
	assert(data);
	assert(hashmask <= trav.search_mask);
	
	// while going through the tree, cannot modify the tree, so we will continuously go through the 
	// list, getting an item one at a time, clearing it and removing it from the tree.  Continue the 
	// process until there is no more items in the tree.
	current = data;
	assert(current);
	while (current) {
		
		assert(current->tree);

		total = g_tree_nnodes(current->tree);
		assert(total >= 0);
		if (total == 0) { finished = 1; }
		
		finished = 0;
		while (finished == 0) {
			trav.map = NULL;
			g_tree_foreach(current->tree, traverse_hash_fn, &trav);
			if (trav.map == NULL) {
				// there are no more hash items.  SO we finish out of the loop.
				finished ++;
				assert(finished > 0);
			}
			else {
				assert(finished == 0);
				assert(trav.map->mapstree);
				
				while (finished == 0) {
					trav.item = NULL;
					g_tree_foreach(trav.map->mapstree, traverse_map_fn, &trav);
					if (trav.item == NULL)  {
						// didn't find any more map entries, so we can continue
						finished ++;
						assert(finished != 0);
					}
					else {
						assert(trav.item->item_key == trav.map->item_key);
						
						// now remove the reference from the binary tree.
						gboolean found = g_tree_remove(trav.map->mapstree, &trav.item->map_key);
						assert(found == TRUE);
						
						item_destroy(trav.item);
						trav.item = NULL;
					}
				}
				
				assert(trav.item == NULL);
				
				// we need to reset 'finished' to zero, because the outer loop is using it too.
				finished = 0;
				
				total = g_tree_nnodes(trav.map->mapstree);
				assert(total >= 0);
				if (total > 0) {
					g_tree_destroy(trav.map->mapstree);
					trav.map->mapstree = NULL;
					
				}
				
				g_tree_remove(data->tree, &trav.map->item_key);
				free(trav.map);
			}
		}
	
		current = current->next;
	}
}

void data_free(bucket_data_t *data)
{
	assert(data);
	assert(data->ref == 0);
	assert(data->tree);
	assert(data->next == NULL);
	assert(g_tree_nnodes(data->tree) == 0);
	g_tree_destroy(data->tree);
	data->tree = NULL;
	
	g_tree_destroy(data->keyvalues);
	data->keyvalues = NULL;
	
	free(data);
}


static keyvalue_t * find_keyvalue(hash_t key_hash, bucket_data_t *data)
{
	keyvalue_t *entry = NULL;
	bucket_data_t *current;
	
	assert(data);

	current = data;
	assert(current->tree);

	while (current) {
		
		logger(LOG_DEBUG, "find_keyvalue: Looking for %#llx in container %#llx/%#llx", key_hash, current->mask, current->hashmask);
		
		entry = g_tree_lookup(current->tree, &key_hash);
		if (entry == NULL) {
			// list is not in this one, so we should try the next one.
			current = current->next;
		}
		else {
			// we found the item.
			
			assert(entry->key_hash == key_hash);
			if (current != data) {
				// we found the item in one of the sub-chains, so we need to move it to the top.
				g_tree_remove(current->tree, &key_hash);
				g_tree_insert(data->tree, &key_hash, entry);
			}
			
			// since the item was found, it shouldn't be anywhere else, so we can stop going 
			// through the trees.
			current = NULL;
			
			// since we MUST be returning a list at this point, make sure we haven't lost it.
			assert(entry);
		}
	}
	
	return (entry);
}




// will look through the data-tree for this item hash-key.  If it finds the item in a sub-chain of 
// trees, it will move it to the one in 'data', in other words, it will move it to the front.   It 
// will return the maplist_t object so that it can be accessed directly.  It will return NULL if it 
// could not find the hash in the entire chain, therefore a new entry should be created, but is not 
// done for you.
static maplist_t * find_maplist(hash_t key_hash, bucket_data_t *data)
{
	maplist_t *list = NULL;
	bucket_data_t *current;
	
	assert(data);

	current = data;
	assert(current->tree);

	while (current) {
		
		logger(LOG_DEBUG, "find_maplist: Looking for %#llx in container %#llx/%#llx", key_hash, current->mask, current->hashmask);
		
		list = g_tree_lookup(current->tree, &key_hash);
		if (list == NULL) {
			// list is not in this one, so we should try the next one.
			current = current->next;
		}
		else {
			// we found the item.
			
			assert(list->mapstree);
			if (current != data) {
				// we found the item in one of the sub-chains, so we need to move it to the top.
				g_tree_remove(current->tree, &key_hash);
				g_tree_insert(data->tree, &key_hash, list);
			}
			
			// since the item was found, it shouldn't be anywhere else, so we can stop going 
			// through the trees.
			current = NULL;
			
			// since we MUST be returning a list at this point, make sure we haven't lost it.
			assert(list);
		}
	}
	
	return (list);
}



value_t * data_get_value(hash_t map_hash, hash_t key_hash, bucket_data_t *ddata) 
{
	maplist_t *list;
	item_t *item;
	value_t *value = NULL;

	assert(ddata);
	assert(ddata->tree);

	logger(LOG_DEBUG, "data_get_value: Looking up [%#llx/%#llx].", map_hash, key_hash);
	
	list = find_maplist(key_hash, ddata);
	if (list) {

		logger(LOG_DEBUG, "data_get_value: key found. [%#llx/%#llx].", map_hash, key_hash);
		
		// search the list of maps for a match.
		assert(list->mapstree);
		item = g_tree_lookup(list->mapstree, &map_hash);
		if (item) {
			// item is found, return with the data.
			assert(item->value);

			logger(LOG_DEBUG, "data_get_value: key and map found. [%#llx/%#llx].", map_hash, key_hash);
			
			if (item->expires > 0 && item->expires < seconds_get()) {
				// item has expired.   We need to remove it from the map list.
				assert(value == NULL);
				assert(0);
			}
			else {
				value = item->value;
				assert(value);
			}
		}
	}
	
	return(value);
}



static maplist_t * list_new(hash_t key_hash)
{
	maplist_t *list;
	
	list = calloc(1, sizeof(maplist_t));
	assert(list);
	list->item_key = key_hash;
	list->mapstree = g_tree_new(key_compare_fn);
	assert(list->mapstree);
	assert(list->migrate == 0);

	return(list);
}




// the control of 'value' is given to this function.
// NOTE: value is controlled by the tree after this function call.
// NOTE: name is controlled by the tree after this function call.
void data_set_value(
	hash_t map_hash, hash_t key_hash, bucket_data_t *ddata,  
	value_t *value, int expires, client_t *backup_client) 
{
	maplist_t *list;
	item_t *item = NULL;
	
	assert(ddata);
	assert(value);
	assert(expires >= 0);

	assert(ddata->tree);
	
	// first we are going to store the value in the primary 'bucket_data'.  If it exists, we will 
	// update the value in there.  If it doesn't exist we will add it, and when we've done that, 
	// then we will go through the rest of the chain to find this same entry, if it exists, then 
	// search the btree in the bucket for this key.
	list = find_maplist(key_hash, ddata);
	if (list == NULL) {

		logger(LOG_DEBUG, "data_set_value: key %#llx not found.  Creating new one.", key_hash);
		
		list = list_new(key_hash);
		g_tree_insert(ddata->tree, &list->item_key, list);
	}
	else {
		// since we found a maplist for this hash, we should look to see if this map is in there.  
		// We only do this if we found the list, because we know a newly created one wont have it, 
		// so no need to waste time doing a lookup when we know it wont be in there.  That is why we 
		// have a slightly convoluted logic structure here.
		
		logger(LOG_DEBUG, "data_set_value: key %#llx found.", key_hash);
		
		// Now we need to look at the maps to see if the complete entry is here.
		assert(list->mapstree);
		item = g_tree_lookup(list->mapstree, &map_hash);
		if (item) {
			// the item was found, so now we need to update the value with the one we have.

			logger(LOG_DEBUG, "data_set_value: item [%#llx/%#llx] found, updating value.", map_hash, key_hash);
			
			assert(item->value);
			value_move(item->value, value);
			
			// ** PERF: value objects should be put back in a pool to avoid having to alloc/free all the time.
			free(value);
			value = NULL;
			
			item->expires = expires == 0 ? 0 : seconds_get() + expires;
		}
	}
	
	assert(list);

	if (item == NULL) {
		// item was not found, so create a new one.

		logger(LOG_DEBUG, "data_set_value: item [%#llx/%#llx] NOT found, creating a new one.", map_hash, key_hash);
		
		item = calloc(1, sizeof(item_t));
		assert(item);

		item->item_key = key_hash;
		item->map_key = map_hash;
		item->value = value;
		item->expires = expires == 0 ? 0 : seconds_get() + expires;
		item->migrate = 0;
		
		g_tree_insert(list->mapstree, &item->map_key, item);
	}
	
	// by this point, we should have either found an existing item that matches, or created a new one.
	assert(item);

	// if we have a backup node connected, we need to send the item details to it.
	if (backup_client) {
		push_sync_item(backup_client, item);
	}
}




// traverse function for g_tree_foreach search for hashs that have not been migrated.
static gboolean migrate_map_fn(gpointer p_key, gpointer p_value, void *p_data)
{
	trav_t *data = p_data;
	hash_t *key = p_key;
	item_t *item;
	int sync;
	
	assert(p_key);
	assert(p_value);
	assert(p_data);
	assert(data->map == NULL);
	assert(data->client);
	
	assert(data->limit > 0);
	assert(data->items_count < data->limit);
	assert(data->items_count >= 0);
	
	// get the current sync value for all the buckets.  This is used so that we can find the items that have not yet been migrated.
	sync = buckets_get_migrate_sync();
	assert(sync > 0);
	
	item = p_value;
	assert(item->migrate <= sync);
	if (item->migrate < sync) {
		logger(LOG_DEBUG, "migrate: map %#llx ready to migrate.  Sending now.", *key);
		push_sync_item(data->client, item);
		_in_transit ++;
		assert(_in_transit <= TRANSIT_MAX);
		data->items_count ++;
		assert(data->items_count <= data->limit);
		item->migrate = sync;
		logger(LOG_DEBUG, "migrate: items in list: %d", data->items_count);
	}
	
	if (data->items_count < data->limit) {
		// returning TRUE will exit the traversal.
		return(TRUE);
	}
	else {
		// if we have more items we can get, then we return FALSE.
		return(FALSE);
	}
}



// traverse function for g_tree_foreach search for hashs that have not been migrated.
static gboolean migrate_hash_fn(gpointer p_key, gpointer p_value, void *p_data)
{
	trav_t *data = p_data;
	hash_t *key = p_key;
	maplist_t *map;	
	
	assert(p_key);
	assert(p_value);
	assert(p_data);
	assert(data->map == NULL);
	assert(data->client);
	assert(data->search_mask > 0);
	
	assert(data->limit > 0);
	assert(data->items_count < data->limit);

	int sync = buckets_get_migrate_sync();
	assert(sync > 0);
	
	if ((*key & data->search_mask) == data->search_hash) {
		
		map = p_value;
		assert(sync > 0);
		assert(map->migrate >= 0);
		
// 		logger(LOG_DEBUG, "migrate: found hash: %#llx, migrate=%d", *key, map->migrate);
	
		assert(map->migrate <= sync);
		if (map->migrate < sync) {
			// found one.
			// now we need to search the inner tree
			
			logger(LOG_DEBUG, "migrate: hash %#llx is ready to migrate.", *key);
			
			g_tree_foreach(map->mapstree, migrate_map_fn, data);
			
			if (data->items_count < data->limit) {
				// the traversal returned while there are still item slots available.  This must 
				// mean that there are no more items in this map that need to be migrated, so we 
				// will mark the map with the current migrate sync value.
				map->migrate = sync;
				
				logger(LOG_DEBUG,
						"migrate: hash %#llx seems to have all its maps migrated, "
						"so we mark the hash as complete.",
						*key
  						);
			}
		}
	}

	// if we haven't yet reached our limit, then we return FALSE.
	if (data->items_count < data->limit) {
		return(FALSE);
	}
	else {
		// returning TRUE will exit the traversal
		return(TRUE);		
	}
}




// go through the trees in the data to find items for this hashkey that need to be migrated.  Uses 
// some global variables for extra information.
int data_migrate_items(client_t *client, bucket_data_t *data, hash_t hashmask, int limit)
{
	bucket_data_t *current;
	trav_t trav = {
		.search_hash=0, 
		.map=NULL, 
		.item=NULL,
		.items_count=0
	};
	
	assert(data);
	assert(limit > 0);
	
	assert(client);
	trav.client = client;

	assert(trav.items_count == 0);
	trav.limit = limit;
	trav.search_hash = hashmask;
	current = data;

	logger(LOG_DEBUG, "About to search the data for hashmask:%#llx, limit:%d", 
			   hashmask, limit);
	
	while (current && trav.items_count < limit) {

		logger(LOG_DEBUG, "Searching container: %#llx/%#llx", 
				   current->mask, current->hashmask);
		
		assert(current->tree);
		assert(trav.map == NULL);
			
		// go through the 'hash' tree pulling out the items we need.  As we pull the items out, we 
		// will mark things as complete.  the items array then needs to be processed completely or 
		// items will be lost.
		g_tree_foreach(current->tree, migrate_hash_fn, &trav);
		
		current = current->next;
	}
	logger(LOG_DEBUG, "found %d items", trav.items_count);
	
	assert(trav.items_count >= 0);
	return(trav.items_count);
}



// 'keyvalue' is a pointer that is controlled by the keyvalue tree.
void data_set_keyvalue_str(hash_t key_hash, bucket_data_t *data, int len, char *keyvalue)
{
	keyvalue_t *entry;
	
	assert(data);
	assert(data->keyvalues);
	assert(len > 0);
	assert(keyvalue);
	
	// first we are going to store the value in the primary 'bucket_data'.  If it exists, we will 
	// update the value in there.  If it doesn't exist we will add it, and when we've done that, 
	// then we will go through the rest of the chain to find this same entry, if it exists, then 
	
	entry = find_keyvalue(key_hash, data);
	if (entry == NULL) {
		// that hash was not found anywhere in the chain, so we need to create an entry for it.

		entry = malloc(sizeof(keyvalue_t));
		entry->key_hash = key_hash;
		entry->type = KEYVALUE_TYPE_STR;
		entry->keyvalue.s.length = len;
		entry->keyvalue.s.data = keyvalue;
		
		g_tree_insert(data->keyvalues, &entry->key_hash, entry);
	}
	else {

		assert(entry);

		// if the existing entry is a string, then this string should be the same otherwise we have a collision.
		if (entry->type == KEYVALUE_TYPE_STR) {
			if (len != entry->keyvalue.s.length || memcmp(keyvalue, entry->keyvalue.s.data, len) != 0) {
				logger(LOG_INFO, "Keyvalue Collision:%#llx", key_hash);
			}
		}
		else {
			// the existing entry is an integer, we've had a collision.
			logger(LOG_INFO, "Keyvalue Type Collision:%#llx", key_hash);
			assert(0);
		}
		
		// since we are in control of 'keyvalue' at this point. we need to free it.
		free(keyvalue);
	}
}


void data_set_keyvalue_int(hash_t key_hash, bucket_data_t *data, long long keyvalue_int)
{
	keyvalue_t *entry;
	
	assert(data);
	assert(data->keyvalues);
	
	// first we are going to store the value in the primary 'bucket_data'.  If it exists, we will 
	// update the value in there.  If it doesn't exist we will add it, and when we've done that, 
	// then we will go through the rest of the chain to find this same entry, if it exists, then 
	
	entry = find_keyvalue(key_hash, data);
	if (entry == NULL) {
		// that hash was not found anywhere in the chain, so we need to create an entry for it.

		entry = malloc(sizeof(keyvalue_t));
		entry->key_hash = key_hash;
		entry->type = KEYVALUE_TYPE_INT;
		entry->keyvalue.i = keyvalue_int;
		
		g_tree_insert(data->keyvalues, &entry->key_hash, entry);
	}
	else {

		assert(entry);

		// if the existing entry is an integer, then this string should be the same otherwise we have a collision.
		if (entry->type == KEYVALUE_TYPE_INT) {
			if (entry->keyvalue.i != keyvalue_int) {
				logger(LOG_INFO, "Keyvalue Collision:%#llx", key_hash);
			}
		}
		else {
			// the existing entry is a string, we've had a collision.
			logger(LOG_INFO, "Keyvalue Type Collision:%#llx", key_hash);
			assert(0);
		}
	}
}



// not really much we need to do about this, because we marked the data beforehand, but for debug 
// purposes, we will check it out.
void data_migrated(bucket_data_t *data, hash_t map_hash, hash_t key_hash)
{
#ifndef NDEBUG
	maplist_t *list;
	item_t *item;

	assert(data);
	assert(data->tree);

	list = find_maplist(key_hash, data);
	if (list) {
			
		// search the list of maps for a match.
		assert(list->mapstree);
		item = g_tree_lookup(list->mapstree, &map_hash);
		if (item) {
			// item is found, return with the data.
			assert(item->value);
			assert(item->migrate == buckets_get_migrate_sync() || item->migrate == 0);
		}
	}
#endif
}




void data_dump(bucket_data_t *data)
{
	stat_dumpstr("      Data Items: %ld", data->item_count);
	stat_dumpstr("      Data Bytes: %ld", data->data_size);
}
