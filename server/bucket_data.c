// bucket_data.c

#include "bucket_data.h"
#include "bucket.h"
#include "client.h"
#include "constants.h"
#include "globals.h"
#include "hash.h"
#include "item.h"
#include "push.h"

#include <assert.h>
#include <stdlib.h>
#include <string.h>



typedef struct {
	hash_t search_hash;
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
	const register unsigned int *aa, *bb;
	
	aa = a;
	bb = b;
	
	return((*aa) - (*bb));
}



bucket_data_t * data_new(hash_t hashmask)
{
	bucket_data_t *data;
	
	data = malloc(sizeof(bucket_data_t));
	assert(data);
	data->ref = 1;
	data->tree = g_tree_new(key_compare_fn);
	assert(data->tree);
	data->next = NULL;
	
	assert(_mask > 0);
	assert(hashmask >= 0 && hashmask <= _mask);
	
	data->mask = _mask;
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
	
	if ((*key & _mask) == data->search_hash) {
		data->map = p_value;
		return(TRUE);
	}
	else {
		// this item isn't one that we care about.
		return(FALSE);
	}
}

void data_destroy(bucket_data_t *data, hash_t hash) 
{
	int total;
	trav_t trav;
	int finished = 0;
	bucket_data_t *current;
	
	
	assert(data);
	assert(hash <= _mask);
	
	
	// while going through the tree, cannot modify the tree, so we will continuously go through the 
	// list, getting an item one at a time, clearing it and removing it from the tree.  Continue the 
	// process until there is no more items in the tree.

	trav.search_hash = hash;
	
	
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
	
	free(data);
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
		
		if (_verbose > 4)
			printf("find_maplist: Looking in container %08X/%08X\n", current->mask, current->hashmask);
		
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



value_t * data_get_value(int map_hash, int key_hash, bucket_data_t *ddata) 
{
	maplist_t *list;
	item_t *item;
	value_t *value = NULL;

	assert(ddata);
	assert(ddata->tree);

	list = find_maplist(key_hash, ddata);
	if (list) {
			
		// search the list of maps for a match.
		assert(list->mapstree);
		item = g_tree_lookup(list->mapstree, &map_hash);
		if (item) {
			// item is found, return with the data.
			assert(item->value);
				
			if (item->expires > 0 && item->expires < _seconds) {
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
	assert(list->migrate_name == 0);
	assert(list->name == NULL);
	assert(list->int_key == 0);

	return(list);
}




// the control of 'value' is given to this function.
// NOTE: value is controlled by the tree after this function call.
// NOTE: name is controlled by the tree after this function call.
void data_set_value(int map_hash, int key_hash, bucket_data_t *ddata, char *name, int name_int, value_t *value, int expires) 
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
		
		list = list_new(key_hash);
		g_tree_insert(ddata->tree, &list->item_key, list);
		
		assert(list->name == NULL);
		list->name = name;
		list->int_key = name_int;
		name = NULL;
	}
	else {
		// since we found a maplist for this hash, we should look to see if this map is in there.  
		// We only do this if we found the list, because we know a newly created one wont have it, 
		// so no need to waste time doing a lookup when we know it wont be in there.  That is why we 
		// have a slightly convoluted logic structure here.
		
		
		// Now we need to look at the maps to see if the complete entry is here.
		assert(list->mapstree);
		item = g_tree_lookup(list->mapstree, &map_hash);
		if (item) {
			// the item was found, so now we need to update the value with the one we have.
			
			assert(item->value);
			value_move(item->value, value);
			free(value);
			value = NULL;
			
			// since we control the 'name' memory, but dont actually need it, we will free it.
			if (name) {
				free(name);
				name = NULL;
			}
			
			item->expires = expires == 0 ? 0 : _seconds + expires;
		}
	}
	
	assert(list);

	if (item == NULL) {
			
		// item was not found, so create a new one.
		item = calloc(1, sizeof(item_t));
		assert(item);

		item->item_key = key_hash;
		item->map_key = map_hash;
		item->value = value;
		item->expires = expires == 0 ? 0 : _seconds + expires;
		item->migrate = 0;
		
		g_tree_insert(list->mapstree, &item->map_key, item);
	}

	assert(name == NULL);
}




// traverse function for g_tree_foreach search for hashs that have not been migrated.
gboolean migrate_map_fn(gpointer p_key, gpointer p_value, void *p_data)
{
	trav_t *data = p_data;
	hash_t *key = p_key;
	item_t *item;
	
	assert(p_key);
	assert(p_value);
	assert(p_data);
	assert(data->map == NULL);
	assert(data->client);
	
	assert(data->limit > 0);
	assert(data->items_count < data->limit);
	assert(data->items_count >= 0);
	
// 	if (_verbose > 4) 
// 		printf("migrate: map found, %08X\n", *key);
		
	item = p_value;
	assert(_migrate_sync > 0);
	assert(item->migrate <= _migrate_sync);
	if (item->migrate < _migrate_sync) {
		
		if (_verbose > 4) 
			printf("migrate: map %08X ready to migrate.  Sending now.\n", *key);
		
		push_sync_item(data->client, item);
		_in_transit ++;
		assert(_in_transit <= TRANSIT_MAX);
		
		data->items_count ++;
		assert(data->items_count <= data->limit);
		
		item->migrate = _migrate_sync;
		
		if (_verbose > 4) 
			printf("migrate: items in list: %d\n", data->items_count);
		
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
gboolean migrate_hash_fn(gpointer p_key, gpointer p_value, void *p_data)
{
	trav_t *data = p_data;
	hash_t *key = p_key;
	maplist_t *map;	
	
	assert(p_key);
	assert(p_value);
	assert(p_data);
	assert(data->map == NULL);
	assert(data->client);
	
	assert(data->limit > 0);
	assert(data->items_count < data->limit);

	if ((*key & _mask) == data->search_hash) {
		
		map = p_value;
		assert(_migrate_sync > 0);
		assert(map->migrate >= 0);
		
		if (_verbose > 5)
			printf("migrate: found hash: %08X, migrate=%d\n", *key, map->migrate);
	
		assert(map->migrate <= _migrate_sync);
		assert(map->migrate_name <= _migrate_sync);
		
		// if we havent supplied the 'name' of this hash to the receiving server, then we need to send it.
		if (map->migrate_name < _migrate_sync) {
			push_sync_name(data->client, map->item_key, map->name, map->int_key);
			map->migrate_name = _migrate_sync;
		}
		
		if (map->migrate < _migrate_sync) {
			// found one.
			// now we need to search the inner tree
			
			if (_verbose > 4) 
				printf("migrate: hash %08X is ready to migrate.\n", *key);
			
			g_tree_foreach(map->mapstree, migrate_map_fn, data);
			
			if (data->items_count < data->limit) {
				// the traversal returned while there are still item slots available.  This must 
				// mean that there are no more items in this map that need to be migrated, so we 
				// will mark the map with the current migrate sync value.
				map->migrate = _migrate_sync;
				
				if (_verbose > 4)
					printf(
						"migrate: hash %08X seems to have all its maps migrated, so we mark the hash as complete.\n",
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
	assert(_mask > 0);
	assert(hashmask >= 0 && hashmask <= _mask);
	assert(limit > 0);
	
	assert(client);
	trav.client = client;

	assert(trav.items_count == 0);
	trav.limit = limit;
	trav.search_hash = hashmask;
	current = data;

	if (_verbose > 4) 
		printf("About to search the data for hashmask:%08X/%08X, limit:%d\n", 
			   _mask, hashmask, limit);
	
	while (current && trav.items_count < limit) {

		if (_verbose > 4)
			printf("Searching container: %08X/%08X\n", 
				   current->mask, current->hashmask);
		
		assert(current->tree);
		assert(trav.map == NULL);
			
		// go through the 'hash' tree pulling out the items we need.  As we pull the items out, we 
		// will mark things as complete.  the items array then needs to be processed completely or 
		// items will be lost.
		g_tree_foreach(current->tree, migrate_hash_fn, &trav);
		
		current = current->next;
	}
	if (_verbose > 4) printf("found %d items\n", trav.items_count);
	
	assert(trav.items_count >= 0);
	return(trav.items_count);
}







void data_set_name(hash_t key_hash, bucket_data_t *data, char *name, int name_int)
{
	maplist_t *list;
	
	assert(data);
	assert(data->tree);
	
	assert((name && name_int == 0) || name == NULL);
	
	// first we are going to store the value in the primary 'bucket_data'.  If it exists, we will 
	// update the value in there.  If it doesn't exist we will add it, and when we've done that, 
	// then we will go through the rest of the chain to find this same entry, if it exists, then 
	
	list = find_maplist(key_hash, data);
	if (list == NULL) {
		// that hash was not found anywhere in the chain, so we need to create an entry for it.

		list = list_new(key_hash);
		g_tree_insert(data->tree, &list->item_key, list);
	}

	assert(list);
	
	// now that we've got a maplist created for this hash, we need to add this map (item) to it.
	if (name) {
		if (list->name) { 
			free(list->name); 
		}
		list->name = name;
		list->int_key = 0;
		name = NULL;
	}
	else {
		assert(list->name == NULL);
		list->int_key = name_int;
	}

	assert(name == NULL);
}


// not really much we need to do about this, because we marked the data beforehand, but for debug purposes, we will check it out.
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
			assert(item->migrate == _migrate_sync);
		}
	}
#endif
}

