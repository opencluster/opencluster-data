// bucket_data.c

#include "bucket_data.h"
#include "globals.h"
#include "hash.h"
#include "item.h"

#include <assert.h>
#include <stdlib.h>
#include <string.h>


typedef struct {
	GTree *mapstree;
	hash_t item_key;
} maplist_t;

typedef struct {
	hash_t search_hash;
	maplist_t *map;
	item_t *item;
} trav_map_t;




static gint key_compare_fn(gconstpointer a, gconstpointer b)
{
	const register unsigned int *aa, *bb;
	
	aa = a;
	bb = b;
	
	return((*aa) - (*bb));
}



bucket_data_t * data_new(void)
{
	bucket_data_t *data;
	
	data = malloc(sizeof(bucket_data_t));
	assert(data);
	data->ref = 1;
	data->tree = g_tree_new(key_compare_fn);
	assert(data->tree);
	data->next = NULL;
	
	return(data);
}



gboolean traverse_map_fn(gpointer p_key, gpointer p_value, void *p_data)
{
	trav_map_t *data = p_data;
	
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
	trav_map_t *data = p_data;
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
	trav_map_t trav;
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



value_t * data_get_value(int map_hash, int key_hash, bucket_data_t *ddata) 
{
	bucket_data_t *current;
	maplist_t *list;
	maplist_t *first_list = NULL;
	item_t *item;
	int finished = 0;
	value_t *value = NULL;

	assert(ddata);
	assert(ddata->tree);
	
	current = ddata;
	while (finished == 0 && current) {
		assert(value == NULL);
		
		list = g_tree_lookup(current->tree, &key_hash);
		if (list == NULL) {
			if (current->next) {
				// didn't find the hash in the tree, and there is another tree in the chain, so we 
				// will look there instead.
				current = current->next;
				assert(finished == 0);
				assert(value == NULL);
			}
			else {
				// we didn't find it, and there are no more in the chain, so we need to indicate 
				// that it is not here.
				assert(value == NULL);
				finished ++;
			}
		}
		else {

			// we found this hashkey here, 
			
			if (first_list == NULL) {
				// make a note of the first list found, because if we find this entry in another 
				// tree, we need to remove it from that tree and put it in this list.  Saves us 
				// having to look it up again.
				first_list = list;
			}
			
			// search the list of maps for a match.
			assert(list->mapstree);
			item = g_tree_lookup(list->mapstree, &map_hash);
			if (item == NULL) {
				if (current->next) {
					// it wasn't found, but we have a parent tree, so we should run the query again on that.
					current = current->next;
					assert(finished == 0);
					assert(value == NULL);
				}
				else {
					assert(value == NULL);
					finished ++;
				}
			}
			else {
				// item is found, return with the data.
				assert(item->name);
				assert(item->value);
				
				// we dont need to loop anymore.  We found what we are looking for.
				finished ++;
				
				if (item->expires > 0 && item->expires < _seconds) {
					// item has expired.   We need to remove it from the map list.
					assert(value == NULL);
					assert(0);
				}
				else {
					value = item->value;
					assert(value);
					
					// if the item was found in a tree that is not the primary, then we need to 
					// remove it from the tree and put it in the primary.  
					if (current != ddata) {
						
						// first remove the one in the chained tree
						g_tree_remove(list->mapstree, &map_hash);

						// set it to NULL so that we dont accidentally use it.
						list = NULL;
						
						if (first_list == NULL) {
							// we dont have an entry for this hash, so we need to make one.
							
							first_list = malloc(sizeof(maplist_t));
							assert(first_list);
							first_list->item_key = key_hash;
							first_list->mapstree = g_tree_new(key_compare_fn);
							assert(first_list->mapstree);

							g_tree_insert(ddata->tree, &first_list->item_key, first_list);
						}
						
						assert(first_list);
						assert(first_list->mapstree);
						
						// obviously if we got this far, then this entry is NOT already in the map entry for this tree, otherwise current == ddata, which it isn't.
						g_tree_insert(first_list->mapstree, &item->map_key, item);
					}
				}	
			}
		}
	}
	
	return(value);
}

// the control of 'value' is given to this function.
// NOTE: value is controlled by the tree after this function call.
// NOTE: name is controlled by the tree after this function call.
int data_set_value(int map_hash, int key_hash, bucket_data_t *ddata, char *name, value_t *value, int expires) 
{
	bucket_data_t *current;
	maplist_t *list;
	maplist_t *first_list = NULL;
	item_t *item;
	int result = -1;
	int finished = 0;
	
	assert(ddata);
	assert(name);
	assert(value);
	assert(expires >= 0);

	assert(ddata->tree);
	current = ddata;
	
	// first we are going to store the value in the primary 'bucket_data'.  If it exists, we will 
	// update the value in there.  If it doesn't exist we will add it, and when we've done that, 
	// then we will go through the rest of the chain to find this same entry, if it exists, then 
	
	// search the btree in the bucket for this key.
	list = g_tree_lookup(ddata->tree, &key_hash);
	if (list == NULL) {
		assert(first_list == NULL);
		first_list = malloc(sizeof(maplist_t));
		assert(first_list);
		first_list->item_key = key_hash;
		first_list->mapstree = g_tree_new(key_compare_fn);
		assert(first_list->mapstree);

		g_tree_insert(ddata->tree, &first_list->item_key, first_list);
		
		// hash was not found, so we assume that we are still going to check the chain for it.
		assert(finished == 0);
	}
	else {
		// the hash was found in the tree.
		first_list = list;
		
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
			free(name);
			name = NULL;
			
			item->expires = expires == 0 ? 0 : _seconds + expires;
			
			// since we've moved the value to the existing one, we dont need to add an entry, so we 
			// can clear first_list to ensure that doesn't happen.
			first_list = NULL;
			assert(result < 0);
			result = 0;
		}
	}

	if (first_list) {	
		// now that we've got a maplist created for this hash, we need to add this map (item) to it.
		item = malloc(sizeof(item_t));
		assert(item);

		item->item_key = key_hash;
		item->map_key = map_hash;
		item->name = name;
		item->value = value;
		item->expires = expires == 0 ? 0 : _seconds + expires;
		
		g_tree_insert(first_list->mapstree, &item->map_key, item);
	
		assert(result < 0);
		result = 0;

		name = NULL;
		
		assert(list == NULL);
		
		// since we created this item here, we now need to go through the chain to make sure this 
		// item is not in there.
		current = ddata->next;
		assert(finished == 0);
		while (finished == 0 && current) {
			list = g_tree_lookup(ddata->tree, &key_hash);
			if (list == NULL) {
				current = current->next;
				assert(finished == 0);
			}
			else {
				assert(list->mapstree);
				item = g_tree_lookup(list->mapstree, &map_hash);
				if (item == NULL) {
					current = current->next;
					assert(finished == 0);
				}
				else {
					g_tree_remove(list->mapstree, &map_hash);
					
					item_destroy(item);
				}
			}
		}
	}

	assert(name == NULL);
	
	return(result);
}



// go through the trees in the data to find items for this hashkey that need to be migrated.  Uses 
// some global variables for extra information.
item_t ** data_get_migrate_items(bucket_data_t *data, hash_t hash, int limit)
{
	item_t **items = NULL;
	
	
	assert(0);
	
	return(items);
}

