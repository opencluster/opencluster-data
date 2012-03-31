// bucket.c

#define __BUCKET_C
#include "bucket.h"
#undef __BUCKET_C

#include "globals.h"
#include "item.h"
#include "push.h"
#include "timeout.h"

#include <assert.h>
#include <stdlib.h>
#include <string.h>


// the list of buckets that this server is handling.  '_mask' indicates how many entries in the 
// array there is, but the ones that are not handled by this server, will have a NULL entry.
bucket_t ** _buckets = NULL;


// keep a count of the number of buckets we have that do not have backup copies.  This is used to 
// speed up certain operations to avoid having to iterate through the list of buckets to find out 
// if we have any that are backup-less.   So it is important to keep this value accurate or 
// certain importain options might get skipped incorrectly.
int _nobackup_buckets = 0;


// keep a count of the number of primary and secondary buckets this node has.
int _primary_buckets = 0;
int _secondary_buckets = 0;

// 0 if no buckets are currently being transferred, 1 if there is.  There can only be one transfer 
// at a time.
int _bucket_transfer = 0;

// the list of hashmasks is to know which servers are responsible for which buckets.
// this list of hashmasks will also replace the need to 'settle'.  Instead, a timed event will 
// occur periodically that checks that the hashmask coverage is complete.
hashmask_t ** _hashmasks = NULL;


// When a migration of a bucket needs to occur, and only one migration can occur at a time, we need 
// to send ALL the data for this bucket to the other node, we need to have a way to tell if an item 
// has already been transferred, and if it hasn't.   And we dont particularly want to go through the 
// list first marking a flag.  So instead, each hash item will have an integer, and we will have a 
// master integer (_migrate_sync).  When we increment the master integer, which will immediately 
// make the integer in all the items obsolete.   Now the tree can be searched for items that have an 
// outdated integer, transfer that item, and update the integer to match the master integer.   Since 
// it will need to look at the chain of trees, this process will also work for them also.   Any 
// items founds in the sub-chains will need to be moved to the current chain as it continues this 
// process.
int _migrate_sync = 0;



// get a value from whichever bucket is resposible.
value_t * buckets_get_value(int map_hash, int key_hash) 
{
	int bucket_index;
	bucket_t *bucket;
	value_t *value = NULL;

	// calculate the bucket that this item belongs in.
	bucket_index = _mask & key_hash;
	assert(bucket_index >= 0);
	assert(bucket_index < _mask);
	bucket = _buckets[bucket_index];

	// if we have a record for this bucket, then we are either a primary or a backup for it.
	if (bucket) {

		assert(bucket->hash == bucket_index);
		
		// make sure that this server is 'primary' for this bucket.
		if (bucket->level != 0) {
			// we need to reply with an indication of which server is actually responsible for this bucket.
			assert(value == NULL);
			assert(0);
		}
		else {
			// search the btree in the bucket for this key.
			assert(bucket->data);
			value = data_get_value(map_hash, key_hash, bucket->data);
		}	
	}
	
	return(value);
}



// store the value in whatever bucket is resposible for the key_hash.
// NOTE: value is controlled by the tree after this function call.
// NOTE: name is controlled by the tree after this function call.
int buckets_store_value(int map_hash, int key_hash, char *name, int name_int, int expires, value_t *value) 
{
	int bucket_index;
	bucket_t *bucket;
	client_t *backup_client;

		// calculate the bucket that this item belongs in.
	bucket_index = _mask & key_hash;
	assert(bucket_index >= 0);
	assert(bucket_index <= _mask);
	bucket = _buckets[bucket_index];

	// if we have a record for this bucket, then we are (potentially) either a primary or a backup for it.
	if (bucket) {
		assert(bucket->hash == bucket_index);
		if (bucket->backup_node) {
			backup_client = bucket->backup_node->client;
			assert(backup_client);
		}
		else {
			backup_client = NULL;
		}
		
		data_set_value(map_hash, key_hash, bucket->data, name, name_int, value, expires, backup_client);
		return(0);
	}
	else {
		return(-1);
	}
}



bucket_t * bucket_new(hash_t hash)
{
	bucket_t *bucket;
	
	assert(_buckets[hash] == NULL);
	
	bucket = calloc(1, sizeof(bucket_t));
	bucket->hash = hash;
	bucket->level = -1;
			
	assert(bucket->backup_node == NULL);
	assert(bucket->target_node == NULL);
	assert(bucket->logging_node == NULL);
	assert(bucket->transfer_event == NULL);
	assert(bucket->shutdown_event == NULL);
	assert(bucket->transfer_client == NULL);
	assert(data_in_transit() == 0);
	assert(bucket->oldbucket_event == NULL);
	assert(bucket->transfer_mode_special == 0);
			
	bucket->data = data_new(hash);
	
	return(bucket);
}



// delete the contents of the bucket.  Note, that the bucket becomes empty, but the bucket itself is 
// not destroyed.
void bucket_destroy(bucket_t *bucket)
{
	assert(bucket);
	
	// at this point, since the bucket is being destroyed, there should be a connected transfer client.
	assert(bucket->transfer_client == NULL);

	if (bucket->data) {
		data_destroy(bucket->data, bucket->hash);
	}
	
	assert(bucket->data == NULL);
}


static void bucket_close(bucket_t *bucket)
{
	assert(bucket);
	
	
	// cant remember what this function is used for, need to look through the code that calls it.
	// it might be the same as bucket_destroy.
	assert(0);
}




// this function will take the current array, and put it aside, creating a new array based on the 
// new mask supplied (we can only make the mask bigger, and cannot shrink it).
// We create a new hashmasks array, and for each entry, we compare it against the old mask, and use 
// the data for that hash from the old list.
// NOTE: We may be starting off with an empty hashmasks lists.  If this is the first time we've 
//       received some hashmasks.  To implement this easily, if we dont already have hashmasks, we 
//       may need to create one that has a dummy entry in it.
void buckets_split_mask(int mask) 
{
	hashmask_t **newlist = NULL;
	hashmask_t **oldlist = NULL;
	
	bucket_t **newbuckets = NULL;
	bucket_t **oldbuckets = NULL;
	
	int i;
	int index;
	
	assert(mask > _mask);
	
	if (_verbose > 1) printf("Splitting bucket list: oldmask=%08X, newmask=%08X\n", _mask, mask);
	
	// first grab a copy of the existing hashmasks as the 'oldlist'
	oldlist = _hashmasks;
	_hashmasks = NULL;
	if (oldlist == NULL) {
		
		oldlist = malloc(sizeof(hashmask_t *));
		assert(oldlist);
		
		// need to create at least one dummy entry so that we can split it to the new entries.
		oldlist[0] = malloc(sizeof(hashmask_t));
		assert(oldlist[0]);
		
		oldlist[0]->local = 0;
		oldlist[0]->primary = NULL;
		oldlist[0]->secondary = NULL;
	}
	
	// grab a copy of the existing buckets as the 'oldbuckets';
	oldbuckets = _buckets;
	_buckets = NULL;
	
	// make an appropriate sized new hashmask list.
	newlist = malloc(sizeof(hashmask_t *) * (mask+1));
	assert(newlist);
	
	// make an appropriate sized new buckets list;
	newbuckets = malloc(sizeof(bucket_t *) * (mask+1));
	assert(newbuckets);
	
	// go through every hash for this mask.
	for (i=0; i<=mask; i++) {

		// determine what the old index is.
		index = i & _mask;
		
		// create the new hashmask entry for the new index.  Copy the strings from the old index.
		newlist[i] = malloc(sizeof(hashmask_t));
		assert(_mask == 0 || index < _mask);
		if (oldlist[index]->primary) { newlist[i]->primary = strdup(oldlist[index]->primary); }
		else { newlist[i]->primary = NULL; }
		if (oldlist[index]->secondary) { newlist[i]->secondary = strdup(oldlist[index]->secondary); }
		else { newlist[i]->primary = NULL; }
		
		// create the new bucket ONLY if we already have a bucket object for that index.
		if (oldbuckets == NULL || oldbuckets[index] == NULL) {
			newbuckets[i] = NULL;
		}
		else {
			// we have a bucket for this old index.  So we need to create a new one for this index.
			
			
			newbuckets[i] = bucket_new(i);

			assert(newbuckets[i]->data);
			assert(newbuckets[i]->data->next == NULL);
			assert(oldbuckets[index]->data);
			assert(oldbuckets[index]->data->ref > 0);
			newbuckets[i]->data->next = oldbuckets[index]->data;
			oldbuckets[index]->data->ref ++;

			assert(newbuckets[i]->hash == i);
			newbuckets[i]->level = oldbuckets[index]->level;
			
			newbuckets[i]->target_node = oldbuckets[index]->target_node;
			newbuckets[i]->backup_node = oldbuckets[index]->backup_node;
			newbuckets[i]->logging_node = oldbuckets[index]->logging_node;
			
			assert(data_in_transit() == 0);
		}
	}

	
	// ---------
	
	
	// now we clean up the old hashmask list.
	for (i=0; i<=_mask; i++) {
		assert(oldlist[i]);
		if (oldlist[i]->primary) {
			free(oldlist[i]->primary);
		}
		if (oldlist[i]->secondary) {
			free(oldlist[i]->secondary);
		}
		free(oldlist[i]);
	}
	free(oldlist);
	oldlist = NULL;
	
	
	// clean up the old buckets list.
	if (oldbuckets) {
		for (i=0; i<=_mask; i++) {
			if (oldbuckets[i]) {
				assert(oldbuckets[i]->data);
				assert(oldbuckets[i]->data->ref > 1);
				oldbuckets[i]->data->ref --;
				assert(oldbuckets[i]->data->ref > 0);
				
				oldbuckets[i]->data = NULL;
				
				bucket_close(oldbuckets[i]);
			}
		}
	}	
	
	_hashmasks = (void *) newlist;
	_buckets = newbuckets;
	_mask = mask;
	
	
	assert(_mask > 0);
	assert(_hashmasks);
	assert(_buckets);
}


int buckets_nobackup_count(void)
{
	assert(_nobackup_buckets >= 0);
	return(_nobackup_buckets);
}




static void bucket_shutdown_handler(evutil_socket_t fd, short what, void *arg) 
{
	bucket_t *bucket = arg;
	int done = 0;
	
	assert(fd == -1);
	assert(arg);
	assert(bucket);
	assert(bucket->shutdown_event);
	
	// if the bucket is a backup bucket, we can simply destroy it, and send out a message to clients 
	// that it is no longer the backup for the bucket.
	if (bucket->level > 0) {
		done ++;
	}
	else {
		assert(bucket->level == 0);

		// if the bucket is primary, but there are no nodes to send it to, then we destroy it.
		if (_node_count == 0) {
			done ++;
		}
		else {
			assert(_node_count > 0);
			
			// If the backup node is connected, then we will tell that node, that it has been 
			// promoted to be primary for the bucket.
			if (bucket->backup_node && bucket->backup_node->client) {
				push_promote(bucket->backup_node->client, bucket->hash);

				done ++;
			}
			else {
			
				// at this point, we are the primary and there is no backup.  There are other nodes 
				// connected, so we need to try and transfer this bucket to another node.
				assert(0);
				
				assert(done == 0);
				
// 				assert(_buckets[i]->transfer_event == NULL);
// 				_buckets[i]->transfer_event = evtimer_new(_evbase, bucket_transfer_handler, _buckets[i]);
// 				assert(_buckets[i]->transfer_event);
			}
		}
	}
	
	if (done > 0) {
		// we are done with the bucket.
	
		assert(bucket->transfer_client == NULL);
		bucket_destroy(bucket);
		push_hashmask_update(bucket);
				
		event_free(bucket->shutdown_event);
		bucket->shutdown_event = NULL;

		assert(_buckets[bucket->hash] == bucket);
		_buckets[bucket->hash] = NULL;
		free(bucket);
		bucket = NULL;
	}
	else {
		// we are not done yet, so we need to schedule the event again.
		assert(bucket->shutdown_event);
		evtimer_add(bucket->shutdown_event, &_timeout_shutdown);
	}		
}



// if the shutdown process has not already been started, then we need to start it.  Otherwise do nothing.
void bucket_shutdown(bucket_t *bucket)
{
	assert(bucket);
	
	if (bucket->shutdown_event == NULL) {
		printf("Bucket shutdown initiated: %04X\n", bucket->hash);

		assert(_evbase);
		bucket->shutdown_event = evtimer_new(_evbase, bucket_shutdown_handler, bucket);
		assert(bucket->shutdown_event);
		evtimer_add(bucket->shutdown_event, &_timeout_now);
	}
}



void buckets_init(void)
{
	int i;
	
	assert(_mask > 0);
	
	_buckets = calloc(_mask+1, sizeof(bucket_t *));
	assert(_buckets);

	assert(_primary_buckets == 0);
	assert(_secondary_buckets == 0);
	
	assert(_hashmasks == NULL);
	_hashmasks = calloc(_mask+1, sizeof(hashmask_t *));
	assert(_hashmasks);

	// for starters we will need to create a bucket for each hash.
	for (i=0; i<=_mask; i++) {
		_buckets[i] = bucket_new(i);

		_primary_buckets ++;
		_buckets[i]->level = 0;

		// send out a message to all connected clients, to let them know that the buckets have changed.
		push_hashmask_update(_buckets[i]); // all_hashmask(i, 0);
		
		_hashmasks[i] = calloc(1, sizeof(hashmask_t));
		assert(_hashmasks[i]);
		
		_hashmasks[i]->local = -1;
		_hashmasks[i]->primary = NULL;
		_hashmasks[i]->secondary = NULL;
	}

	// indicate that we have buckets that do not have backup copies on other nodes.
	_nobackup_buckets = _mask + 1;
	
	// we should have hashmasks setup as well by this point.
	assert(_hashmasks);
}



// we've been given a 'name' for a hash-key item, and so we lookup the bucket that is responsible 
// for that item.  the 'data' module will then find the data store within that handles that item.
int buckets_store_name(hash_t key_hash, char *name, int int_key)
{
	int bucket_index;
	bucket_t *bucket;

	assert((name == NULL) || (name && int_key == 0));

	// calculate the bucket that this item belongs in.
	bucket_index = _mask & key_hash;
	assert(bucket_index >= 0);
	assert(bucket_index <= _mask);
	bucket = _buckets[bucket_index];

	// if we have a record for this bucket, then we are either a primary or a backup for it.
	if (bucket) {
		assert(bucket->hash == bucket_index);
		
		// make sure that this server is 'primary' or 'secondary' for this bucket.
		assert(bucket->data);
		data_set_name(key_hash, bucket->data, name, int_key);
		return(0);
	}
	else {
		// we dont have the bucket, we need to let the other node know that something has gone wrong.
		return(-1);
	}
}
