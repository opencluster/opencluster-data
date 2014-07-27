// bucket.c


// By setting __BUCKET_C, we indicate that we dont want the externs to be defined.
#include "bucket.h"

#include "constants.h"
#include "item.h"
#include "logging.h"
#include "push.h"
#include "server.h"
#include "stats.h"
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

// the mask is used to determine which bucket a hash belongs to.
hash_t _mask = 0;


// keep a count of the number of primary and secondary buckets this node has.
int _primary_buckets = 0;
int _secondary_buckets = 0;

// 0 if no buckets are currently being transferred, 1 if there is.  There can only be one transfer 
// at a time.
int _bucket_transfer = 0;



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

static struct event_base *_evbase = NULL;


// return the current migrate sync counter.  This is used when traversing the data trees to find 
// items that have not been migrated.
int buckets_get_migrate_sync(void)
{
	assert(_migrate_sync >= 0);
	return(_migrate_sync);
}

// get a value from whichever bucket is resposible.
value_t * buckets_get_value(hash_t map_hash, hash_t key_hash) 
{
	int bucket_index;
	bucket_t *bucket;
	value_t *value = NULL;

	// calculate the bucket that this item belongs in.
	assert(_mask > 0);
	bucket_index = _mask & key_hash;
	assert(bucket_index >= 0);
	assert(bucket_index <= _mask);
	bucket = _buckets[bucket_index];

	// if we have a record for this bucket, then we are either a primary or a backup for it.
	if (bucket) {
		assert(bucket->hashmask == bucket_index);
		
		// make sure that this server is 'primary' for this bucket.
		if (bucket->level != 0) {
			// we need to reply with an indication of which server is actually responsible for this bucket.
			assert(value == NULL);
		}
		else {
			// search the btree in the bucket for this key.
			assert(bucket->data);
			value = data_get_value(map_hash, key_hash, bucket->data);
		}	
	}
	else {
		assert(value == NULL);
	}
	
	return(value);
}



// store the value in whatever bucket is resposible for the key_hash.
// NOTE: value is controlled by the tree after this function call.
// NOTE: name is controlled by the tree after this function call.
int buckets_store_value(hash_t map_hash, hash_t key_hash, int expires, value_t *value) 
{
	int bucket_index;
	bucket_t *bucket;
	client_t *backup_client;

	// calculate the bucket that this item belongs in.
	bucket_index = _mask & key_hash;
	assert(bucket_index >= 0);
	assert(bucket_index <= _mask);
	bucket = _buckets[bucket_index];

	// if we have a record for this bucket, then we are (potentially) either a primary or a backup 
	// for it.
	if (bucket) {
		assert(bucket->hashmask == bucket_index);
		if (bucket->backup_node) {
			// since we have a backup_node specified, then we must be the primary.
			backup_client = bucket->backup_node->client;
			assert(backup_client);
		}
		else {
			// since we dont have a backup_node specified, then we are the backup and dont need to 
			// send the data anywhere else.
			backup_client = NULL;
		}
		
		data_set_value(map_hash, key_hash, bucket->data, value, expires, backup_client);
		return(0);
	}
	else {
		assert(0);
		return(-1);
	}
}



bucket_t * bucket_new(hash_t hashmask)
{
	bucket_t *bucket;

	assert(_mask > 0);
	assert(hashmask >= 0);
	assert(hashmask <= _mask);
	
	assert(_buckets[hashmask] == NULL);
	
	bucket = calloc(1, sizeof(bucket_t));
	bucket->hashmask = hashmask;
	bucket->level = -1;

	assert(bucket->primary_node == NULL);
	assert(bucket->secondary_node == NULL);
	assert(bucket->backup_node == NULL);
	assert(bucket->source_node == NULL);
	assert(bucket->logging_node == NULL);
	assert(bucket->transfer_event == NULL);
	assert(bucket->shutdown_event == NULL);
	assert(bucket->transfer_client == NULL);
	assert(data_in_transit() == 0);
	assert(bucket->oldbucket_event == NULL);
	assert(bucket->transfer_mode_special == 0);
	assert(bucket->promoting == NOT_PROMOTING);
	
	bucket->data = data_new(_mask, hashmask);
	
	return(bucket);
}



// delete the contents of the bucket.  Note, that the bucket becomes empty, but the bucket itself is 
// not destroyed.
void bucket_destroy_contents(bucket_t *bucket)
{
	assert(bucket);
	
	// at this point, since the bucket is being destroyed, there should be a connected transfer client.
	assert(bucket->transfer_client == NULL);

	if (bucket->data) {
		data_destroy(bucket->data, _mask, bucket->hashmask);
		data_free(bucket->data);
		bucket->data = NULL;
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
// We create a new array, and for each entry, we compare it against the old mask, and use 
// the data for that hash from the old list.
// NOTE: We may be starting off with an empty lists.  If this is the first time we've 
//       received some hashmasks.  To implement this easily, if we dont already have hashmasks, we 
//       may need to create one that has a dummy entry in it.
void buckets_split_mask(hash_t current_mask, hash_t new_mask) 
{
	bucket_t **newbuckets = NULL;
	bucket_t **oldbuckets = NULL;
	
	int i;
	int index;
	
	assert(new_mask > current_mask);
	
	// grab a copy of the existing buckets as the 'oldbuckets';
	oldbuckets = _buckets;
	_buckets = NULL;
	
	// make an appropriate sized new buckets list;
	newbuckets = malloc(sizeof(bucket_t *) * (new_mask+1));
	assert(newbuckets);
	
	// go through every hash for this mask.
	for (i=0; i<=new_mask; i++) {

		// determine what the old index is.
		index = i & current_mask;
		
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

			assert(newbuckets[i]->hashmask == i);
			newbuckets[i]->level = oldbuckets[index]->level;
			
			newbuckets[i]->source_node = oldbuckets[index]->source_node;
			newbuckets[i]->backup_node = oldbuckets[index]->backup_node;
			newbuckets[i]->logging_node = oldbuckets[index]->logging_node;
			
			newbuckets[i]->primary_node = oldbuckets[index]->primary_node;
			newbuckets[i]->secondary_node = oldbuckets[index]->secondary_node;
			
			assert(data_in_transit() == 0);
		}
	}

	assert(_mask >= 0);
	assert(new_mask > _mask);
	_mask = new_mask;
	
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
	
	_buckets = newbuckets;
	assert(_buckets);
}


int buckets_nobackup_count(void)
{
	assert(_nobackup_buckets >= 0);
	return(_nobackup_buckets);
}


// check the integrity of the empty bucket, and then free the memory it uses.
static void bucket_free(bucket_t *bucket)
{
	assert(bucket);
	assert(bucket->level < 0);
	assert(bucket->data == NULL);
	assert(bucket->source_node == NULL);
	assert(bucket->backup_node == NULL);
	assert(bucket->logging_node == NULL);
	assert(bucket->transfer_client == NULL);
	assert(bucket->transfer_mode_special == 0);
	assert(bucket->shutdown_event == NULL);
	assert(bucket->transfer_event == NULL);
	assert(bucket->oldbucket_event == NULL);
	
	free(bucket);
}


void update_hashmasks(bucket_t *bucket)
{
	assert(bucket);
	
	assert(bucket->hashmask >= 0 && bucket->hashmask <= _mask);
	assert(bucket->level == -1 || bucket->level == 0 || bucket->level == 1);

	client_update_hashmasks(_mask, bucket->hashmask, bucket->level);
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
		if (node_active_count() == 0) {
			done ++;
		}
		else {
			// If the backup node is connected, then we will tell that node, that it has been 
			// promoted to be primary for the bucket.
			if (bucket->backup_node) {
				assert(bucket->backup_node->client);
				push_promote(bucket->backup_node->client, bucket->hashmask);

				assert(bucket->promoting == NOT_PROMOTING);
				bucket->promoting = PROMOTING;
				
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
		bucket_destroy_contents(bucket);
		update_hashmasks(bucket);
				
		assert(bucket->shutdown_event);
		event_free(bucket->shutdown_event);
		bucket->shutdown_event = NULL;

		assert(_buckets[bucket->hashmask] == bucket);
		_buckets[bucket->hashmask] = NULL;
		
		bucket_free(bucket);
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
	
}



int buckets_shutdown(void)
{
	int waiting = 0;
	hash_t i;
	bucket_t *bucket;
	
	if (_buckets) {
		assert(_mask > 0);
		for (i=0; i<=_mask; i++) {
			assert(_buckets[i]);
			bucket = _buckets[i];
			
			if (bucket->data) {
			
				waiting ++;
				if (bucket->shutdown_event == NULL) {
					printf("Bucket shutdown initiated: %#llx\n", bucket->hashmask);

					assert(_evbase);
					bucket->shutdown_event = evtimer_new(_evbase, bucket_shutdown_handler, bucket);
					assert(bucket->shutdown_event);
					evtimer_add(bucket->shutdown_event, &_timeout_now);
				}
			}
		}
	}

	return(waiting);
}



// this is only used when this node is the first node in the cluster, and we need to create some new empty buckets.
void buckets_init(hash_t mask, struct event_base *evbase)
{
	int i;

	assert(_evbase == NULL);
	assert(evbase);
	_evbase = evbase;
	
	assert(_mask == 0);
	assert(mask >= _mask);
	_mask = mask;
	
	assert(_mask > 0);
	_buckets = calloc(_mask+1, sizeof(bucket_t *));
	assert(_buckets);

	assert(_primary_buckets == 0);
	assert(_secondary_buckets == 0);
	
	
	
	// for starters we will need to create a bucket for each hash.
	for (i=0; i<=_mask; i++) {
		_buckets[i] = bucket_new(i);

		_primary_buckets ++;
		_buckets[i]->level = 0;

		// send out a message to all connected clients, to let them know that the buckets have changed.
		update_hashmasks(_buckets[i]); // all_hashmask(i, 0);
	}

	// indicate that we have buckets that do not have backup copies on other nodes.
	_nobackup_buckets = _mask + 1;
}



// we've been given a keyvalue for a hash-key item, and so we lookup the bucket that is responsible 
// for that item.  the 'data' module will then find the data store within that handles that item.
int buckets_store_keyvalue_str(hash_t key_hash, int length, char *name)
{
	hash_t bucket_index;
	bucket_t *bucket;

	assert(name);
	assert(length > 0);

	// calculate the bucket that this item belongs in.
	assert(_mask > 0);
	bucket_index = _mask & key_hash;
	assert(bucket_index <= _mask);
	bucket = _buckets[bucket_index];

	// if we have a record for this bucket, then we are either a primary or a backup for it.
	if (bucket) {
		assert(bucket->hashmask == bucket_index);
		
		// make sure that this server is 'primary' or 'secondary' for this bucket.
		assert(bucket->data);
		
		// 'name' will be controlled by the keyvalue tree after this function.
		data_set_keyvalue_str(key_hash, bucket->data, length, name);
		return(0);
	}
	else {
		// we dont have the bucket, we need to let the other node know that something has gone wrong.
		return(-1);
	}
}


// we've been given a 'name' for a hash-key item, and so we lookup the bucket that is responsible 
// for that item.  the 'data' module will then find the data store within that handles that item.
int buckets_store_keyvalue_int(hash_t key_hash, long long int_key)
{
	hash_t bucket_index;
	bucket_t *bucket;

	// calculate the bucket that this item belongs in.
	assert(_mask > 0);
	bucket_index = _mask & key_hash;
	assert(bucket_index <= _mask);
	bucket = _buckets[bucket_index];

	// if we have a record for this bucket, then we are either a primary or a backup for it.
	if (bucket) {
		assert(bucket->hashmask == bucket_index);
		
		// make sure that this server is 'primary' or 'secondary' for this bucket.
		assert(bucket->data);
		data_set_keyvalue_int(key_hash, bucket->data, int_key);
		return(0);
	}
	else {
		// we dont have the bucket, we need to let the other node know that something has gone wrong.
		return(-1);
	}
}


static void hashmasks_dump(void)
{
	hash_t i;

	assert(_buckets);
	
	stat_dumpstr("HASHMASKS");
	for (i=0; i<=_mask; i++) {
		assert(_buckets[i]);
		assert(_buckets[i]->primary_node);
		assert(_buckets[i]->secondary_node);
		const char *name_primary = node_name(_buckets[i]->primary_node);
		const char *name_secondary = node_name(_buckets[i]->secondary_node);

		stat_dumpstr("  Hashmask:%#llx, Primary:'%s', Secondary:'%s'", i, 
					 name_primary ? name_primary : "",
					 name_secondary ? name_secondary : "");
	}
	
	stat_dumpstr(NULL);
}



static void bucket_dump(bucket_t *bucket)
{
	node_t *node;
	char *mode = NULL;
	char *altmode = NULL;
	const char *altnode = NULL;
	
	assert(bucket);
	
	if (bucket->level == 0) {
		mode = "Primary";
		altmode = "Backup";
		assert(bucket->source_node == NULL);
		if (bucket->backup_node) {
			assert(bucket->backup_node->conninfo);
			altnode = conninfo_name(bucket->backup_node->conninfo);
		}
		else {
			altnode = "";
		}
	}
	else if (bucket->level == 1) {
		mode = "Secondary";
		assert(bucket->backup_node == NULL);
		assert(bucket->source_node);
		assert(bucket->source_node->conninfo);
		altmode = "Source";
		altnode = conninfo_name(bucket->source_node->conninfo);
	}
	else {
		mode = "Unknown";
		altmode = "Unknown";
		altnode = "";
	}

	assert(mode);
	assert(altmode);
	assert(altnode);
	stat_dumpstr("    Bucket:%#llx, Mode:%s, %s Node:%s", bucket->hashmask, mode, altmode, altnode);
	
	assert(bucket->data);
//	data_dump(bucket->data);

	if (bucket->transfer_client) {
		node = bucket->transfer_client->node;
		assert(node);
		assert(node->conninfo);
		const char *name = conninfo_name(node->conninfo);
		assert(name);
		stat_dumpstr("      Currently transferring to: %s", name);
		stat_dumpstr("      Transfer Mode: %d", bucket->transfer_mode_special);
	}
}


void buckets_dump(void)
{
	int i;
	
	stat_dumpstr("BUCKETS");
	stat_dumpstr("  Mask: %#llx", _mask);
	stat_dumpstr("  Buckets without backups: %d", _nobackup_buckets);
	stat_dumpstr("  Primary Buckets: %d", _primary_buckets);
	stat_dumpstr("  Secondary Buckets: %d", _secondary_buckets);
	stat_dumpstr("  Bucket currently transferring: %s", _bucket_transfer == 0 ? "no" : "yes");
	stat_dumpstr("  Migration Sync Counter: %d", _migrate_sync);

	hashmasks_dump();
	
	stat_dumpstr("  List of Buckets:");
	
	for (i=0; i<=_mask; i++) {
		if (_buckets[i]) {
			bucket_dump(_buckets[i]);
		}
	}
	stat_dumpstr(NULL);
}









// Get the primary node for an external bucket.  If the bucket is being handled by this instance, 
// then this function will return NULL.  If it is being handled by another node, then it will return 
// a node pointer.
node_t * buckets_get_primary_node(hash_t key_hash) 
{
	int bucket_index;
	bucket_t *bucket;

	// calculate the bucket that this item belongs in.
	assert(_mask > 0);
	bucket_index = _mask & key_hash;
	assert(bucket_index >= 0);
	assert(bucket_index <= _mask);
	bucket = _buckets[bucket_index];
	if (bucket->source_node) {
		// that bucket is being handled 
		return(NULL);
	}
	else {
		assert(bucket->primary_node);
		return(bucket->primary_node);
	}
}


int buckets_get_primary_count(void)
{
	assert(_primary_buckets >= 0);
	return(_primary_buckets);
}

int buckets_get_secondary_count(void)
{
	assert(_secondary_buckets >= 0);
	return(_secondary_buckets);
}


// return the number of buckets that are currently transferring (normally either 0 or 1).
int buckets_transferring(void)
{
	assert(_bucket_transfer >= 0);
	return(_bucket_transfer);
}



int buckets_accept_bucket(client_t *client, hash_t mask, hash_t hashmask)
{
	int accepted = -1;
	
	assert(client);
	assert(_mask > 0);
	
	if (_bucket_transfer != 0) {
		//  we are currently transferring another bucket, therefore we cannot accept another one.
		logger(LOG_WARN, "cant accept bucket, already transferring one.");
		accepted = 0;
	}
	else { 
		assert(_bucket_transfer == 0);
		if (mask != _mask) {
			// the masks are different, we cannot accept a bucket unless our masks match... which should 
			// balance out once hashmasks have proceeded through all the nodes.
			logger(LOG_WARN, "cant accept bucket, masks are not compatible.");
			accepted = 0;
		}
		else {
			// now we need to check that this bucket isn't already handled by this server.

			assert(hashmask <= mask);
			assert(_mask == mask);
			
			// if we dont currently have a buckets list, we need to create one.
			if (_buckets == NULL) {
				_buckets = calloc(_mask + 1, sizeof(bucket_t *));
				assert(_buckets);
				assert(_buckets[0] == NULL);
				assert(_primary_buckets == 0);
				assert(_secondary_buckets == 0);
			}
			
			assert(_buckets);
			if (_buckets[hashmask]) {
				logger(LOG_ERROR, "cant accept bucket, already have that bucket.");
				accepted = 0;
			}
			else {
				logger(LOG_INFO, "accepting bucket. (%#llx/%#llx)", mask, hashmask);
				
				assert(_bucket_transfer == 0);
				_bucket_transfer = 1;
				
				// need to create the bucket, and mark it as in-transit.
				bucket_t *bucket = bucket_new(hashmask);
				_buckets[hashmask] = bucket;
				assert(data_in_transit() == 0);
				assert(bucket->transfer_client == NULL);
				assert(client->node);
				logger(LOG_DEBUG, "Setting transfer client ('%s') to bucket %#llx.", node_name(client->node), bucket->hashmask);
				bucket->transfer_client = client;
				assert(bucket->level < 0);
				accepted = 1;
			}
		}
	}

	assert(accepted == 0 || accepted == 1);
	return(accepted);
}





void buckets_control_bucket(client_t *client, hash_t mask, hash_t hashmask, int level)
{
	bucket_t *bucket = NULL;
	
	assert(client);
	assert(_mask > 0);
	assert(_mask == mask);
	assert(level == 0 || level == 1);
	
	assert(_buckets);
	assert(hashmask <= _mask);
	assert(hashmask >= 0);

	// ** some of these values should be checked at runtime
	bucket = _buckets[hashmask];
	assert(bucket);
	assert(bucket->hashmask == hashmask);
	assert(bucket->transfer_client == NULL);
	assert(data_in_transit() == 0);
	assert(bucket->transfer_event == NULL);
	
	// mark the bucket as ready for action.
	if (level == 0) {
		// we are switching a bucket from secondary to primary.
		assert(bucket->level == 1);
		assert(bucket->backup_node == NULL);
		assert(bucket->source_node);
		assert(bucket->source_node->client == client);
		bucket->backup_node = bucket->source_node;
		bucket->source_node = NULL;
		
		_primary_buckets ++;
		_secondary_buckets --;
		assert(_primary_buckets > 0);
		assert(_secondary_buckets >= 0);
	}
	else if (level == 1)  {
		// we are switching a bucket from primary to secondary.  
		assert(bucket->level == 0);
		
		// we should be connected to this other node.
		assert(bucket->source_node == NULL);
		assert(bucket->backup_node);
		assert(bucket->backup_node->client == client);
		bucket->source_node = bucket->backup_node;
		bucket->backup_node = NULL;
		
		_primary_buckets --;
		_secondary_buckets ++;
		assert(_primary_buckets >= 0);
		assert(_secondary_buckets > 0);
	}
	else {
		assert(level == -1);
		
		// the other node is telling us to destroy this copy of the bucket.
		assert(0);
	}
	bucket->level = level;

	assert(bucket->primary_node);
	assert(bucket->secondary_node);
	node_t *node = bucket->primary_node;
	bucket->primary_node = bucket->secondary_node;
	bucket->secondary_node = node;
		
	// since this node is receiving the 'switch' command, we should not have any buckets transferring.
	assert(_bucket_transfer == 0);
}





void buckets_hashmasks_update(node_t *node, hash_t hashmask, int level)
{
	assert(node);
	assert(level == 0 || level == 1);
	
	// verify that the hash provided actually describes a bucket.
	assert((hashmask & _mask) == hashmask);	
	
	if (level == 0) {
		_buckets[hashmask]->primary_node = node;
		
		logger(LOG_DEBUG, "Setting HASHMASK: Primary [%#llx] = '%s'",
			hashmask, node_name(node)
		);
	}
	else if (level == 1) {
		_buckets[hashmask]->secondary_node = node;

		logger(LOG_DEBUG, "Setting HASHMASK: Secondary [%#llx] = '%s'",
			hashmask, node_name(node)
		);
	}
	else {
		assert(0);
	}
}


// return the mask
hash_t buckets_mask(void)
{
	assert(_mask > 0);
	return(_mask);
}



bucket_t * buckets_find_switchable(node_t *node)
{
	int i;
	bucket_t *bucket = NULL;
	
	assert(node);
	assert(_buckets);
	
	// go through all the buckets.
	for (i=0; i<=_mask && bucket == NULL; i++) {
		
		// do we have this bucket?
		if (_buckets[i]) {
			
			// are we primary?
			if (_buckets[i]->level == 0) {
				
				// is this client the backup node for this bucket?
				if (_buckets[i]->backup_node == node) {
					
					// yes it is, we can promote this bucket.
					bucket = _buckets[i];

				}
			}
		}
	}
	
	return(bucket);
}





void buckets_finalize_migration(client_t *client, hash_t hashmask, int level, const char *conninfo_str)
{
	bucket_t *bucket;
	
	assert(_mask > 0 && (hashmask >= 0 && hashmask <= _mask));
	assert(client);
	assert(level == 0 || level == 1);
	assert(conninfo_str);
	
	// ** some of these valumes should be checked at runtime
	bucket = _buckets[hashmask];
	assert(bucket);
	assert(bucket->hashmask == hashmask);
	assert(bucket->level < 0);
	assert(bucket->source_node == NULL);
	assert(bucket->backup_node == NULL);
	assert(data_in_transit() == 0);
	assert(bucket->transfer_event == NULL);
	assert(_bucket_transfer == 1);

	assert(bucket->transfer_client == client);
	assert(client);
	assert(client->node);
	logger(LOG_DEBUG, "Removing transfer client ('%s') from bucket %#llx.", node_name(client->node), bucket->hashmask);


	bucket->transfer_client = NULL;
		
	// mark the bucket as ready for action.
	bucket->level = level;
	if (level == 0) {
		// we are receiving a primary bucket.  
		if (conninfo_str) {
			assert(bucket->backup_node == NULL);
			bucket->backup_node = node_find(conninfo_str);
			assert(bucket->backup_node);
			assert(bucket->backup_node->client);
		}
		else {
			assert(bucket->backup_node == NULL);
		}
		assert(bucket->source_node == NULL);
		
		// if level is -1 then indicates bucket is not here.  bucket is here.
		assert(bucket->level >= 0);	

		// indicates buckets is hosted here if level is not -1.
		bucket->primary_node = NULL;
		
		_primary_buckets ++;
		assert(_primary_buckets > 0);
	}
	else {
		// we are receiving a backup bucket.  
		assert(conninfo_str);
		bucket->source_node = node_find(conninfo_str);
		assert(bucket->source_node);
		assert(bucket->source_node->client);

		// if level is -1 then indicates bucket is not here.  bucket is here.
		assert(bucket->level >= 0);	
		
		// setting this to NULL while level is not -1 indicates that the bucket is here.
		bucket->secondary_node = NULL;
		
		// we should be connected to this other node.
		assert(bucket->backup_node == NULL);
		
		_secondary_buckets ++;
		assert(_secondary_buckets > 0);
	}
}





void buckets_set_transferring(bucket_t *bucket, client_t *client)
{
	assert(bucket);
	assert(client);
	assert(client->node);
	
	assert(bucket->transfer_client == NULL);
	logger(LOG_DEBUG, "Setting transfer client ('%s') to bucket %#llx.", node_name(client->node), bucket->hashmask);
	bucket->transfer_client = client;
	
	assert(_bucket_transfer == 0);
	_bucket_transfer = 1;
}

void buckets_clear_transferring(bucket_t *bucket)
{
	assert(bucket);
	
	assert(_bucket_transfer == 1);
	_bucket_transfer = 0;
	
	assert(bucket->transfer_client);
	assert(bucket->transfer_client->node);
	logger(LOG_DEBUG, "Finished transferring to client ('%s') of bucket %#llx.", 
		   node_name(bucket->transfer_client->node), bucket->hashmask);
	bucket->transfer_client = NULL;
}





// go through the list of buckets, and find one that doesn't have a backup copy.
bucket_t * buckets_nobackup_bucket(void) 
{
	int i;
	bucket_t *bucket = NULL;

	assert(_mask > 0);
	
	for (i=0; i<=_mask && bucket == NULL; i++) {
		if (_buckets[i]) {
			if (_buckets[i]->level == 0) {
				if (_buckets[i]->backup_node == NULL) {
					bucket = _buckets[i];

					logger(LOG_INFO, "Attempting to migrate bucket #%#llx that has no backup copy.", bucket->hashmask); 
					
					assert(bucket->hashmask == i);
					assert(bucket->transfer_client == NULL);
				}
			}
		}
	}
	
	return(bucket);
}





static bucket_t * choose_bucket_for_migrate(client_t *client, int primary, int backups, int ideal) 
{
	bucket_t *bucket = NULL;
	int send_level = 0;
	int i;
	
	if (((primary+backups) < ideal) && ((_primary_buckets+_secondary_buckets) > ideal)) {
		// we have more buckets than the target, and it needs one, so should send one.
		
		assert(bucket == NULL);
						
		// If we have more primary than secondary buckets, then we need to send a primary, 
		// otherwise we need to send a secondary.
		assert(send_level == 0);
		if (_secondary_buckets >= _primary_buckets) {
			send_level = 1;
		}
		
		// simply go through the bucket list until we find the appropriate bucket.
		for (i=0; i<=_mask && bucket == NULL; i++) {
			if (_buckets[i]) {
				if (_buckets[i]->level == send_level) {
					
					if (send_level == 0) {
						assert(_buckets[i]->source_node == NULL);
						assert(_buckets[i]->backup_node);
						
						if (_buckets[i]->backup_node != client->node) {
							bucket = _buckets[i];
						}
					}
					else {
						assert(_buckets[i]->source_node);
						assert(_buckets[i]->backup_node == NULL);
						
						if (_buckets[i]->source_node != client->node) {
							bucket = _buckets[i];
						}
					}
				}
			}
		}
	}

	return(bucket);
}



bucket_t * buckets_check_loadlevels(client_t *client, int primary, int backups)
{
	bucket_t *bucket = NULL;
	
	assert(client);
	assert(primary >= 0);
	assert(backups >= 0);
	
	// if the target node is not currently transferring, and we are not currently transferring
	if (_bucket_transfer == 0) {

			
		// we havent sent anything yet, so now we need to check to see if we have any buckets 
		// that do not have backup copies (we do not care about the ideal number of buckets, we 
		// just need to make it a priority to get a second copy of these buckets out quick).
		assert(bucket == NULL);
		if (buckets_nobackup_count() > 0 && (primary+backups < _mask+1)) {
			assert(client);
			assert(client->node);
			bucket = buckets_find_switchable(client->node);
			assert(_bucket_transfer == 0);
		}
	
		if (bucket == NULL) {
			// we didn't find any buckets with no backup copies, so we need to see if there are any 
			// other buckets we should migrate.

			// determine the 'ideal' number of buckets that each node should have.  Integer maths is 
			// perfect here, because we treat this as a 'minimum' which means that if the ideal is 
			// 10.66 (for 16*2/3), then we want to make sure that each node has at least 10, and if 
			// two nodes have 11, then that is fine.
			int active = node_active_count();
			assert(active > 0);
			int ideal = ((_mask+1) *2) / active;
			assert(ideal > 0);
					
			if (ideal < MIN_BUCKETS) {
				// the 'ideal' number of buckets for each node is less than the split threshold, so 
				// we need to split our buckets to maintain integrity of the cluster.   
				
				assert(bucket == NULL);
				assert(0);
			}
			else {
				bucket = choose_bucket_for_migrate(client, primary, backups, ideal);
			}
		}	
	}
	
	return(bucket);
}





