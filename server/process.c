// process replies received from commands sent.

#include "bucket.h"
#include "constants.h"
#include "globals.h"
#include "item.h"
#include "node.h"
#include "payload.h"
#include "process.h"
#include "protocol.h"
#include "push.h"

#include <assert.h>


void process_ack(client_t *client, header_t *header)
{
	int active;
	
	assert(client);
	assert(header);
	
	// if there are any special repcmds that we need to process, then we would add them here.  

	if (header->repcmd == CMD_SERVERHELLO) {
		active = node_active_inc();
		if (_verbose) printf("Active cluster node connections: %d\n", active);
	}
}




// check to see if this client has buckets we should promote.  
// Return 0 if we did not switch, 1 if we did.
static int attempt_switch(client_t *client) 
{
	bucket_t *bucket = NULL;
	int i;
	
	// go through all the buckets.
	for (i=0; i<=_mask && bucket == NULL; i++) {
		
		// do we have this bucket?
		if (_buckets[i]) {
			
			// are we primary?
			if (_buckets[i]->level == 0) {
				
				// is this client the backup node for this bucket?
				if (_buckets[i]->backup_node == client->node) {
					
					// yes it is, we can promote this bucket.
					bucket = _buckets[i];
					
					printf("[%u] Attempting to promote bucket #%X on '%s'\n",
					   _seconds, bucket->hash, ((node_t*)client->node)->name); 

					
					// we found a bucket we can promote, so we send out the command to start it.
					assert(0);
					
				}
			}
		}
	}
	
	
	// if we have a bucket, then we must have done a switch, so return 1.  Otherwise return 0.
	return (bucket ? 1 : 0);
}



// go through the list of buckets, and find one that doesn't have a backup copy.
static bucket_t * find_nobackup_bucket(void) 
{
	int i;
	bucket_t *bucket = NULL;
	
	for (i=0; i<=_mask && bucket == NULL; i++) {
		if (_buckets[i]) {
			if (_buckets[i]->level == 0) {
				if (_buckets[i]->backup_node == NULL) {
					bucket = _buckets[i];

					printf("[%u] Attempting to migrate bucket #%X that has no backup copy.\n",
					   _seconds, bucket->hash); 
					
					
					assert(bucket->hash == i);
					assert(bucket->transfer_client == NULL);
				}
			}
		}
	}
	
	return(bucket);
}




// 
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
						assert(_buckets[i]->target_node == NULL);
						assert(_buckets[i]->backup_node);
						
						if (_buckets[i]->backup_node != client->node) {
							bucket = _buckets[i];
						}
					}
					else {
						assert(_buckets[i]->target_node);
						assert(_buckets[i]->backup_node == NULL);
						
						if (_buckets[i]->target_node != client->node) {
							bucket = _buckets[i];
						}
					}
				}
			}
		}
	}

	return(bucket);
}




// this function is called when it receives a LOADLEVELS reply from a server node.  Based on that 
// reply, and the state of this currrent node, we determine if we are going to try and send a bucket 
// to that node or not.  If we are going to send the bucket to the node, we will send out a message 
// to the node, indicating what we are going to do, and then we wait for a reply back.  Once we 
// decide to send a bucket, we do not attempt to send buckets to any other node.
void process_loadlevels(client_t *client, header_t *header, void *ptr)
{
	char *next;
	int primary;
	int backups;
	int transferring;
	bucket_t *bucket = NULL;
	int ideal;
	
	assert(client);
	assert(header);
	assert(ptr);
	
	assert(client->node);
	
	next = ptr;

	// need to get the data out of the payload.
	primary = data_int(&next);
	backups = data_int(&next);
	transferring = data_int(&next);
	
	// if the target node is not currently transferring, and we are not currently transferring
	if (_bucket_transfer == 0 && transferring == 0) {

		
		printf("[%u] Processing loadlevel data from: '%s'\n",
			_seconds, ((node_t*)client->node)->name); 

		
		// first check to see if the target needs to have some buckets switched (if it has more secondaries than primaries)
		// before contemplating promoting any buckets, we need to make sure it wont destabilize us.
		if ((_primary_buckets-1 >= _secondary_buckets+1) && (backups > primary)) {
			if (_verbose > 2) 
				printf("[%u] Attempting to switch with '%s'\n",
					   _seconds, ((node_t*)client->node)->name); 
			if (attempt_switch(client) == 0) {
				// we started a promotion process, so we dont need to continue.
				assert(payload_length() == 0);
				return;
			}
		}
				
		// we havent sent anything yet, so now we need to check to see if we have any buckets 
		// that do not have backup copies (we do not care about the ideal number of buckets, we 
		// just need to make it a priority to get a second copy of these buckets out quick).
		assert(bucket == NULL);
		if (buckets_nobackup_count() > 0 && (primary+backups < _mask+1)) {
			bucket = find_nobackup_bucket();
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
			ideal = ((_mask+1) *2) / active;
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
		
		if (bucket) {
			// indicate which client is currently receiving a transfer.  This can be used if we 
			// need to cancel the transfer for some reason (either shutting down, or we are 
			// splitting the buckets)
			assert(bucket->transfer_client == NULL);
			bucket->transfer_client = client;
			
			assert(bucket->hash >= 0);
			assert(client->node);
			assert(((node_t*)client->node)->name);
			printf("[%u] Migrating bucket #%X to '%s'\n",
					   _seconds, bucket->hash, ((node_t*)client->node)->name); 
	
			// We know what level this bucket is, but we dont need to tell the target node yet 
			// because they dont need to know.  When we finalise the bucket migration we will 
			// tell them what it is and what the other (backup or source)nodes are.
			// So we dont tell them yet, we just send them the details of the bucket.
			push_accept_bucket(client, bucket->hash);
		}
	}
}



static void send_transfer_items(bucket_t *bucket)
{
	item_t **items;		// items we are going to send out. (NULL terminated array of item_t pointers)
	int avail;
	
	assert(bucket);
	assert(bucket->in_transit >= 0);
	assert(bucket->in_transit <= TRANSIT_MIN);
	assert(bucket->transfer_client);

	assert(bucket->data);

	avail = TRANSIT_MAX - bucket->in_transit;
	
	if (_verbose > 3) printf("Requesting %d items to migrate.\n", avail);
	
	// ask the data system for a certain number of migrate items.
	items = data_get_migrate_items(bucket->data, bucket->hash, avail);
	if (items == NULL) {
		// there are no more items to migrate.
		assert(0);
	}
	else {
		// we have some items to send.
		assert(0);
	}
}



/*
 * When we receive a reply of REPLY_ACCEPTING_BUCKET, we can start sending the bucket contents to 
 * this client.  This means that we first need to make a list of all the items that need to be sent.  
 * Then we need to send the first X messages (we send them in blocks).
 */
void process_accept_bucket(client_t *client, header_t *header, void *ptr)
{
	char *next;
	hash_t mask;
	hash_t hash;
	bucket_t *bucket;
	
	assert(client && header && ptr);
	assert(client->node);
	
	next = ptr;

	// need to get the data out of the payload.
	mask = data_int(&next);
	hash = data_int(&next);
	
	// get the bucket.
	assert(_buckets);
	assert(mask == _mask);
	bucket = _buckets[hash];
	assert(bucket);

	// increment the migrate_sync counter which will help indicate which items have already been 
	// sent or not.
	assert(_migrate_sync >= 0);
	_migrate_sync ++;
	assert(_migrate_sync > 0);
	
	// send the first queued item.
	send_transfer_items(bucket);
}




/* 
 * The other node now has control of the bucket, so we can clean it up and remove it completely..
 */
void process_control_bucket_complete(client_t *client, header_t *header, void *ptr)
{
	char *next;
	hash_t mask;
	hash_t hash;
	bucket_t *bucket;
	
	assert(client && header && ptr);
	assert(client->node);
	
	next = ptr;

	// need to get the data out of the payload.
	mask = data_int(&next);
	hash = data_int(&next);
	
	// get the bucket.
	assert(_buckets);
	assert(mask == _mask);
	assert(hash >= 0 && hash <= mask);
	bucket = _buckets[hash];
	assert(bucket);

	assert(bucket->transfer_client == client);
	bucket->transfer_client = NULL;
	
	if (_verbose > 2) 
		printf("Bucket migration complete: %X\n", hash);
	
	bucket_destroy(bucket);
	bucket = NULL;

	// now that this migration is complete, we need to ask for loadlevels again.
	push_loadlevels(client);
}



void process_unknown(client_t *client, header_t *header)
{
	assert(client);
	assert(header);

	// we sent a command, and the client didn't know what to do with it.  If we had certain modes 
	// we could enable for compatible capabilities (for this client), we would do it here.
	
	// during initial development, leave this here to catch protocol errors. but once initial 
	// development is complete, should not do an assert here.
	assert(0);
}





// the migration of the actual data for the bucket is complete, but now we need to complete the meta 
// process as well.  If this bucket is a primary bucket with no backups, then we tell the client 
// that it is a backup.   If we are a primary with backups, we need to tell the backup server that 
// this client will be its new source.  If it is a backup copy, then we tell the primary to send 
// backup data here.   Once we have started this process, we ignore any future SYNC data for this 
// bucket, and we do not send out SYNC data to the backup nodes.
static void finalize_migration(client_t *client, bucket_t *bucket) 
{
	assert(client);
	assert(bucket);

	if (bucket->level == 0 && bucket->backup_node == NULL) {
		// we are sending a nobackup bucket.
		
		// send a message to the client telling them that they are now a backup node for the bucket.
		push_control_bucket(client, bucket, 1);
	}
	else if (bucket->level == 0 && bucket->backup_node) {
		// we are sending a primary bucket.  

		// check that we dont have pending data to send to the backup node.
		assert(0);
		
		// send a message to the existing backup server, telling it that we are migrating the 
		// primary to a new node.
		assert(0);
	}
	else if (bucket->level == 1) {
		// we are sending a backup bucket. 
		
		// if we are a backup bucket, we should have a connection to the primary node.
		assert(bucket->target_node);
		
		// mark our bucket so that if we get new sync data, we can ignore it.
		assert(0);
		
		// send a message to the primary, telling it that we have migrated to the new node.
		assert(0);
		
	}
	else {
		assert(0);
	}
	

	// when we get the appropriate replies:
	// update the local hashmasks so we can inform clients who send data.
	// Dont destroy the bucket until the new node has indicatedestroy the bucket.
}
