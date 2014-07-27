// process replies received from commands sent.

#include "bucket.h"
#include "client.h"
#include "constants.h"
#include "header.h"
#include "item.h"
#include "logging.h"
#include "node.h"
#include "payload.h"
#include "process.h"
#include "protocol.h"
#include "push.h"
#include "server.h"

#include <assert.h>
#include <stdlib.h>




/*
static void process_ack(client_t *client, header_t *header)
{
	int active;
	
	assert(client);
	assert(header);
	
	// if there are any special repcmds that we need to process, then we would add them here.  

	if (header->repcmd == CMD_SERVERHELLO) {
		active = node_active_inc();
		logger(LOG_INFO, "Active cluster node connections: %d", active);
	}
}
*/


/*
// check to see if this client has buckets we should promote.  
// Return 0 if we did not switch, 1 if we did.
static int attempt_switch(client_t *client) 
{
	assert(client);
	
	// should only be doing this if the client is also a server node.
	assert(client->node);
	const char *name = node_name(client->node);
	
	bucket_t *bucket = buckets_find_switchable(client->node);
	if (bucket) {

		logger(LOG_INFO, "Attempting to promote bucket #%#llx on '%s'",
			bucket->hashmask, name); 

		buckets_set_transferring(bucket, client);
		
		// we found a bucket we can promote, so we send out the command to start it.
		push_control_bucket(client, bucket->hashmask, bucket->level);
		
		return(1);
	}
	else {
		return(0);
	}
}
*/









/*
// this function is called when it receives a LOADLEVELS reply from a server node.  Based on that 
// reply, and the state of this currrent node, we determine if we are going to try and send a bucket 
// to that node or not.  If we are going to send the bucket to the node, we will send out a message 
// to the node, indicating what we are going to do, and then we wait for a reply back.  Once we 
// decide to send a bucket, we do not attempt to send buckets to any other node.
static void process_loadlevels(client_t *client, header_t *header, void *ptr)
{
	char *next;
	int primary;
	int backups;
	int transferring;
	bucket_t *bucket = NULL;
	int switching = 0;
	
	assert(client);
	assert(header);
	assert(ptr);
	
	assert(client->node);
	
	// point to the begining of the payload data.
	next = ptr;

	// need to get the data out of the payload.
	primary = data_int(&next);
	backups = data_int(&next);
	transferring = data_int(&next);
	
	if (transferring == 0) {

		// first check to see if the target needs to have some buckets switched (if it has more secondaries than primaries)
		// before contemplating promoting any buckets, we need to make sure it wont destabilize us.
		if ((buckets_get_primary_count()-1 >= buckets_get_secondary_count()+1) && (backups > primary)) {
			if (attempt_switch(client) == 1) {
				// we started a promotion process, so we dont need to continue.
				assert(payload_length() == 0);
				assert(switching == 0);
				switching = 1;
			}
		}
		
		if (switching == 0) {
		
			assert(client->node);
			logger(LOG_DEBUG, "Processing loadlevel data from: '%s'", node_name(client->node)); 
			
			bucket = buckets_check_loadlevels(client, primary, backups);
				
			if (bucket) {
				// indicate which client is currently receiving a transfer.  This can be used if we 
				// need to cancel the transfer for some reason (either shutting down, or we are 
				// splitting the buckets)
				assert(bucket->transfer_client == NULL);
				bucket->transfer_client = client;
				assert(client);

				assert(bucket->hashmask >= 0);
				assert(client->node);
				logger(LOG_DEBUG, "Migrating bucket #%#llx to '%s'", bucket->hashmask, node_name(client->node)); 

				buckets_set_transferring(bucket, client);
				
				// We know what level this bucket is, but we dont need to tell the target node yet 
				// because they dont need to know.  When we finalise the bucket migration we will 
				// tell them what it is and what the other (backup or source)nodes are.
				// So we dont tell them yet, we just send them the details of the bucket.
				push_accept_bucket(client, bucket->hashmask);
			}
		}
	}
}
*/




/*
// the migration of the actual data for the bucket is complete, but now we need to complete the meta 
// process as well.  If this bucket is a primary bucket with no backups, then we tell the client 
// that it is a backup.   If we are a primary with backups, we need to tell the backup server that 
// this client will be its new source.  If it is a backup copy, then we tell the primary to send 
// backup data here.   Once we have started this process, we ignore any future SYNC data for this 
// bucket, and we do not send out SYNC data to the backup nodes.
static void finalize_migration(client_t *client, bucket_t *bucket) 
{
	conninfo_t *conninfo=NULL;
	
	assert(client);
	assert(bucket);

	if (bucket->level == 0) {
		if ( bucket->backup_node == NULL) {
			// we are sending a nobackup bucket.
			
			// send a message to the client telling them that they are now a backup node for the bucket.
			assert(client == bucket->transfer_client);
			
			// if we are sending a primary bucket, then there must be a backup node somewhere. If we are 
			// sending a backup node, then there could be a source node elsewhere (moving a bucket), or we 
			// are the source (bucket had no backup).

			if (bucket->level == 0) {
				assert(bucket->primary_node);
				assert(bucket->primary_node->conninfo);
				assert(conninfo == NULL);
				conninfo = bucket->primary_node->conninfo;
			}
			else if (bucket->level == 1) {
				if (bucket->source_node) {
					assert(bucket->secondary_node);
					assert(bucket->secondary_node->conninfo);
					assert(conninfo == NULL);
					conninfo = bucket->secondary_node->conninfo;
				}
				else {
					assert(conninfo == NULL);
					conninfo = server_conninfo();
				}
			}
			else {
				assert(conninfo == NULL);
				conninfo = server_conninfo();
			}
			
			assert(conninfo);
			push_finalise_migration(client, bucket->hashmask, conninfo_str(conninfo), 1);
		}
		else if (bucket->backup_node) {
			// we are sending a primary bucket.  

			// check that we dont have pending data to send to the backup node.
			assert(0);
			
			// send a message to the existing backup server, telling it that we are migrating the 
			// primary to a new node.
			assert(0);
		}
	}
	else if (bucket->level == 1) {
		// we are sending a backup bucket. 
		
		// if we are a backup bucket, we should have a connection to the primary node.
		assert(bucket->source_node);
		
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
*/


/*
static void send_transfer_items(bucket_t *bucket)
{
	int avail;
	int items;
	
	assert(bucket);
	assert(data_in_transit() >= 0);
	assert(data_in_transit() <= TRANSIT_MIN);
	assert(bucket->transfer_client);
	assert(TRANSIT_MAX >= TRANSIT_MIN);
	assert(TRANSIT_MIN >= 0);

	avail = TRANSIT_MAX - data_in_transit();
	
	logger(LOG_DEBUG, "Requesting %d items to migrate.", avail);
	
	// ask the data system for a certain number of migrate items.
	assert(bucket->data);
	assert(avail > 0);
	items = data_migrate_items(bucket->transfer_client, bucket->data, bucket->hashmask, avail);
	if (items <= 0) {
		// there are no more items to migrate.
		assert(bucket);
		assert(bucket->transfer_client);
		assert(data_in_transit() == 0);
		finalize_migration(bucket->transfer_client, bucket);
	}
}
*/


/*
 * When we receive a reply of REPLY_ACCEPTING_BUCKET, we can start sending the bucket contents to 
 * this client.  This means that we first need to make a list of all the items that need to be sent.  
 * Then we need to send the first X messages (we send them in blocks).
 */
/*
static void process_accept_bucket(client_t *client, header_t *header, void *ptr)
{
	char *next;
	hash_t mask;
	hash_t hash;
	bucket_t *bucket;
	
	assert(client && header && ptr);
	assert(client->node);
	
	next = ptr;

	// need to get the data out of the payload.
	mask = data_long(&next);
	hash = data_long(&next);

	logger(LOG_DEBUG, "Accept Bucket: current mask=%#llx, bucket mask=%#llx", _mask, mask);
	
	// get the bucket.
	assert(_buckets);
	assert(mask == _mask);
	bucket = _buckets[hash];
	assert(bucket);

	assert(bucket->transfer_client == client);

	// increment the migrate_sync counter which will help indicate which items have already been 
	// sent or not.
	assert(_migrate_sync >= 0);
	_migrate_sync ++;
	assert(_migrate_sync > 0);
	
	logger(LOG_DEBUG, "Setting Migration SYNC counter to: %d", _migrate_sync);

	assert(data_in_transit() == 0);
	
	// send the first queued item.
	send_transfer_items(bucket);
}
*/


/*
static void process_control_bucket_complete(client_t *client, header_t *header, void *ptr)
{
	char *next;
	hash_t mask;
	hash_t hash;
	bucket_t *bucket;
	
	assert(client && header && ptr);
	assert(client->node);
	
	next = ptr;

	// need to get the data out of the payload.
	mask = data_long(&next);
	hash = data_long(&next);
	
	// get the bucket.
	assert(_buckets);
	assert(mask == _mask);
	assert(hash >= 0 && hash <= mask);
	bucket = _buckets[hash];
	assert(bucket);
	assert(bucket->hash == hash);

	assert(bucket->transfer_client == client);
	bucket->transfer_client = NULL;
	assert(client);
	assert(client->node);
	assert(((node_t*)client->node)->details.name);
	logger(LOG_DEBUG, "Removing transfer client ('%s') from bucket %#llx.", ((node_t*)client->node)->details.name, bucket->hash);
	
	logger(LOG_INFO, "Bucket switching complete: %#llx", hash);

	// do we need to let other nodes that the transfer is complete?

	// we are switching a bucket.  So if we are currently primary, then we need to switch to 
	// secondary, and vice-versa.
	
	if (bucket->level == 0) {
		bucket->level = 1;
		_primary_buckets --;
		_secondary_buckets ++;
		
		assert(bucket->backup_node);
		assert(bucket->source_node == NULL);
		bucket->source_node = bucket->backup_node;
		bucket->backup_node = NULL;
		
		assert(_primary_buckets >= 0);
		assert(_secondary_buckets > 0);
	}
	else {
		assert(bucket->level == 1);
		bucket->level = 0;
		_primary_buckets ++;
		_secondary_buckets --;

		assert(bucket->backup_node == NULL);
		assert(bucket->source_node);
		bucket->backup_node = bucket->source_node;
		bucket->source_node = NULL;
		
		assert(_primary_buckets > 0);
		assert(_secondary_buckets >= 0);
	}
	
	// since we are switching, need to swap the hashmask entries around.
	hashmask_switch(hash);
	
	// Tell all our clients that the hashmasks are changing.
	assert(0);
//	push_hashmask_update(bucket);
	
	assert(_bucket_transfer == 1);
	_bucket_transfer = 0;

	// now that this migration is complete, we need to ask for loadlevels again.
	push_loadlevels(client);
}
*/



/* 
 * The other node now has control of the bucket, so we can clean it up and remove it completely..
 */
/*
static void process_migration_ack(client_t *client, header_t *header, void *ptr)
{
	char *next;
	hash_t mask;
	hash_t hash;
	bucket_t *bucket;
	
	assert(client && header && ptr);
	assert(client->node);
	
	// need to get the data out of the payload.
	next = ptr;		// the 'next' pointer gets moved to the next param each time.
	mask = data_long(&next);
	hash = data_long(&next);
	
	// get the bucket.
	assert(_buckets);
	assert(mask == _mask);
	assert(hash >= 0 && hash <= mask);
	bucket = _buckets[hash];
	assert(bucket);

	assert(bucket->transfer_client == client);
	bucket->transfer_client = NULL;
	assert(client);
	assert(client->node);
	logger(LOG_DEBUG, "Removing transfer client ('%s') from bucket %#llx.", node_getname(client->node), bucket->hash);
	
	logger(LOG_INFO, "Bucket migration complete: %#llx", hash);

	// do we need to let other nodes that the transfer is complete?
	
// #error "yes, we need to push the hashmask list to all the clients.... or at least this entry.  need to look at the protocol."

	// if we transferred a backup node, or we transferred a primary that already has a backup node, 
	// then we dont need this copy of the bucket anymore, so we can delete it.
	if (bucket->level == 0 && bucket->backup_node == NULL) {
		assert(client->node);
		bucket->backup_node = client->node;
		assert(_nobackup_buckets > 0);
		_nobackup_buckets --;
		assert(_nobackup_buckets >= 0);
	}
	else {
		if (bucket->level == 0) {
			_primary_buckets --;
		}
		else {
			assert(bucket->level == 1);
			_secondary_buckets --;
		}
		
		bucket_destroy_contents(bucket);
		bucket = NULL;
	}
	
	assert(_bucket_transfer == 1);
	_bucket_transfer = 0;

	// now that this migration is complete, we need to ask for loadlevels again.
	push_loadlevels(client);
}
*/






static void process_unknown(client_t *client, header_t *header)
{
	assert(client);
	assert(header);

	// we sent a command, and the client didn't know what to do with it.  If we had certain modes 
	// we could enable for compatible capabilities (for this client), we would do it here.
	
	// during initial development, leave this here to catch protocol errors. but once initial 
	// development is complete, should not do an assert here.
	assert(0);
}







/*
static void process_sync_name_ack(client_t *client, header_t *header, void *ptr)
{
	char *next;
	hash_t hash;
	bucket_t *bucket;
	int index;

	assert(client && header && ptr);
	assert(client->node);
	
	// need to get the data out of the payload.
	next = ptr;
	hash = data_long(&next);
	index = hash & _mask;
		
	// get the bucket.
	assert(_buckets);
	assert(index >= 0 && index <= _mask);
	bucket = _buckets[index];
	assert(bucket);

	// ** This assert doesnt seem to be correct when we lose a client connection.  Not sure what is happening here.
	assert(client);
	assert(bucket->backup_node);
	assert(bucket->backup_node->client == client);
	logger(LOG_DEBUG, "Sync of item name complete: %#llx", hash);
}
*/



/*
static void process_sync_ack(client_t *client, header_t *header, void *ptr)
{
	char *next;
	hash_t hash;
	hash_t map;
	bucket_t *bucket;
	int index;

	assert(client && header && ptr);
	assert(client->node);
	
	// need to get the data out of the payload.
	next = ptr;
	map = data_long(&next);
	hash = data_long(&next);
	
	logger(LOG_DEBUG, "PROCESS sync_ack. map=%#llx, hash=%#llx", map, hash);
	
	index = hash & _mask;
	assert(index >= 0 && index <= _mask);
		
	// get the bucket.
	assert(_buckets);
	bucket = _buckets[index];
	assert(bucket);
	assert(bucket->backup_node);
	assert(bucket->backup_node->client == client);

	logger(LOG_DEBUG, "Sync of item complete: %#llx", hash);
}
*/



/*
static void process_migrate_name_ack(client_t *client, header_t *header, void *ptr)
{
	char *next;
	hash_t hash;
	bucket_t *bucket;
	int index;

	assert(client && header && ptr);
	assert(client->node);
	
	// need to get the data out of the payload.
	next = ptr;
	hash = data_long(&next);
	index = hash & _mask;

	// get the bucket.
	assert(_buckets);
	assert(index >= 0 && index <= _mask);
	bucket = _buckets[index];
	assert(bucket);
	
	logger(LOG_DEBUG, "Migration of item name complete: bucket=%#llx, hash=%#llx", bucket->hash, hash);

	// ** This assert doesnt seem to be correct when we lose a client connection.  Not sure what is happening here.
	assert(client);
	assert(bucket->transfer_client == client);
	logger(LOG_DEBUG, "Migration of item name complete: %#llx", hash);
}
*/

/*
static void process_migrate_ack(client_t *client, header_t *header, void *ptr)
{
	char *next;
	hash_t map;
	hash_t hash;
	bucket_t *bucket;
	int index;

	assert(client && header && ptr);
	assert(client->node);
	
	// need to get the data out of the payload.
	next = ptr;
	map = data_long(&next);
	hash = data_long(&next);
	
	index = hash & _mask;
	assert(index >= 0 && index <= _mask);
		
	// get the bucket.
	assert(_buckets);
	bucket = _buckets[index];
	assert(bucket);

	assert(client == bucket->transfer_client);
	
	// this was a result of a migration, so we need to continue migrating.
	data_migrated(bucket->data, map, hash);
	data_in_transit_dec();
	
	// send another if there is one more available.
	send_transfer_items(bucket);

	logger(LOG_DEBUG, "Migration of item complete: %#llx", hash);
}
*/


// Add the reply processor callbacks to the client list.
void process_init(void) 
{
// 	client_add_reply(REPLY_ACK, process_ack);
// 	client_add_reply(REPLY_SYNC_NAME_ACK, process_sync_name_ack);
// 	client_add_(REPLY_SYNC_ACK, process_sync_ack);
// 	client_add_cmd(REPLY_MIGRATE_NAME_ACK, process_migrate_name_ack);
// 	client_add_cmd(REPLY_MIGRATE_ACK, process_migrate_ack);
// 	client_add_cmd(REPLY_LOADLEVELS, process_loadlevels);
// 	client_add_cmd(REPLY_ACCEPTING_BUCKET, process_accept_bucket);
// 	client_add_cmd(REPLY_CONTROL_BUCKET_COMPLETE, process_control_bucket_complete);
// 	client_add_cmd(REPLY_MIGRATION_ACK, process_migration_ack);

	client_add_special(RESPONSE_UNKNOWN, process_unknown);
}




