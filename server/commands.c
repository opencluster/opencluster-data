// commands.c

#include "bucket.h"
#include "client.h"
#include "commands.h"
#include "globals.h"
#include "header.h"
#include "logging.h"
#include "payload.h"
#include "protocol.h"
#include "push.h"
#include "server.h"
#include "timeout.h"
#include "value.h"

#include <assert.h>
#include <stdlib.h>
#include <string.h>



// external references...
extern bucket_t ** _buckets;
extern hashmask_t ** _hashmasks;




// Get a value from storage.
static void cmd_get_int(client_t *client, header_t *header, char *payload)
{
	char *next;
	hash_t map_hash;
	hash_t key_hash;
	value_t *value;
	
	assert(client);
	assert(header);
	assert(payload);

	next = payload;
	
	map_hash = data_long(&next);
	key_hash = data_long(&next);
	
	logger(LOG_INFO, "CMD: get (integer) [%#llx/%#llx]", map_hash, key_hash);
	value = buckets_get_value(map_hash, key_hash);
	
	// send the ACK reply.
	if (value) {
		if (value->type != VALUE_INT) {
			// need to indicate stored value is a different type.
			assert(0);
		}
		else {
			// we have the data, build the reply.
			assert(payload_length() == 0);
			payload_long(map_hash);
			payload_long(key_hash);
			payload_int(value->data.i);

			assert(payload_length() > 0);
			client_send_message(client, header, REPLY_DATA_INT, payload_length(), payload_ptr());
			payload_clear();
		}
	}
	else {
		logger(LOG_DEBUG, "CMD: get (integer) FAILED [%#llx/%#llx]", map_hash, key_hash);
		// the data they are looking for is not here.
		client_send_message(client, header, REPLY_FAIL, 0, NULL);
	}
	
	assert(payload_length() == 0);
}



// Get a value from storage.
static void cmd_get_str(client_t *client, header_t *header, char *payload)
{
	char *next;
	hash_t map_hash;
	hash_t key_hash;
	value_t *value;
	
	assert(client);
	assert(header);
	assert(payload);

	next = payload;
	
	map_hash = data_long(&next);
	key_hash = data_long(&next);
	
	value = buckets_get_value(map_hash, key_hash);
	
	// send the ACK reply.
	if (value) {
		if (value->type != VALUE_STRING) {
			// need to indicate stored value is a different type.
			assert(0);
		}
		else {
			// build the reply.
			assert(payload_length() == 0);
			payload_long(map_hash);
			payload_long(key_hash);
			payload_data(value->data.s.length, value->data.s.data);
			
			assert(payload_length() > 0);
			client_send_message(client, header, REPLY_DATA_STR, payload_length(), payload_ptr());
			payload_clear();
		}
	}
	else {
		// the data they are looking for is not here.
		client_send_message(client, header, REPLY_FAIL, 0, NULL);
	}

	assert(payload_length() == 0);
}



// Set a value into the hash storage.
static void cmd_set_str(client_t *client, header_t *header, char *payload)
{
	char *next;
	hash_t map_hash;
	hash_t key_hash;
	int expires;
	value_t *value;
	char *str;
	char *name;
	int name_len;
	int result;
	int str_len;
	
	assert(client);
	assert(header);
	assert(payload);

	// create a new value.
	value = calloc(1, sizeof(value_t));
	assert(value);
	
	next = payload;
	
	map_hash = data_long(&next);
	key_hash = data_long(&next);
	expires = data_int(&next);

	str = data_string(&next, &name_len);
	assert(str && name_len > 0);
	name = malloc(name_len + 1);
	memcpy(name, str, name_len);
	name[name_len] = 0;

	value->type = VALUE_STRING;
	str = data_string(&next, &str_len);
	value->data.s.data = malloc(str_len + 1);
	memcpy(value->data.s.data, str, str_len);
	value->data.s.data[str_len] = 0;
	value->data.s.length = str_len;
	
	// store the value into the trees.  If a value already exists, it will get released and this one 
	// will replace it, so control of this value is given to the tree structure.
	// NOTE: value is controlled by the tree after this function call.
	// NOTE: name is controlled by the tree after this function call.
	result = buckets_store_value(map_hash, key_hash, name, 0, expires, value);
	value = NULL;
	name = NULL;
	
	// send the ACK reply.
	if (result == 0) {
		client_send_message(client, header, REPLY_ACK, 0, NULL);
		payload_clear();
	}
	else {
		assert(0);
	}
	
	assert(payload_length() == 0);
}



static void cmd_loadlevels(client_t *client, header_t *header)
{
	assert(client);
	assert(header);

	assert(payload_length() == 0);
	
	assert(_primary_buckets >= 0);
	assert(_secondary_buckets >= 0);
	assert(_bucket_transfer >= 0);
	payload_int(_primary_buckets);
	payload_int(_secondary_buckets);
	payload_int(_bucket_transfer);
	
	// send the reply.
	client_send_message(client, header, REPLY_LOADLEVELS, payload_length(), payload_ptr());

	payload_clear();
	
	assert(_hashmasks);
}




// Get a value from storage.
static void cmd_accept_bucket(client_t *client, header_t *header, char *payload)
{
	char *next;
	hash_t mask;
	hash_t key_hash;
	int reply = 0;
	bucket_t *bucket;
	
	assert(client);
	assert(header);
	assert(payload);

	next = payload;
	
	mask = data_long(&next);
	key_hash = data_long(&next);
	
	logger(LOG_INFO, "CMD: accept bucket (%#llx/%#llx)", mask, key_hash);

	assert(payload_length() == 0);

	// regardless of the reply, the payload will be the same.
	payload_long(mask);
	payload_long(key_hash);

	if (_bucket_transfer != 0) {
		//  we are currently transferring another bucket, therefore we cannot accept another one.
		logger(LOG_WARN, "cant accept bucket, already transferring one.");
		reply = REPLY_CANT_ACCEPT_BUCKET;
	}
	else { 
		assert(_bucket_transfer == 0);
		if (mask != _mask) {
			// the masks are different, we cannot accept a bucket unless our masks match... which should 
			// balance out once hashmasks have proceeded through all the nodes.
			logger(LOG_WARN, "cant accept bucket, masks are not compatible.");
			reply = REPLY_CANT_ACCEPT_BUCKET;
		}
		else {
			// now we need to check that this bucket isn't already handled by this server.
			assert(key_hash <= mask);
			assert(_mask == mask);
			
			assert(_hashmasks);
			
			// if we dont currently have a buckets list, we need to create one.
			if (_buckets == NULL) {
				_buckets = calloc(_mask + 1, sizeof(bucket_t *));
				assert(_buckets);
				assert(_buckets[0] == NULL);
				assert(_primary_buckets == 0);
				assert(_secondary_buckets == 0);
			}
			
			assert(_buckets);
			if (_buckets[key_hash]) {
				logger(LOG_ERROR, "cant accept bucket, already have that bucket.");
				reply = REPLY_CANT_ACCEPT_BUCKET;
			}
			else {
				logger(LOG_INFO, "accepting bucket.");
				reply = REPLY_ACCEPTING_BUCKET;
				
				assert(_bucket_transfer == 0);
				_bucket_transfer = 1;
				
				// need to create the bucket, and mark it as in-transit.
				bucket = bucket_new(key_hash);
				_buckets[key_hash] = bucket;
				assert(data_in_transit() == 0);
				bucket->transfer_client = client;
				assert(bucket->level < 0);
			}
		}
	}

	assert(payload_length() > 0);
	assert(reply > 0);
	client_send_message(client, header, reply, payload_length(), payload_ptr());

	payload_clear();
}




// Set a value into the hash storage.
static void cmd_set_int(client_t *client, header_t *header, char *payload)
{
	char *next;
	hash_t map_hash;
	hash_t key_hash;
	int expires;
	value_t *value;
	char *str;
	char *name;
	int name_len;
	int result;
	
	assert(client);
	assert(header);
	assert(payload);

	// create a new value.
	// ** PERF: get the 'value' objects from a pool to improve performance.
	value = calloc(1, sizeof(value_t));
	assert(value);
	
	next = payload;
	
	map_hash = data_long(&next);
	key_hash = data_long(&next);
	expires = data_int(&next);
	str = data_string(&next, &name_len);
	value->type = VALUE_INT;
	value->data.i = data_int(&next);
	
	assert(str && name_len > 0);
	
	name = malloc(name_len + 1);
	memcpy(name, str, name_len);
	name[name_len] = 0;
	
	logger(LOG_DEBUG, "CMD: set (integer): [%#llx/%#llx]'%s'=%d", map_hash, key_hash, name, value->data.i);


	// store the value into the trees.  If a value already exists, it will get released and this one 
	// will replace it, so control of this value is given to the tree structure.
	// NOTE: value is controlled by the tree after this function call.
	// NOTE: name is controlled by the tree after this function call.
	result = buckets_store_value(map_hash, key_hash, name, 0, expires, value);
	value = NULL;
	name = NULL;
	
	// send the ACK reply.
	if (result == 0) {
		client_send_message(client, header, REPLY_ACK, 0, NULL);
		assert(payload_length() == 0);
	}
	else {
		assert(0);
	}
	
	assert(payload_length() == 0);
}



static void cmd_ping(client_t *client, header_t *header)
{
	assert(client);
	assert(header);

	// there is no payload required for an ack.
	
	// send the ACK reply.
	client_send_message(client, header, REPLY_ACK, 0, NULL);
}


static void cmd_goodbye(client_t *client, header_t *header)
{
	assert(client);
	assert(header);

	logger(LOG_INFO, "CMD: goodbye");
	
	client_closing(client);
	
	// send the ACK reply.
	client_send_message(client, header, REPLY_ACK, 0, NULL);
}



static void cmd_serverhello(client_t *client, header_t *header, char *payload)
{
	char *next;
	char *name = NULL;
	node_t *node = NULL;
	
	assert(client);
	assert(header);
	assert(payload);

	next = payload;
	
	assert(client->node == NULL);
	
	// the only parameter is a string indicating the servers connection data.  
	// Normally an IP and port.
	name = data_string_copy(&next);
	assert(name);

	
	// we have a server name.  We need to check it against out node list.  If it is there, then we 
	// dont do anything.  If it is not there, then we need to add it.
	node = node_find(name);
	if (node == NULL) {
		// the node was not found in the list.  We need to add it to the list, and add this client 
		// to the node.

		node = node_add(client, name);
		assert(node);

		// need to send to the node, the list of hashmasks.
		push_hashmasks(client);
		
		// this is a new server we didn't know about, so we need to send this info to our connected clients as well.
		push_all_newserver(name, client);
		
		
		logger(LOG_INFO, "Adding '%s' as a New Node.", name);
	}
	else {
		
		// the node was found in the list.  Now we need to remove the existing one, and replace it 
		// with this new one.
		assert(node->client);
		if (node->client == client) {
			// is this even possible?
			assert(0);
		}
		else {
			
			if (node->client->read_event) {
				// this client object already exists and is reading data.  We need to close this.
				assert(0);
			}
			else {
				if (node->connect_event) {
					assert(node->client->handle > 0);
					assert(node->wait_event == NULL);
					
					// this client is in the middle of connecting, and we've received a connection at the same time.
					assert(0);
				}
				else {
					assert(node->connect_event == NULL);
					assert(node->wait_event);
					assert(node->client->read_event == NULL);
					assert(node->client->handle == -1);
					
					// need to send to the node, the list of hashmasks.
					push_hashmasks(client);
					
					// the client is waiting to connect, so we can break it down.
					client_free(node->client);
					assert(node->client == NULL);
					client->node = node;
	}	}	}	}

	// since this connection is a client, we need to set a 'loadlevel' timer.
	assert(node);
	assert(node->loadlevel_event == NULL);
	assert(_evbase);
	node->loadlevel_event = evtimer_new(_evbase, node_loadlevel_handler, (void *) node);
	assert(node->loadlevel_event);
	evtimer_add(node->loadlevel_event, &_timeout_node_loadlevel);

	// send the ACK reply.
	client_send_message(client, header, REPLY_ACK, 0, NULL);
	
	free(name);
	name=NULL;
}



static void cmd_serverinfo(client_t *client, header_t *header, char *payload)
{
	char *next;
	char *name = NULL;
	node_t *node = NULL;
	
	assert(client);
	assert(header);
	assert(payload);

	next = payload;
	
	assert(client->node == NULL);
	
	// the only parameter is a string indicating the servers connection data.  
	// Normally an IP and port.
	name = data_string_copy(&next);
	assert(name);

	
	// we have a server name.  We need to check it against out node list.  If it is there, then we 
	// dont do anything.  If it is not there, then we need to add it.
	node = node_find(name);
	if (node == NULL) {
		// the node was not found in the list.  We need to add it to the list,

		node = node_new(name);
		assert(node);
		
		// this is a new server we didn't know about, so we need to send this info to our connected clients as well.
		push_all_newserver(name, client);
		
		
		logger(LOG_INFO, "Adding '%s' as a New Node.", name);
	}

	// send the ACK reply.
	client_send_message(client, header, REPLY_ACK, 0, NULL);
	
	free(name);
	name=NULL;
}








/* 
 * This command is recieved from other nodes, which describe a bucket that node handles.
 * Over-write current internal data with whatever is in here.
 */
static void cmd_hashmask(client_t *client, header_t *header, char *payload)
{
	char *next;
	hash_t mask;
	hash_t hash;
	int level;
	node_t *node;
	
	assert(client);
	assert(header);
	assert(payload);

	assert(client->node);

	// get the data out of the payload.
	next = payload;
	mask = data_long(&next);
	hash = data_long(&next);
	level = data_int(&next);

	// check integrity of the data provided.
	assert(mask != 0);
	assert(hash >= 0 && hash <= mask);
	assert(level == -1 || level >= 0);
	
	// check that the mask is the same as our existing mask... 

	logger(LOG_DEBUG, "debug: cmd_hashmask.  orig mask:%#llx, new mask:%#llx", _mask, mask);
	
	if (mask < _mask) {
		// if the mask supplied is LESS than the mask we use, then when we read in the data, we need 
		// to convert it to the new mask as we process it.
		assert(0);
	}
	else if (mask > _mask) {
		// if the mask supplied is GREATER than the mask we use, we need to first permenantly update 
		// our own mask to match the new one received.  Then process the incoming data.
		buckets_split_mask(mask);
		assert(_mask == mask);
	}
	else {
		// the masks match, so the data should be ok.
	}
	
	// now we process the bucket info that was provided.
	if (level < 0) {
		// the hashmask is being deleted.
		assert(0);
	}

	// these entries should only be coming from server nodes;
	assert(client->node);
	node = client->node;
	assert(node->name);

	if (level == 0) {
		assert(_hashmasks);
		assert(_hashmasks[hash]);
		if (_hashmasks[hash]->primary) { free(_hashmasks[hash]->primary); }
		_hashmasks[hash]->primary = strdup(node->name);
		
		logger(LOG_DEBUG, "Setting HASHMASK: Primary [%#llx] = '%s'",
			hash, _hashmasks[hash]->primary
		);
	}
	else if (level == 1) {
		assert(_hashmasks);
		assert(_hashmasks[hash]);
		if (_hashmasks[hash]->secondary) { free(_hashmasks[hash]->secondary); }
		_hashmasks[hash]->secondary = strdup(node->name);

		logger(LOG_DEBUG, "Setting HASHMASK: Secondary [%#llx] = '%s'",
			hash, _hashmasks[hash]->primary
		);
	}
	else {
		assert(0);
	}

	// send the ACK reply.
	client_send_message(client, header, REPLY_ACK, 0, NULL);
}





// this node, and the other node have the same bucket (one is primary, one is secondary), and one is 
// telling the other to switch.
static void cmd_control_bucket(client_t *client, header_t *header, char *payload)
{
	char *next;
	hash_t mask;
	hash_t key_hash;
	int level;
	int reply = 0;
	bucket_t *bucket = NULL;
	
	assert(client);
	assert(header);
	assert(payload);

	next = payload;
	mask = data_long(&next);
	key_hash = data_long(&next);
	level = data_int(&next);
	
	logger(LOG_INFO, "CMD: bucket control (%#llx/%#llx), level:%d", mask, key_hash, level);

	// regardless of the reply, the payload will be the same.
	assert(payload_length() == 0);
	payload_long(mask);
	payload_long(key_hash);

	// make sure that the masks are the same.
	if (mask != _mask) {
		reply = REPLY_CONTROL_BUCKET_FAILED;
		assert(bucket == NULL);
	}
	else {
		reply = REPLY_CONTROL_BUCKET_COMPLETE;
		
		assert(_buckets);
		assert(key_hash <= _mask);
		assert(key_hash >= 0);

		// ** some of these values should be checked at runtime
		bucket = _buckets[key_hash];
		assert(bucket);
		assert(bucket->hash == key_hash);
		assert(bucket->transfer_client == NULL);
		assert(data_in_transit() == 0);
		assert(bucket->transfer_event == NULL);
		
		// mark the bucket as ready for action.
		if (level == 0) {
			// we are switching a bucket from secondary to primary.
			assert(bucket->level == 1);
			assert(bucket->backup_node == NULL);
			assert(bucket->target_node);
			assert(bucket->target_node->client == client);
			bucket->backup_node = bucket->target_node;
			bucket->target_node = NULL;
			
			_primary_buckets ++;
			_secondary_buckets --;
			assert(_primary_buckets > 0);
			assert(_secondary_buckets >= 0);
		}
		else if (level == 1)  {
			// we are switching a bucket from primary to secondary.  
			assert(bucket->level == 0);
			
			// we should be connected to this other node.
			assert(bucket->target_node == NULL);
			assert(bucket->backup_node);
			assert(bucket->backup_node->client == client);
			bucket->target_node = bucket->backup_node;
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
		
		hashmask_switch(key_hash);
	}

	assert(payload_length() > 0);
	assert(reply > 0);
	client_send_message(client, header, reply, payload_length(), payload_ptr());
	payload_clear();

	// since this node is receiving the 'switch' command, we should not have any buckets transferring.
	assert(_bucket_transfer == 0);
	
	if (bucket) {
		// send message to all other connected clients and nodes alerting them to the change in buckets.
		assert(bucket->level >= 0);
		push_hashmask_update(bucket);
	}
}




// a bucket is being migrated to this server, and the other node is telling us that all the data has 
// been transferred, and now it is passing control to this node.  When this operation is complete, 
// then the other node will finalize its part in the migration, and migration will be over.
static void cmd_finalise_migration(client_t *client, header_t *header, char *payload)
{
	char *next;
	hash_t mask;
	hash_t key_hash;
	int level;
	char *remote_host = NULL;
	int reply = 0;
	bucket_t *bucket = NULL;
	
	assert(client);
	assert(header);
	assert(payload);

	next = payload;
	
	mask        = data_long(&next);
	key_hash    = data_long(&next);
	level       = data_int(&next);
	remote_host = data_string_copy(&next);
	
	logger(LOG_INFO, "CMD: finalise migration (%#llx/%#llx), level:%d, remote:'%s'", mask, key_hash, level, remote_host);

	// regardless of the reply, the payload will be the same.
	assert(payload_length() == 0);
	payload_long(mask);
	payload_long(key_hash);

	// make sure that the masks are the same.
	if (mask != _mask) {
		reply = REPLY_MIGRATION_FAIL;
		
		// since we are not going to be using the remote_host, then we should free it now.
		if (remote_host) {
			free(remote_host);
			remote_host = NULL;
		}
	
		assert(bucket == NULL);
	}
	else {
		reply = REPLY_MIGRATION_ACK;
		
		assert(_buckets);
		assert(key_hash <= _mask);
		assert(key_hash >= 0);

		// ** some of these values should be checked at runtime
		bucket = _buckets[key_hash];
		assert(bucket);
		assert(bucket->hash == key_hash);
		assert(bucket->transfer_client == client);
		assert(bucket->level < 0);
		assert(bucket->target_node == NULL);
		assert(bucket->backup_node == NULL);
		assert(data_in_transit() == 0);
		assert(bucket->transfer_event == NULL);
		assert(_bucket_transfer == 1);
		
		// mark the bucket as ready for action.
		bucket->level = level;
		if (level == 0) {
			// we are receiving a primary bucket.  
			if (remote_host) {
				assert(bucket->backup_node == NULL);
				bucket->backup_node = node_find(remote_host);
				assert(bucket->backup_node);
				assert(bucket->backup_node->client);
			}
			else {
				assert(bucket->backup_node == NULL);
			}
			assert(bucket->target_node == NULL);
			
			assert(_hashmasks);
			assert(_hashmasks[key_hash]);
			if (_hashmasks[key_hash]->primary) {
				free(_hashmasks[key_hash]->primary);
			}
			_hashmasks[key_hash]->primary = strdup(_interface);
			
			_primary_buckets ++;
			assert(_primary_buckets > 0);
		}
		else {
			// we are receiving a backup bucket.  
			assert(remote_host);
			bucket->target_node = node_find(remote_host);
			assert(bucket->target_node);
			assert(bucket->target_node->client);

			assert(_hashmasks);
			assert(_hashmasks[key_hash]);
			if (_hashmasks[key_hash]->secondary) {
				free(_hashmasks[key_hash]->secondary);
			}
			_hashmasks[key_hash]->secondary = strdup(_interface);
			assert(_hashmasks[key_hash]->primary);
			
			// we should be connected to this other node.
			assert(bucket->backup_node == NULL);
			
			_secondary_buckets ++;
			assert(_secondary_buckets > 0);
		}

		bucket->transfer_client = NULL;
	}
	
	// at this point, the bucket should have at least a source node, or a backup node listed.
	assert(bucket->target_node || bucket->backup_node);

	assert(bucket->transfer_client == NULL);
	
	assert(payload_length() > 0);
	assert(reply > 0);
	client_send_message(client, header, reply, payload_length(), payload_ptr());
	payload_clear();

	assert(_bucket_transfer == 1);
	_bucket_transfer = 0;
	
	if (bucket) {
		// send message to all other connected clients and nodes alerting them to the change in buckets.
		assert(bucket->level >= 0);
		push_hashmask_update(bucket);
	}
}



// the hello command does not require a payload, and simply does a reply.   
// However, it triggers a servermap, and a hashmasks command to follow it.
static void cmd_hello(client_t *client, header_t *header)
{
	assert(client);
	assert(header);

	// there is no payload required for an ack.
	
	// send the ACK reply.
	client_send_message(client, header, REPLY_ACK, 0, NULL);
	
	// send a servermap command to the client.
	push_serverlist(client);
	
	// send a hashmasks command to the client.
	push_hashmasks(client);
}




// Set a value into the hash storage for a new bucket we are receiving.  Almost the same as 
// cmd_set_str, except the data received is slightly different.
static void cmd_sync_string(client_t *client, header_t *header, char *payload)
{
	char *next;
	hash_t map_hash;
	hash_t key_hash;
	int expires;
	value_t *value;
	char *str;
	int result;
	int str_len;
	
	assert(client);
	assert(header);
	assert(payload);

	// create a new value.
	value = calloc(1, sizeof(value_t));
	assert(value);
	
	next = payload;
	
	map_hash = data_long(&next);
	key_hash = data_long(&next);
	expires = data_int(&next);
	
	assert(_hashmasks);

	
	// we cant treat the string as a typical C string, because it is actually a binary blob that may 
	// contain NULL chars.
	str = data_string(&next, &str_len);
	value->data.s.data = malloc(str_len + 1);
	memcpy(value->data.s.data, str, str_len);
	value->data.s.data[str_len] = 0;
	value->data.s.length = str_len;

	value->type = VALUE_STRING;
	
	
	// store the value into the trees.  If a value already exists, it will get released and this one 
	// will replace it, so control of this value is given to the tree structure.
	// NOTE: value is controlled by the tree after this function call.
	// NOTE: name is controlled by the tree after this function call.
	result = buckets_store_value(map_hash, key_hash, NULL, 0, expires, value);
	value = NULL;
	
	// send the ACK reply.
	if (result == 0) {
		assert(payload_length() == 0);
		payload_long(map_hash);
		payload_long(key_hash);
		client_send_message(client, header, REPLY_SYNC_ACK, payload_length(), payload_ptr());
		payload_clear();
	}
	else {
		assert(0);
	}
	
	assert(payload_length() == 0);
}




// Set a value into the hash storage for a new bucket we are receiving.  Almost the same as 
// cmd_set_str, except the data received is slightly different.
static void cmd_sync_int(client_t *client, header_t *header, char *payload)
{
	char *next;
	hash_t map_hash;
	hash_t key_hash;
	int expires;
	value_t *value;
	int result;
	
	assert(client);
	assert(header);
	assert(payload);

	// create a new value.
	value = calloc(1, sizeof(value_t));
	assert(value);
	
	next = payload;
	
	map_hash = data_long(&next);
	key_hash = data_long(&next);
	expires = data_int(&next);
	
	value->data.i = data_int(&next);
	value->type = VALUE_INT;
	
	// store the value into the trees.  If a value already exists, it will get released and this one 
	// will replace it, so control of this value is given to the tree structure.
	// NOTE: value is controlled by the tree after this function call.
	// NOTE: name is controlled by the tree after this function call (if it is supplied).
	result = buckets_store_value(map_hash, key_hash, NULL, 0, expires, value);
	value = NULL;
	
	// send the ACK reply.
	if (result == 0) {
		
		assert(payload_length() == 0);
		payload_int(map_hash);
		payload_int(key_hash);
		client_send_message(client, header, REPLY_SYNC_ACK, payload_length(), payload_ptr());
		payload_clear();
	}
	else {
		assert(0);
	}
	
	assert(payload_length() == 0);
}





// Set a value into the hash storage for a new bucket we are receiving.  Almost the same as 
// cmd_set_str, except the data received is slightly different.
static void cmd_sync_name(client_t *client, header_t *header, char *payload)
{
	char *next;
	hash_t key_hash;
	int result;
	char *name;
	
	assert(client);
	assert(header);
	assert(payload);
	
	next = payload;
	
	key_hash = data_long(&next);
	name = data_string_copy(&next);
	assert(name);
	
	logger(LOG_DEBUG, "Received: CMD_SYNC_NAME: %#llx='%s'", key_hash, name);
	
	// NOTE: name is controlled by the tree after this function call.
	result = buckets_store_name_str(key_hash, name);
	name = NULL;
	
	// send the ACK reply.
	if (result == 0) {
		assert(payload_length() == 0);
		payload_int(key_hash);
		client_send_message(client, header, REPLY_SYNC_NAME_ACK, payload_length(), payload_ptr());
		payload_clear();
	}
	else {
		assert(0);
	}
	
	assert(payload_length() == 0);
}


void cmd_init(void)
{
	// add the commands to the client processing code.
	client_add_cmd(CMD_GET_INT, cmd_get_int);
	client_add_cmd(CMD_GET_STR, cmd_get_str);
	client_add_cmd(CMD_SET_INT, cmd_set_int);
	client_add_cmd(CMD_SET_STR, cmd_set_str);
	client_add_cmd(CMD_SYNC_INT, cmd_sync_int);
	client_add_cmd(CMD_SYNC_NAME, cmd_sync_name);
	client_add_cmd(CMD_SYNC_STRING, cmd_sync_string);
	client_add_cmd(CMD_PING, cmd_ping);
	client_add_cmd(CMD_LOADLEVELS, cmd_loadlevels);
	client_add_cmd(CMD_ACCEPT_BUCKET, cmd_accept_bucket);
	client_add_cmd(CMD_CONTROL_BUCKET, cmd_control_bucket);
	client_add_cmd(CMD_FINALISE_MIGRATION, cmd_finalise_migration);
	client_add_cmd(CMD_HASHMASK, cmd_hashmask);
	client_add_cmd(CMD_HELLO, cmd_hello);
	client_add_cmd(CMD_GOODBYE, cmd_goodbye);
	client_add_cmd(CMD_SERVERHELLO, cmd_serverhello);
	client_add_cmd(CMD_SERVERINFO, cmd_serverinfo);
}


