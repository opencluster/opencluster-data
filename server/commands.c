// commands.c

#include "bucket.h"
#include "client.h"
#include "commands.h"
#include "hashfn.h"
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

	/* First we will blindly attempt to get the data from this node.   If this data is not primarily 
	 * stored on this server, it is rather quick to exit.   Since we need to cater for clients that 
	 * connect directly to each node for super fast response, we need to treat that method the 
	 * quickest.  Therefore it is assumed that if the client is asking this node for the data, then 
	 * there is a good change the data is on this server.
	 * 
	 * The function will return NULL even if the data is there but this server is a backup node.  
	 * Since the source of truth is the primary store, it will not grab the data from the backup 
	 * node.
	 */
	value = buckets_get_value(map_hash, key_hash);
	
	if (value) {
		if (value->type != VALUE_LONG) {
			// need to indicate stored value is a different type.
			assert(0);
		}
		else {
			// we have the data, build the reply.
			assert(payload_length() == 0);
			payload_long(map_hash);
			payload_long(key_hash);
			payload_long(value->valuehash);
			payload_long(value->data.l);

			assert(payload_length() > 0);
			client_send_reply(client, header, RESPONSE_DATA_INT, payload_length(), payload_ptr());
			payload_clear();
		}
	}
	else {
		
		
		/* NOTE:
		 * 
		 * For this function, we are assuming that the majority of requests will be correctly 
		 * allocated to the correct server, and would likely result in an item being found.   
		 * That is why we first just blindly attempt to find the item in the maps, rather than first 
		 * checking that this key belongs on this server.   If we are unable to find the item, then 
		 * we check to see if the client should be looking on a different server.
		 * 
		 * You may be tempted to do those checks first, but then you are penalizing the 99% of 
		 * requests that return an item.  So you better make sure that's actually what you want to 
		 * do.
		 */
		
		
		logger(LOG_DEBUG, "CMD: get (integer) FAILED [%#llx/%#llx]", map_hash, key_hash);
		// the data they are looking for is not here.
		// we need to check to make sure taht this instance is responsible for the bucket this item would be located.  If it this instance, then the item doesnt exist, but if this instance is not responsible, we need to reply with the primary server for that bucket.
		
		node_t *node = buckets_get_primary_node(key_hash);
		if (node == NULL) {
			// the server for the bucket is this one, so the key mustn't exit.
			client_send_reply(client, header, RESPONSE_FAIL, 0, NULL);
		}
		else {
			// The data is not here, but we know where the data is, so we need to make a request to the actual server that has it.
	
			assert(node->conninfo);
			const char *server_name = conninfo_name(node->conninfo);
			assert(server_name);
			logger(LOG_DEBUG, "CMD: Bucket %#llx not here, it is at '%s'", key_hash, server_name);
			
			assert(0);

			// create a structure with the data we will need when the response comes back.

			// The data is no
			
			
			
// 			assert(payload_length() == 0);
// 			payload_string(server);

// 			assert(payload_length() > 0);
// 			client_send_message(client, header, REPLY_TRYELSEWHERE, payload_length(), payload_ptr());
// 			payload_clear();

		}
	}
	
	assert(payload_length() == 0);
}


// Get a value from storage.
static void cmd_get_str(client_t *client, header_t *header, char *payload)
{
	char *next;
	hash_t map_hash;
	hash_t key_hash;
	int max_length;
	value_t *value;
	
	assert(client);
	assert(header);
	assert(payload);

	next = payload;
	map_hash = data_long(&next);
	key_hash = data_long(&next);
	max_length = data_int(&next);
	
	if (max_length < 0) {
		// the client gave a negative number which is invalid.
		client_send_reply(client, header, RESPONSE_FAIL, 0, NULL);
	}
	else {

		/* First we will blindly attempt to get the data from this node.   If this data is not primarily 
		* stored on this server, it is rather quick to exit.   Since we need to cater for clients that 
		* connect directly to each node for super fast response, we need to treat that method the 
		* quickest.  Therefore it is assumed that if the client is asking this node for the data, then 
		* there is a good change the data is on this server.
		* 
		* The function will return NULL even if the data is there but this server is a backup node.  
		* Since the source of truth is the primary store, it will not grab the data from the backup 
		* node.
		*/
		value = buckets_get_value(map_hash, key_hash);
		
		if (value) {
			if (value->type != VALUE_STRING) {
				// need to indicate stored value is a different type.
				client_send_reply(client, header, RESPONSE_WRONGTYPE, 0, NULL);
			}
			else {
				
				// the value is a string, but is it within the max length specified?
				if (max_length > 0 && value->data.s.length > max_length) {
					client_send_reply(client, header, RESPONSE_TOOLARGE, 0, NULL);
				}
				else {
					// everything is goog, so build the reply.
					assert(payload_length() == 0);
					payload_long(map_hash);
					payload_long(key_hash);
					payload_long(value->valuehash);
					payload_data(value->data.s.length, value->data.s.data);
					
					assert(payload_length() > 0);
					client_send_reply(client, header, RESPONSE_DATA_STRING, payload_length(), payload_ptr());
					payload_clear();
				}
			}
		}
		else {
			
			/* NOTE:
			* 
			* For this function, we are assuming that the majority of requests will be correctly 
			* allocated to the correct server, and would likely result in an item being found.   
			* That is why we first just blindly attempt to find the item in the maps, rather than first 
			* checking that this key belongs on this server.   If we are unable to find the item, then 
			* we check to see if the client should be looking on a different server.
			* 
			* You may be tempted to do those checks first, but then you are penalizing the 99% of 
			* requests that return an item.  So you better make sure that's actually what you want to 
			* do.
			*/
			
			
			// the data they are looking for is not here.
			node_t *node = buckets_get_primary_node(key_hash);
			if (node == NULL) {
				// the server for the bucket is this one, so the key mustn't exit.
				client_send_reply(client, header, RESPONSE_FAIL, 0, NULL);
			}
			else {
				// need to reply with the actual server that is responsible for this key.

				assert(node->conninfo);
				const char *server_name = conninfo_name(node->conninfo);
				assert(server_name);
				assert(strlen(server_name) > 0);
				logger(LOG_DEBUG, "CMD: Bucket %#llx not here, it is at '%s'", key_hash, server_name);
				
				// need to pass the request on to the appropriate server that has the data.
				assert(0);

			}
		}
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
	int result;
	int str_len;
	
	assert(client);
	assert(header);
	assert(payload);
	
	next = payload;
	map_hash = data_long(&next);
	key_hash = data_long(&next);
	expires = data_int(&next);
	str = data_string(&next, &str_len);
	
	// first we need to check that this server is responsible for this data.  If not, we need to pass a message to the server that is.
	node_t *node = buckets_get_primary_node(key_hash);
	if (node) {
		// this data is being served by another node... we need to relay the query.
		assert(0);
	}
	else {
		
		// create a new value.
		value = calloc(1, sizeof(value_t));
		assert(value);

		value->type = VALUE_STRING;
		value->valuehash = generate_hash_str(str, str_len);
		value->data.s.data = malloc(str_len + 1);
		memcpy(value->data.s.data, str, str_len);
		value->data.s.data[str_len] = 0;
		value->data.s.length = str_len;

		// store the value into the trees.  If a value already exists, it will get released and this one 
		// will replace it, so control of this value is given to the tree structure.
		// NOTE: value is controlled by the tree after this function call.
		result = buckets_store_value(map_hash, key_hash, expires, value);
		value = NULL;
		
		// send the ACK reply.
		if (result == 0) {
			client_send_reply(client, header, RESPONSE_OK, 0, NULL);
			payload_clear();
		}
		else {
			assert(0);
		}
	}
	
	assert(payload_length() == 0);
}



static void cmd_loadlevels(client_t *client, header_t *header)
{
	assert(client);
	assert(header);

	// the header shouldn't have any payload for this command.
	assert(header->length == 0);
	assert(payload_length() == 0);
	
	int primary_count = buckets_get_primary_count();
	int secondary_count = buckets_get_secondary_count();
	int trans = buckets_transferring();

	assert(primary_count >= 0);
	assert(secondary_count >= 0);
	assert(trans >= 0);
	payload_int(primary_count);
	payload_int(secondary_count);
	payload_int(trans);
	
	// send the reply.
	client_send_reply(client, header, RESPONSE_LOADLEVELS, payload_length(), payload_ptr());

	payload_clear();
}




// Get a value from storage.
static void cmd_accept_bucket(client_t *client, header_t *header, char *payload)
{
	char *next;
	hash_t mask;
	hash_t key_hash;
	
	assert(client);
	assert(header);
	assert(payload);

	next = payload;
	mask = data_long(&next);
	key_hash = data_long(&next);
	
	logger(LOG_INFO, "CMD: accept bucket (%#llx/%#llx)", mask, key_hash);

	int accepted = buckets_accept_bucket(client, mask, key_hash);
	if (accepted == 0) {
		// bucket was not accepted.
		client_send_reply(client, header, RESPONSE_FAIL, 0, NULL);
	}
	else {
		client_send_reply(client, header, RESPONSE_OK, 0, NULL);
	}

	// we did not create an outgoing payload, better check that it is still empty.
	assert(payload_length() == 0);
}




// Set a value into the hash storage.
static void cmd_set_int(client_t *client, header_t *header, char *payload)
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
	// ** PERF: get the 'value' objects from a pool to improve performance.
	value = calloc(1, sizeof(value_t));
	assert(value);
	
	next = payload;
	map_hash      = data_long(&next);
	key_hash      = data_long(&next);
	expires       = data_int(&next);
	value->data.l = data_long(&next);
	value->type = VALUE_LONG;
	
	logger(LOG_DEBUG, "CMD: set (integer): [%#llx/%#llx]=%d", map_hash, key_hash, value->data.l);

	// store the value into the trees.  If a value already exists, it will get released and this one 
	// will replace it, so control of this value is given to the tree structure.
	// NOTE: value is controlled by the tree after this function call.
	result = buckets_store_value(map_hash, key_hash, expires, value);
	value = NULL;
	
	// send the ACK reply.
	if (result == 0) {
		client_send_reply(client, header, RESPONSE_OK, 0, NULL);
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
	assert(header->length == 0);
	
	// send the ACK reply.
	client_send_reply(client, header, RESPONSE_OK, 0, NULL);
}


static void cmd_goodbye(client_t *client, header_t *header)
{
	assert(client);
	assert(header);

	logger(LOG_INFO, "CMD: goodbye");
	
	client_closing(client);
	
	// send the ACK reply.
	client_send_reply(client, header, RESPONSE_OK, 0, NULL);
}


/*
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

	
	// we have a server name.  We need to check it against our node list.  If it is there, then we 
	// dont do anything.  If it is not there, then we need to add it.
	node = node_find(name);
	if (node == NULL) {
		// the node was not found in the list.  We need to add it to the list, and add this client 
		// to the node.

		node = node_add(client, name);
		assert(node);

		// need to send to the node, the list of hashmasks.
		push_hashmasks(client);
		
		// this is a new server we didn't know about, so we need to send this info to our connected 
		// clients as well.
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
					
					// this client is in the middle of connecting, and we've received a connection 
					// at the same time.
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
*/

/*
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
*/







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
	hash_t current_mask;
	
	assert(client);
	assert(header);
	assert(payload);
	assert(header->length > 0);

	assert(client->node);

	// get the data out of the payload.
	next = payload;
	mask = data_long(&next);
	hash = data_long(&next);
	level = data_int(&next);

	current_mask = buckets_mask();
	
	// check integrity of the data provided.
	assert(mask != 0);
	assert(hash >= 0 && hash <= mask);
	assert(level == -1 || level >= 0);
	
	// check that the mask is the same as our existing mask... 

	logger(LOG_DEBUG, "debug: cmd_hashmask.  orig mask:%#llx, new mask:%#llx", current_mask, mask);
	
	if (mask < current_mask) {
		// if the mask supplied is LESS than the mask we use, then when we read in the data, we need 
		// to convert it to the new mask as we process it.
		assert(0);
	}
	else if (mask > current_mask) {
		// if the mask supplied is GREATER than the mask we use, we need to first permenantly update 
		// our own mask to match the new one received.  Then process the incoming data.
		buckets_split_mask(current_mask, mask);
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
	assert(node);
	buckets_hashmasks_update(node, hash, level);

	// send the ACK reply.
	client_send_reply(client, header, RESPONSE_OK, 0, NULL);
}


// send message to all other connected clients and nodes alerting them to the change in buckets.
static void hashmask_send_update(bucket_t *bucket)
{
	assert(bucket);
	assert(0);
}




// this node, and the other node have the same bucket (one is primary, one is secondary), and one is 
// telling the other to switch.
static void cmd_control_bucket(client_t *client, header_t *header, char *payload)
{
	bucket_t *bucket = NULL;
	
	assert(client);
	assert(header);
	assert(payload);

	char * next = payload;
	hash_t mask     = data_long(&next);
	hash_t hashmask = data_long(&next);
	int level       = data_int(&next);
	
	logger(LOG_INFO, "CMD: bucket control (%#llx/%#llx), level:%d", mask, hashmask, level);

	hash_t current_mask = buckets_mask();
	assert(current_mask > 0);
	
	
	if (client->node) {
		// the client we received the command from, isnt even a node, we cannot accept it.
	}
	else if (mask != current_mask) {
		// make sure that the masks are the same.
		client_send_reply(client, header, RESPONSE_FAIL, 0, NULL);
	}
	else {
		buckets_control_bucket(client, mask, hashmask, level);
		client_send_reply(client, header, RESPONSE_OK, 0, NULL);
	}

	if (bucket) {
		// send message to all other connected clients and nodes alerting them to the change in buckets.
		assert(bucket->level >= 0);
		hashmask_send_update(bucket);
	}

	// outgoing payload should not be needed, better make sure that it is still empty.
	assert(payload_length() == 0);

}




// a bucket is being migrated to this server, and the other node is telling us that all the data has 
// been transferred, and now it is passing control to this node.  When this operation is complete, 
// then the other node will finalize its part in the migration, and migration will be over.
static void cmd_finalise_migration(client_t *client, header_t *header, char *payload)
{
	char *remote_host = NULL;
	bucket_t *bucket = NULL;
	
	assert(client);
	assert(header);
	assert(payload);

	char *next = payload;
	hash_t mask     = data_long(&next);
	hash_t hashmask = data_long(&next);
	int level       = data_int(&next);
	remote_host     = data_string_copy(&next);  // the connect_info for a particular node.
	
	logger(LOG_INFO, "CMD: finalise migration (%#llx/%#llx), level:%d, remote:'%s'", mask, hashmask, level, remote_host);

	// regardless of the reply, the payload will be the same.
	assert(payload_length() == 0);

	hash_t current_mask = buckets_mask();
	
	// make sure that the masks are the same.
	if (mask != current_mask) {
		assert(bucket == NULL);
		client_send_reply(client, header, RESPONSE_FAIL, 0, NULL);
	}
	else {
		buckets_finalize_migration(client, hashmask, level, remote_host);
		client_send_reply(client, header, RESPONSE_OK, 0, NULL);
	}
	
	// at this point, the bucket should have at least a source node, or a backup node listed.
	assert(bucket->source_node || bucket->backup_node);

	assert(bucket->transfer_client == NULL);

	buckets_clear_transferring(bucket);

	if (remote_host) {
		free(remote_host);
		remote_host = NULL;
	}
	
	if (bucket) {
		// send message to all other connected clients and nodes alerting them to the change in buckets.
		assert(bucket->level >= 0);
		hashmask_send_update(bucket);
	}
}



// the hello command does not require a payload, and simply does a reply.   
// However, it triggers a servermap, and a hashmasks command to follow it.
static void cmd_hello(client_t *client, header_t *header, char *payload)
{
	char *auth;
	int auth_len;
	char *next;
	
	assert(client);
	assert(header);

	// there is no payload required for an ack.
	assert(header->length > 0);
	
	next = payload;
	auth = data_string(&next, &auth_len);
	
// TODO: Need to actually parse the authentication information and compare against teh server's authentication methods to determine if there is a match.
	
	// send the ACK reply.
	client_send_reply(client, header, RESPONSE_OK, 0, NULL);
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
	str = data_string(&next, &str_len);
	
	// we cant treat the string as a typical C string, because it is actually a binary blob that may 
	// contain NULL chars.
	value->data.s.data = malloc(str_len + 1);
	memcpy(value->data.s.data, str, str_len);
	value->data.s.data[str_len] = 0;
	value->data.s.length = str_len;
	value->type = VALUE_STRING;
	
	// store the value into the trees.  If a value already exists, it will get released and this one 
	// will replace it, so control of this value is given to the tree structure.
	// NOTE: value is controlled by the tree after this function call.
	// NOTE: name is controlled by the tree after this function call.
	result = buckets_store_value(map_hash, key_hash, expires, value);
	value = NULL;
	
	// send the ACK reply.
	if (result == 0) {
		client_send_reply(client, header, RESPONSE_OK, 0, NULL);
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
	value->data.l = data_long(&next);
	value->type = VALUE_LONG;
	
	// store the value into the trees.  If a value already exists, it will get released and this one 
	// will replace it, so control of this value is given to the tree structure.
	// NOTE: value is controlled by the tree after this function call.
	// NOTE: name is controlled by the tree after this function call (if it is supplied).
	result = buckets_store_value(map_hash, key_hash, expires, value);
	value = NULL;
	
	// send the ACK reply.
	if (result == 0) {
		client_send_reply(client, header, RESPONSE_OK, 0, NULL);
	}
	else {
		assert(0);
	}
	
	assert(payload_length() == 0);
}





// Set a value into the hash storage for a new bucket we are receiving.  Almost the same as 
// cmd_set_str, except the data received is slightly different.
static void cmd_sync_keyvalue(client_t *client, header_t *header, char *payload)
{
	char *next;
	hash_t key_hash;
	int result;
	char *str;
	char *keyvalue;
	int length;
	
	assert(client);
	assert(header);
	assert(payload);
	
	next = payload;
	
	key_hash = data_long(&next);
	str = data_string(&next, &length);
	assert(str);
	assert(length > 0);

	keyvalue = malloc(length + 1);
	memcpy(keyvalue, str, length);
	keyvalue[length] = 0;
	
	logger(LOG_DEBUG, "Received: CMD_SYNC_KEYVALUE: %#llx", key_hash);
	
	// NOTE: name is controlled by the tree after this function call.
	result = buckets_store_keyvalue_str(key_hash, length, keyvalue);
	
	// send the ACK reply.
	if (result == 0) {
		client_send_reply(client, header, RESPONSE_OK, 0, NULL);
	}
	else {
		assert(0);
	}
	
	assert(payload_length() == 0);
}




void cmd_init(void)
{
	// add the commands to the client processing code.   It doesn't really matter which order they 
	// are set.  Previous versions used an array so it was beneficial to have the most common ones 
	// added first.
	client_add_cmd(COMMAND_GET_INT, cmd_get_int);
 	client_add_cmd(COMMAND_GET_STRING, cmd_get_str);
 	client_add_cmd(COMMAND_SET_INT, cmd_set_int);
 	client_add_cmd(COMMAND_SET_STRING, cmd_set_str);
 	client_add_cmd(COMMAND_SYNC_INT, cmd_sync_int);
 	client_add_cmd(COMMAND_SYNC_KEYVALUE, cmd_sync_keyvalue);
 	client_add_cmd(COMMAND_SYNC_STRING, cmd_sync_string);
 	client_add_cmd(COMMAND_PING, cmd_ping);
 	client_add_cmd(COMMAND_LOADLEVELS, cmd_loadlevels);
 	client_add_cmd(COMMAND_ACCEPT_BUCKET, cmd_accept_bucket);
 	client_add_cmd(COMMAND_CONTROL_BUCKET, cmd_control_bucket);
 	client_add_cmd(COMMAND_FINALISE_MIGRATION, cmd_finalise_migration);
 	client_add_cmd(COMMAND_HASHMASK, cmd_hashmask);
 	client_add_cmd(COMMAND_HELLO, cmd_hello);
 	client_add_cmd(COMMAND_GOODBYE, cmd_goodbye);
//  	client_add_cmd(COMMAND_SERVERHELLO, cmd_serverhello);
}


