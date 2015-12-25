// push.c

#include "client.h"
#include "logging.h"
#include "node.h"
#include "payload.h"
#include "protocol.h"
#include "push.h"
#include "seconds.h"
#include "server.h"

#include <assert.h>
#include <string.h>


// Pushes out a ping command to the specified client (which should be a cluster node).   
// We do not send PINGS to regular clients.
void push_ping(client_t *client)
{
	assert(client);
	assert(client->handle > 0);
	assert(client->node);
	
	PAYLOAD payload = payload_new(client, COMMAND_HASHMASK);
	// no payload data needed.
	
	client_send_message(payload);
}




void push_shuttingdown(client_t *client)
{
	assert(client);
	assert(client->handle > 0);
	PAYLOAD payload = payload_new(client, COMMAND_SHUTTINGDOWN);

	// check payload meets protocol specifications.
	assert(0);

	client_send_message(payload);
}



// send hashmask information for this bucket to all the connected clients.
// This method might be a problem for clients that connect to only one server, as it wont get the 
// hashmask updates, but then, it doesn't really need it.
void push_hashmask(client_t *client, hash_t mask, hash_t hashmask, int level)
{
	assert(client);
		
	assert(mask > 0);
	assert(hashmask >= 0 && hashmask <= mask);
	assert(level == -1 || level == 0 || level == 1);

	PAYLOAD payload = payload_new(client, COMMAND_HASHMASK);
	payload_long(payload, mask);			// mask
	payload_long(payload, hashmask);		// hash
	payload_int(payload, level);			// instance
	
	// we have a client, that seems to be connected.
	logger(LOG_DEBUG, "sending HASHMASK: (%#llx/%#llx)", mask, hashmask);
	client_send_message(payload);
}


// Pushes out a command to the specified client (which should be a cluster node), informing it that 
// we are a cluster node also, that our interface is such and such.
void push_serverhello(client_t *client, const char *conninfo_str, const char *server_auth)
{
	assert(client);
	assert(client->handle > 0);
	assert(conninfo_str);
	assert(server_auth);
	
	PAYLOAD payload = payload_new(client, COMMAND_SERVERHELLO);
	payload_string(payload, conninfo_str);
	payload_string(payload, server_auth);
	
	logger(LOG_DEBUG, "SERVERHELLO sent to '%s'", node_name(client->node));
	client_send_message(payload);
}


// Pushes out a command to the specified client (which should be a cluster node), asking for its 
// currently load level. 
void push_loadlevels(client_t *client)
{
	assert(client);
	assert(client->handle > 0);

	PAYLOAD payload = payload_new(client, COMMAND_LOADLEVELS);
	// no params needed for this operation.
	
	logger(LOG_DEBUG, "sending LOADLEVELS");
	client_send_message(payload);
}

void push_accept_bucket(client_t *client, hash_t mask, hash_t hashmask)
{
	assert(client);
	assert(client->handle > 0);
	assert(mask > 0);
	assert(hashmask >= 0 && hashmask <= mask);
	
	PAYLOAD payload = payload_new(client, COMMAND_ACCEPT_BUCKET);
	payload_long(payload, mask);
	payload_long(payload, hashmask);
	
	logger(LOG_DEBUG, "sending ACCEPT_BUCKET: (%#llx/%#llx)", mask, hashmask);
	client_send_message(payload);
}



void push_promote(client_t *client, hash_t hash)
{
	assert(client);
	
	// send a message to the client, telling it that it is now the primary node for the specified bucket hash.
	assert(0);
	
}



// push a command to the client telling it that it is now a controller for this particular bucket.
void push_control_bucket(client_t *client, hash_t mask, hash_t hashmask, int level)
{
	assert(client);
	assert(level == 0 || level == 1);
	assert(mask != 0);
	assert(hashmask >= 0 && hashmask <= mask);
	
	PAYLOAD payload = payload_new(client, COMMAND_CONTROL_BUCKET);
	payload_long(payload, mask);
	payload_long(payload, hashmask);
	payload_int(payload, level);
	
	logger(LOG_DEBUG, "CONTROL_BUCKET(bucket:%#llx)", hashmask);
	client_send_message(payload);
}


// push a command to the client telling it that it is now a controller for this particular bucket.
void push_finalise_migration(client_t *client, hash_t mask, hash_t hashmask, const char *conninfo, int level)
{
	assert(client);
	assert(level == 0 || level == 1);
	assert(mask != 0);
	assert(hashmask >= 0 && hashmask <= mask);
	assert(conninfo);
	
	PAYLOAD payload = payload_new(client, COMMAND_FINALISE_MIGRATION);
	payload_long(payload, mask);
	payload_long(payload, hashmask);
	payload_int(payload, level);
	payload_string(payload, conninfo);
	
	logger(LOG_DEBUG, "FINALISE_MIGRATION(bucket:%#llx)", hashmask);
	
	client_send_message(payload);
}




void push_sync_item(client_t *client, item_t *item)
{
	assert(client);
	assert(client->handle > 0);
	
	assert(item);
	assert(item->value);

	int expires = 0;
	if (item->expires > 0) {
		expires = item->expires - seconds_get();
	}
	
	if (item->value->type == VALUE_LONG) {
		PAYLOAD payload = payload_new(client, COMMAND_SYNC_INT);
		payload_long(payload, item->map_key);
		payload_long(payload, item->item_key);
		payload_int(payload, expires);
		payload_long(payload, item->value->data.l);
		logger(LOG_DEBUG, "sending SYNC_INT: (%#llx:%#llx, %ld)", item->map_key, item->item_key, item->value->data.l);
		client_send_message(payload);
	}
	else if (item->value->type == VALUE_STRING) {
		PAYLOAD payload = payload_new(client, COMMAND_SYNC_INT);
		payload_long(payload, item->map_key);
		payload_long(payload, item->item_key);
		payload_int(payload, expires);
		payload_data(payload, item->value->data.s.length, item->value->data.s.data);
		logger(LOG_DEBUG, "sending SYNC_STRING: (%#llx:%#llx)", item->map_key, item->item_key);
		client_send_message(payload);
	}
	else {
		assert(0);
	}
}


void push_sync_keyvalue(client_t *client, hash_t keyhash, int length, char *keyvalue)
{
	assert(client);
	assert(client->handle > 0);
	assert(length > 0);
	assert(keyvalue);
	
	PAYLOAD payload = payload_new(client, COMMAND_SYNC_KEYVALUE);
	payload_long(payload, keyhash);
	payload_int(payload, 0);
	payload_data(payload, length, keyvalue);
	logger(LOG_DEBUG, "sending SYNC_KEYVALUE: (%#llx)", keyhash);
	client_send_message(payload);
}






