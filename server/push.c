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
	
	assert(payload_length() == 0);
	
	client_send_message(client, COMMAND_PING, 0, NULL, NULL);
	assert(payload_length() == 0);
}




void push_shuttingdown(client_t *client)
{
	assert(client);
	assert(client->handle > 0);
	client_send_message(client, COMMAND_SHUTTINGDOWN, 0, NULL, NULL);
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

	assert(payload_length() == 0);
	payload_long(mask);			// mask
	payload_long(hashmask);		// hash
	payload_int(level);			// instance
	
	// we have a client, that seems to be connected.
	logger(LOG_DEBUG, "sending HASHMASK: (%#llx/%#llx)", mask, hashmask);
	client_send_message(client, COMMAND_HASHMASK, payload_length(), payload_ptr(), NULL);
	
	payload_clear();
}


// Pushes out a command to the specified client (which should be a cluster node), informing it that 
// we are a cluster node also, that our interface is such and such.
void push_serverhello(client_t *client, const char *conninfo_str)
{
	assert(client);
	assert(client->handle > 0);
	assert(conninfo_str);
	
	assert(payload_length() == 0);
	payload_string(conninfo_str);
	
	logger(LOG_DEBUG, "SERVERHELLO sent to '%s'", node_name(client->node));
	
	client_send_message(client, COMMAND_SERVERHELLO, payload_length(), payload_ptr(), NULL);
	payload_clear();
}


// Pushes out a command to the specified client (which should be a cluster node), asking for its 
// currently load level. 
void push_loadlevels(client_t *client)
{
	assert(client);
	assert(client->handle > 0);
	
	assert(payload_length() == 0);
	
	logger(LOG_DEBUG, "sending LOADLEVELS");
	
	client_send_message(client, COMMAND_LOADLEVELS, 0, NULL, NULL);
	assert(payload_length() == 0);
}

void push_accept_bucket(client_t *client, hash_t mask, hash_t hashmask)
{
	assert(client);
	assert(client->handle > 0);
	assert(mask > 0);
	assert(hashmask >= 0 && hashmask <= mask);
	
	assert(payload_length() == 0);
	payload_long(mask);
	payload_long(hashmask);
	
	logger(LOG_DEBUG, "sending ACCEPT_BUCKET: (%#llx/%#llx)", mask, hashmask);
	
	client_send_message(client, COMMAND_ACCEPT_BUCKET, payload_length(), payload_ptr(), NULL);
	payload_clear();
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
	
	assert(payload_length() == 0);
	payload_long(mask);
	payload_long(hashmask);
	payload_int(level);
	
	logger(LOG_DEBUG, "CONTROL_BUCKET(bucket:%#llx)", hashmask);
	
	assert(payload_length() > 0);
	client_send_message(client, COMMAND_CONTROL_BUCKET, payload_length(), payload_ptr(), NULL);
	payload_clear();
}


// push a command to the client telling it that it is now a controller for this particular bucket.
void push_finalise_migration(client_t *client, hash_t mask, hash_t hashmask, const char *conninfo, int level)
{
	assert(client);
	assert(level == 0 || level == 1);
	assert(mask != 0);
	assert(hashmask >= 0 && hashmask <= mask);
	assert(conninfo);
	
	assert(payload_length() == 0);
	payload_long(mask);
	payload_long(hashmask);
	payload_int(level);
	payload_string(conninfo);
	
	logger(LOG_DEBUG, "FINALISE_MIGRATION(bucket:%#llx)", hashmask);
	
	assert(payload_length() > 0);
	client_send_message(client, COMMAND_FINALISE_MIGRATION, payload_length(), payload_ptr(), NULL);
	payload_clear();
}




void push_sync_item(client_t *client, item_t *item)
{
	int cmd = 0;
	
	assert(client);
	assert(client->handle > 0);
	
	assert(item);
	assert(item->value);
	
	assert(payload_length() == 0);
	payload_long(item->map_key);
	payload_long(item->item_key);

	// The expires we store is the time in seconds that it will expire.  But the remote host might 
	// have a different time, so we need to send the amount of seconds that are remaining on the expiry.
	if (item->expires == 0) {
		payload_int(0);
	} else {
		payload_int(item->expires - seconds_get());
	}
	
	if (item->value->type == VALUE_LONG) {
		cmd = COMMAND_SYNC_INT;
		payload_long(item->value->data.l);
		logger(LOG_DEBUG, "sending SYNC_INT: (%#llx:%#llx, %ld)", item->map_key, item->item_key, item->value->data.l);
	}
	else if (item->value->type == VALUE_STRING) {
		cmd = COMMAND_SYNC_STRING;
		payload_data(item->value->data.s.length, item->value->data.s.data);
		logger(LOG_DEBUG, "sending SYNC_STRING: (%#llx:%#llx)", item->map_key, item->item_key);
	}
	else {
		assert(0);
	}
	
	assert(cmd > 0);
	client_send_message(client, cmd, payload_length(), payload_ptr(), NULL);
	payload_clear();
}


void push_sync_keyvalue(client_t *client, hash_t keyhash, int length, char *keyvalue)
{
	assert(client);
	assert(client->handle > 0);
	assert(length > 0);
	assert(keyvalue);
	
	assert(payload_length() == 0);
	payload_long(keyhash);
	payload_data(length, keyvalue);
	logger(LOG_DEBUG, "sending SYNC_KEYVALUE: (%#llx)", keyhash);
	client_send_message(client, COMMAND_SYNC_KEYVALUE, payload_length(), payload_ptr(), NULL);
	payload_clear();
}


void push_sync_keyvalue_int(client_t *client, hash_t keyhash, long long int_key)
{
	assert(client);
	assert(client->handle > 0);
	
	assert(payload_length() == 0);
	payload_long(keyhash);
	payload_long(int_key);
	logger(LOG_DEBUG, "sending SYNC_NAME_INT: (%#llx:%ld)", keyhash, int_key);
	client_send_message(client, COMMAND_SYNC_KEYVALUE_INT, payload_length(), payload_ptr(), NULL);
	payload_clear();
}







