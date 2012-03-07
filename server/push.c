// push.c

#include "client.h"
#include "globals.h"
#include "node.h"
#include "payload.h"
#include "protocol.h"
#include "push.h"
#include "server.h"

#include <assert.h>
#include <string.h>


// Pushes out a command to the specified client (which should be a cluster node), informing it that 
// we are a cluster node also, that our interface is such and such.   We do not send PINGS to 
// regular clients.
void push_ping(client_t *client)
{
	assert(client);
	assert(client->handle > 0);
	assert(client->node);
	
	assert(payload_length() == 0);
	
	client_send_message(client, NULL, CMD_PING, 0, NULL);
	assert(payload_length() == 0);
}




void push_shuttingdown(client_t *client)
{
	assert(client->handle > 0);
	client_send_message(client, NULL, CMD_SHUTTINGDOWN, 0, NULL);
}



/* send hashmask information for this bucket to all the connected clients. */
void push_hashmask_update(bucket_t *bucket)
{
	int i;
	
	assert(bucket);
	assert(bucket->hash >= 0 && bucket->hash <= _mask);
	assert(bucket->level == -1 || bucket->level == 0 || bucket->level == 1);

	// create the payload now, because it will be the same for all the clients.
	assert(payload_length() == 0);
	payload_int(_mask);					// mask
	payload_int(bucket->hash);			// hash
	payload_int(bucket->level);			// instance
	
	for (i=0; i<_client_count; i++) {
		if (_clients[i]) {
			if (_clients[i]->handle >= 0) {
				// we have a client, that seems to be connected.

				printf("[%d] sending HASHMASK: (%08X/%08X)\n", _seconds, _mask, bucket->hash);
				client_send_message(_clients[i], NULL, CMD_HASHMASK, payload_length(), payload_ptr());
			}
		}
	}
	
	payload_clear();
}



// send the hashmasks list to the client.
void push_hashmasks(client_t *client)
{
	int i;
	int check;
	
	assert(client);
	assert(client->handle > 0);
	
	assert(_mask >= 0);
	
	// first we set the number of buckets this server contains.
	
	check = 0;
	for (i=0; i<=_mask; i++) {

		if (_buckets[i]->level >= 0) {
			
			// we have the bucket, and we are the 'primary for it'
			assert(_buckets[i]->data);
			assert(_buckets[i]->target_node == NULL);

			assert(payload_length() == 0);
			payload_int(_mask);					// mask
			payload_int(i);						// hash
			payload_int(_buckets[i]->level);	// instance

			client_send_message(client, NULL, CMD_HASHMASK, payload_length(), payload_ptr());
			payload_clear();

			check++;
		}
		else  {
			
			// we dont have the bucket at all, so we dont include it in the list.
			
			assert(_buckets[i]->level < 0);
			assert(_buckets[i]->data);
			assert(_buckets[i]->backup_node == NULL);
		}
	}

	payload_clear();
}


// Pushes out a command to the specified client, with the list of servers that are maintained.
void push_serverlist(client_t *client)
{
	int i;
	
	assert(client);
	assert(client->handle > 0);
	assert(payload_length() == 0);
	assert(_node_count >= 0);
	
	for (i=0; i<_node_count; i++) {
		assert(payload_length() == 0);
		assert(_nodes[i]->name);
		payload_string(_nodes[i]->name);
		client_send_message(client, NULL, CMD_SERVERINFO, payload_length(), payload_ptr());
		payload_clear();
	}
	
	assert(payload_length() == 0);
}




// Pushes out a command to the specified client (which should be a cluster node), informing it that 
// we are a cluster node also, that our interface is such and such.
void push_serverhello(client_t *client)
{
	assert(client);
	assert(client->handle > 0);
	
	assert(payload_length() == 0);
	payload_string(_interface);
	
	printf("[%u] SERVERHELLO: Interface:'%s', length=%d, payload=%d\n", 
		   _seconds, _interface, (int)strlen(_interface), payload_length());
	
	client_send_message(client, NULL, CMD_SERVERHELLO, payload_length(), payload_ptr());
	payload_clear();
}




// Pushes out a command to the specified client (which should be a cluster node), asking for its 
// currently load level. 
void push_loadlevels(client_t *client)
{
	assert(client);
	assert(client->handle > 0);
	
	assert(payload_length() == 0);
	
	printf("[%d] LOADLEVELS: \n", _seconds);
	
	client_send_message(client, NULL, CMD_LOADLEVELS, 0, NULL);
	assert(payload_length() == 0);
}


void push_accept_bucket(client_t *client, hash_t key)
{
	assert(client);
	assert(client->handle > 0);
	
	assert(payload_length() == 0);
	payload_int(_mask);
	payload_int(key);
	
	printf("[%d] sending ACCEPT_BUCKET: (%08X/%08X)\n", _seconds, _mask, key);
	
	client_send_message(client, NULL, CMD_ACCEPT_BUCKET, payload_length(), payload_ptr());
	payload_clear();
}



void push_promote(client_t *client, hash_t hash)
{
	assert(client);
	
	// send a message to the client, telling it that it is now the primary node for the specified bucket hash.
	assert(0);
	
}



// push a command to the client telling it that it is now a controller for this particular bucket.
void push_control_bucket(client_t *client, bucket_t *bucket, int level)
{
	assert(client);
	assert(bucket);
	assert(level == 0 || level == 1);
	assert(bucket->transfer_client == client);
	assert(_mask != 0);
	assert(bucket->hash >= 0 && bucket->hash <= _mask);
	
	assert(payload_length() == 0);
	payload_int(_mask);
	payload_int(bucket->hash);
	payload_int(level);
	
	// if we are sending a primary bucket, then there must be a backup node somewhere. If we are 
	// sending a backup node, then there could be a source node elsewhere (moving a bucket), or we 
	// are the source (bucket had no backup).
	
	
	if (level == 0) {
		assert(_hashmasks[bucket->hash]->primary);
		payload_string(_hashmasks[bucket->hash]->primary);
	}
	else if (level == 1) {
		if (bucket->target_node) {
			assert(_hashmasks[bucket->hash]->secondary);
			payload_string(_hashmasks[bucket->hash]->secondary);
		}
		else {
			payload_string(NULL);
		}
	}
	else payload_string(_interface);

	if (_verbose > 2)
		printf("[%u] CONTROL_BUCKET(bucket:%X): Interface:'%s', length=%d, payload=%d\n", 
		   _seconds, bucket->hash, _interface, (int)strlen(_interface), payload_length());
	
	client_send_message(client, NULL, CMD_CONTROL_BUCKET, payload_length(), payload_ptr());
	payload_clear();
}

