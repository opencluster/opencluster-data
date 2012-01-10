/*
 * This program is intended to simulate the situation where cluster nodes are added and the buckets 
 * are migrated properly amongst them.   It will describe the process it goes through in the 
 * decision making, and then printout a snapshot of the nodes and the buckets.   Various random 
 * elements will be included as optional, to make it more realistic, such as the delay between how 
 * long it takes to transfer a bucket, and when new nodes are added, and maybe even a situation 
 * where nodes are removed, or lost.
 * 
 * To simulate threads, we will compile it with libevent and use the timeout events to handle 
 * everything.  THat might actually be easier than threads, and would actually simulate the real 
 * server a bit better.
 */

#include <stdio.h>
#include <stdlib.h>
#include <event.h>
#include <assert.h>


#define MAX_NODES          4
#define MAX_NODE_DELAY    10
#define MAX_MIGRATE_DELAY  2


typedef int hash_t;

typedef struct {

	hash_t hash;
	
	int level;	
	
	int source_node;
	int backup_node;

	int receiving;
} bucket_t;

typedef struct {
	int primary;
	int secondary;
} balance_t;

typedef struct {
	
	char name;
	int entry;
	
	bucket_t *buckets;
	balance_t balance;

	struct event *send_loadlevel_event;
	
	balance_t *node_balances;
	int migrating;
	int last_bucket;
} node_t;

typedef struct {
	int entry;
	node_t *source;
	node_t *target;
	balance_t balance;
	int bucket;

	struct event *send_event;
	struct event *reply_event;
	struct event *transfer_event;
} msg_t;


hash_t _mask = 0x0F;

// event base for timeout events.
struct event_base *_evbase = NULL;

struct event *_next_node_event = NULL;


struct timeval _timeout_comms = {.tv_sec = 0, .tv_usec = 2000};
struct timeval _timeout_loadlevel = {.tv_sec = 4, .tv_usec = 0};
struct timeval _timeout_second_node = {.tv_sec = 1, .tv_usec = 0};

node_t **_nodes = NULL;
int _node_count = 0;

int _msgs_count = 0;
msg_t **_msgs = NULL;

int _cycle_count = 0;


















// this function prints out the state of the nodes and the buckets in them.
static void printout(void) 
{
	int n;
	int b;
	char btype;
	char trans;
	
	
	for (n=0; n<_node_count; n++) { printf("+--------+ "); }
	printf("\n");
	for (n=0; n<_node_count; n++) { printf("| NODE-%c | ", _nodes[n]->name); }
	printf("\n");
	
	for (b=0; b<=_mask; b++) {
		for (n=0; n<_node_count; n++) { 
			if (_nodes[n]->buckets[b].level == 0) { btype = 'P'; }
			else if (_nodes[n]->buckets[b].level == 1) { btype = 'S'; }
			else if (_nodes[n]->buckets[b].receiving == 1) { btype = '-'; }
			else { btype = ' '; }
			
			if (_nodes[n]->last_bucket == b) { trans = '*'; }
			else { trans = ' '; }
			
			printf("| #%1X - %c%c| ", b, btype, trans); 
		}
		printf("\n");
	}
	for (n=0; n<_node_count; n++) { printf("|        | "); }
	printf("\n");
	for (n=0; n<_node_count; n++) { 
		printf("|%2d+%2d=%2d| ", _nodes[n]->balance.primary, _nodes[n]->balance.secondary, _nodes[n]->balance.primary + _nodes[n]->balance.secondary); 
	}
	printf("\n");
	for (n=0; n<_node_count; n++) { printf("+--------+ "); }
	printf("\n\n\n");
}

static int balance_total(balance_t balance) 
{
	return(balance.primary + balance.secondary);
}

static msg_t * new_msg(node_t *source, node_t *target)
{
	msg_t *msg;
	int i;
	
	msg = malloc(sizeof(msg_t));
	msg->source = source;
	msg->target = target;
	msg->balance.primary = 0;
	msg->balance.secondary = 0;
	msg->bucket = -1;

	msg->entry = -1;
	for (i=0; i<_msgs_count && msg->entry < 0; i++) {
		if (_msgs[i] == NULL) {
			msg->entry = i;
			_msgs[i] = msg;
		}
	}
	
	if (msg->entry < 0) {
		_msgs = realloc(_msgs, sizeof(msg_t *) * (_msgs_count + 1));
		msg->entry = _msgs_count;
		_msgs_count ++;
	}
	
	msg->send_event = NULL;
	msg->reply_event = NULL;
	msg->transfer_event = NULL;
	
	return(msg);
}


// when a bucket is migrated, it becomes the primary bucket.
static void migrated_handler(int fd, short int flags, void *arg)
{
	msg_t *msg = arg;
	bucket_t *source_bucket;
	bucket_t *target_bucket;
	node_t *backup_node;
	bucket_t *backup_bucket;
	
	assert(fd == -1 && flags & EV_TIMEOUT && arg);

	assert(msg->source);
	assert(msg->target);
	assert(msg->source != msg->target);
	assert(msg->bucket >= 0);
	assert(msg->bucket <= _mask);
	
	printf(" * Node-%c: Bucket #%X has finished migrating.\n", msg->source->name, msg->bucket);
	
	source_bucket = &msg->source->buckets[msg->bucket];
	target_bucket = &msg->target->buckets[msg->bucket];
	
	// update the source node and target node entries. 
	if (source_bucket->backup_node >= 0) {
		// the bucket has a backup node, we need to remove that copy.
		backup_node = _nodes[source_bucket->backup_node];
		backup_bucket = &backup_node->buckets[msg->bucket];

		backup_node->balance.secondary --;
		backup_bucket->level = -1;
		backup_bucket->source_node = -1;
		assert(backup_bucket->backup_node == -1);
	}
	
	assert(source_bucket->source_node < 0);
	source_bucket->source_node = msg->target->entry;
	source_bucket->backup_node = -1;
	target_bucket->backup_node = msg->source->entry;
	target_bucket->source_node = -1;
	
	// indicate that source and target are no longer migrating.
	msg->source->migrating = 0;
	msg->target->migrating = 0;
	
	// update the balances for target.
	msg->target->balance.primary ++;
	msg->source->balance.primary --;
	msg->source->balance.secondary ++;
	
// 	msg->source->cycle_count = 0;
// 	msg->target->cycle_count = 0;
	
	// update the target bucket, to indicate that it is now a backup
	target_bucket->level = 0;
	source_bucket->level = 1;
	
	// update the target bucket, it indicate it is not receiving now.
	target_bucket->receiving = 0;
	
	// update the node_balances entry for target, since we know it has changed now.
	msg->source->node_balances[msg->target->entry].primary++;
	
	// remove the transfer event
	assert(msg->transfer_event);
	event_free(msg->transfer_event);
	msg->transfer_event = NULL;
	
	// remove the message from the list.
	assert(msg->entry < _msgs_count);
	_msgs[msg->entry] = NULL;

	// set the timer to do it all again in 'X' seconds, if we are not transferring buckets.
	assert(msg->source->send_loadlevel_event);
	evtimer_add(msg->source->send_loadlevel_event, &_timeout_loadlevel);
	
	// free the message
	assert(msg->send_event == NULL);
	assert(msg->reply_event == NULL);
	assert(msg->transfer_event == NULL);
	free(msg);

	// output
	printout();
	
}


int attempt_switch(msg_t *msg) 
{
	int transferred = 0;
	int b;
	
	
	// target node has more secondaries than it has primaries... we need to switch a bucket if we can.
	if (msg->balance.primary >= msg->balance.secondary) {
		printf(" * Node-%c: Target Node-%c has more primary than secondary.  No need to switch.\n", msg->source->name, msg->target->name);
		assert(transferred == 0);
	}
	else {

		
		if (balance_total(msg->balance) != balance_total(msg->source->balance)) {
			
			// need to make sure, that by switching, we dont unbalance the target.
// 			if (msg->balance.primary
			
			// go through the list of buckets, looking for one we can switch.
			for (b=0; b<=_mask && transferred <= 0; b++) {
				if (msg->source->buckets[b].level == 0) {
					// we have a primary bucket.
					if (msg->source->buckets[b].backup_node == msg->target->entry) {
						
						if (msg->source->last_bucket != b) {
						
							if ((msg->source->balance.primary-1) >= (msg->source->balance.secondary)) {
							
								printf(" * Node-%c: Promoting bucket #%X on Node-%c\n", msg->source->name, b, msg->target->name);
								msg->source->buckets[b].backup_node = -1;
								msg->source->buckets[b].source_node = msg->target->entry;
								msg->source->buckets[b].level = 1;
								msg->source->balance.primary --;
								msg->source->balance.secondary ++;
								
								msg->target->buckets[b].backup_node = msg->source->entry;
								msg->target->buckets[b].source_node = -1;
								msg->target->buckets[b].level = 0;
								msg->target->balance.primary ++;
								msg->target->balance.secondary --;
							
								msg->source->last_bucket = -1;
								msg->target->last_bucket = b;
								
			// 					msg->target->cycle_count = 0;
			// 					msg->source->cycle_count = 0;
								
								// this bucket doesnt have a backup node, so we will send this.
								transferred ++;

								// add the loadlevel timer back in.  We do it here, because it wont be done further in the code if we are migrating the bucket
								evtimer_add(msg->source->send_loadlevel_event, &_timeout_comms);

								printout();
							}
						}
					}
				}
			}
			
			if (transferred == 0) {
				printf(" * Node-%c: Cannot promote bucket on Node-%c because there are no shared buckets.\n", msg->source->name, msg->target->name);
			}
		}
	}
	
	return(transferred);
}


static void send_bucket(node_t *source, node_t *target, int bucket) 
{
	msg_t *txmsg;
	
	assert(source && target);
	assert(bucket >= 0 && bucket <= _mask);
	
	printf(" * Node-%c: Sending bucket #%X to Node-%c\n", source->name, bucket, target->name);
	
	assert(source->migrating == 0);
	assert(target->migrating == 0);
	source->migrating = 1;
	target->migrating = 1;
	
	txmsg = new_msg(source, target);
	txmsg->bucket = bucket;
	
	// create the migrate event.
	txmsg->transfer_event = evtimer_new(_evbase, migrated_handler, txmsg);
	struct timeval tm = {.tv_sec = 1, .tv_usec = 0};
	tm.tv_sec = random() % MAX_MIGRATE_DELAY;
	evtimer_add(txmsg->transfer_event, &tm);
	
	// mark the target bucket as receiving.
	assert(target->buckets[bucket].receiving == 0);
	target->buckets[bucket].receiving = 1;
	
	target->last_bucket = bucket;
	
	// display the table as it is.
	printout();
}




// go through the list of backups, and attempt to send the first one we find that does not have a backup copy.
static int attempt_nobackup(msg_t *msg) 
{
	int transferred = 0;
	int b;

	if (balance_total(msg->balance) <= _mask) {
		// go through the list of buckets, looking for one we can transfer.
		for (b=0; b<=_mask && transferred == 0; b++) {
			if (msg->source->buckets[b].level == 0) {
				// we have a primary bucket.
					
				if (msg->source->buckets[b].backup_node < 0) {
					assert(msg->source->last_bucket != b);
					send_bucket(msg->source, msg->target, b);
					transferred ++;
				}
			}
		}
	}

	return(transferred);
}




// go thru the bucket list, and if there is one we can transfer, then do it.
static int attempt_transfer(msg_t *msg) 
{
	int transferred = 0;
	int b;
	int transfer_bucket = -1;
	int backup_total;
	int ideal;
	
	
	if (balance_total(msg->source->balance) <= balance_total(msg->balance)) {
		printf(" * Node-%c: Not transferring, Source has more buckets than Target Node-%c (Source: %d, Target: %d).\n", msg->source->name, msg->target->name, balance_total(msg->source->balance), balance_total(msg->balance));
		assert(transferred == 0);
	}
	else {
		// source has more buckets total than target.
		
		ideal = (((_mask +1)*2) / _node_count);
		if (balance_total(msg->source->balance) <= ideal) {
			printf(" * Node-%c: Not transferring a bucket, because have less than ideal buckets (%d <= %d).\n", msg->source->name, msg->source->balance.primary, ideal);
		}
		else {
		
			if (msg->source->balance.primary-1 <= msg->balance.primary) {
				printf(" * Node-%c: Not transferring, will unbalance source node.\n", msg->source->name);
				assert(transferred == 0);
			}
			else {
		
				// go through the list of buckets, looking for one we can transfer.
				for (b=0; b<=_mask && transferred == 0; b++) {
					if (msg->source->buckets[b].level == 0) {
						// we have a primary bucket.
						
						if (msg->source->last_bucket != b) {
						
							// need to check that we are not removing a backup bucket from a node that is already balanced.
							assert(msg->source->buckets[b].backup_node >= 0);
							backup_total = balance_total(msg->source->node_balances[msg->source->buckets[b].backup_node]);
							
							
							// *** need to do a better check for the backup node going out of sync.
							
							if (backup_total > balance_total(msg->balance)) {
								transfer_bucket = b;
							}
							
							// if we found a bucket to transfer, then we need to send it.
							if (transfer_bucket >= 0) {
								send_bucket(msg->source, msg->target, b);
								transferred ++;
							}
						}
					}
				}
			}
		}
	}
	
	return(transferred);
}



static int balance_compare(balance_t one, balance_t two) {
	if (one.primary == two.primary && one.secondary == two.secondary) {
		return 0;
	}
	
	return -1;
}





/*
 * This event is triggered when a loadlevel reply is received from the source.  Here we will need to 
 * clean up and remove the message entry, and reset the main loadlevel event.
 */
static void reply_loadlevel_handler(int fd, short int flags, void *arg)
{
	msg_t *msg = arg;
	int transferred = 0;
	int b;
	int target_total;
	
	assert(fd == -1 && flags & EV_TIMEOUT && arg);

	assert(msg->source);
	assert(msg->target);
	
	printf(" * Node-%c: Received loadlevel reply from Node-%c (%d+%d=%d)\n", msg->source->name, msg->target->name, msg->balance.primary, msg->balance.secondary, msg->balance.primary + msg->balance.secondary);

	// get copy of balances out of the message and add them to the node_balances
	assert(msg->target->entry >= 0);
	assert(msg->target->entry < _node_count);

	target_total = msg->balance.primary + msg->balance.secondary;

	if (balance_compare(msg->source->node_balances[msg->target->entry], msg->balance) != 0) {
		printf(" * Node-%c: Previous balance for Node-%c was (%d+%d=%d), changing to (%d+%d=%d)\n", 
				msg->source->name, msg->target->name,
				msg->source->node_balances[msg->target->entry].primary,
				msg->source->node_balances[msg->target->entry].secondary,
				msg->source->node_balances[msg->target->entry].primary + msg->source->node_balances[msg->target->entry].secondary,
				msg->balance.primary, msg->balance.secondary, target_total
		);
		msg->source->node_balances[msg->target->entry] = msg->balance;
	}
	
	if (msg->target->migrating != 0 ) {
		printf(" * Node-%c: Node-%c is currently migrating... ignoring.\n", msg->source->name, msg->target->name);
	}
	else if (msg->source->migrating != 0) {
		printf(" * Node-%c: Already migrating unable to process more.\n", msg->source->name);
	}
	else {
	
		// attempt to promote a bucket on the remote node, if it has more backups than primary, and we have a suitable bucket.
		if (attempt_switch(msg) == 0) {
			if (attempt_nobackup(msg) == 0) {
				if (attempt_transfer(msg) == 0) {
					printf(" * Node-%c: Unable to send any buckets to Node-%c\n", msg->source->name, msg->target->name);
				}
			}
		}
	}
	
	// remove the events
	assert(msg->send_event == NULL);
	assert(msg->reply_event);
	event_free(msg->reply_event);
	msg->reply_event = NULL;
	
	// remove the message from the list.
	assert(msg->entry < _msgs_count);
	_msgs[msg->entry] = NULL;
	
	// free the message
	assert(msg->send_event == NULL);
	assert(msg->reply_event == NULL);
	assert(msg->transfer_event == NULL);
	free(msg);

	// set the timer to do it all again in 'X' seconds, if we are not transferring buckets.
	if (transferred == 0) {
// 		if (msg->source->cycle_count < 3) {
			evtimer_add(msg->source->send_loadlevel_event, &_timeout_loadlevel);
// 		}
	}
// 	else {
// 		assert(msg->source->cycle_count > 0);
// 	}
}



/*
 * Simulates receiving a loadlevel request from a node.  Will look at the data and return it.
 */
static void recv_loadlevel_handler(int fd, short int flags, void *arg)
{
	msg_t *msg = arg;
	
	assert(fd == -1 && flags & EV_TIMEOUT && arg);

	assert(msg->source);
	assert(msg->target);
	
	printf(" * Node-%c: Received loadlevel request from Node-%c (%d+%d=%d)\n", msg->target->name, msg->source->name, msg->target->balance.primary, msg->target->balance.secondary, msg->target->balance.primary+msg->target->balance.secondary);

	assert(msg->balance.primary == 0);
	assert(msg->balance.secondary == 0);
	
	msg->balance.primary = msg->target->balance.primary;
	msg->balance.secondary = msg->target->balance.secondary;

	assert(msg->send_event);
	event_free(msg->send_event);
	msg->send_event = NULL;
	
	msg->reply_event = evtimer_new(_evbase, reply_loadlevel_handler, msg);
	assert(msg->reply_event);
	evtimer_add(msg->reply_event, &_timeout_comms);
}







/* 
 * this event simulates the triggering of a node sending loadlevel events to all the other nodes.
 */
static void send_loadlevel_handler(int fd, short int flags, void *arg)
{
	node_t *node = arg;
	int n;
	int i;
	msg_t *msg;
	
	assert(fd == -1 && flags & EV_TIMEOUT && arg);

	for (n=0; n<_node_count; n++) { 
		if (_nodes[n] != node) {
			printf(" * Node-%c: Sending loadlevel request to Node-%c\n", node->name, _nodes[n]->name);

			msg = new_msg(node, _nodes[n]);
			
			msg->send_event = evtimer_new(_evbase, recv_loadlevel_handler, msg);
			assert(msg->send_event);
			evtimer_add(msg->send_event, &_timeout_comms);
			
			msg->reply_event = NULL;
		}
	}
}




static void add_node(void) 
{
	node_t *node;
	int i;
	int n;

	_nodes = realloc(_nodes, sizeof(node_t *)*(_node_count+1));
	node = calloc(1, sizeof(node_t));
	
	node->name = 'A' + _node_count;
	node->entry = _node_count;
	node->buckets = calloc(_mask+1, sizeof(bucket_t));
	node->migrating = 0;
// 	node->cycle_count = 0;
	node->last_bucket = -1;
	
	for (i=0; i<=_mask; i++) {
		node->buckets[i].hash = i;
		node->buckets[i].level = -1;	
		node->buckets[i].source_node = -1;
		node->buckets[i].backup_node = -1;
		node->buckets[i].receiving = 0;
	}

	// we want this node to send out loadlevel requests frequently.
	node->send_loadlevel_event = evtimer_new(_evbase, send_loadlevel_handler, node);
	assert(node->send_loadlevel_event);
	evtimer_add(node->send_loadlevel_event, &_timeout_loadlevel);


	
	// each existing node keeps track of the balances of each other node, so we need to simulate 
	// this... technically this shouldn't happen immediately, but that would be more complex to 
	// simulate, without adding any real benefit that I can think of.	
	for (n=0; n<_node_count; n++) {
		_nodes[n]->node_balances = realloc(_nodes[n]->node_balances, sizeof(balance_t) * (_node_count+1));
		_nodes[n]->node_balances[_node_count].primary = 0;
		_nodes[n]->node_balances[_node_count].secondary = 0;
	}
	
	// need to setup the balances for this node.  They are currently zero, because this node hasn't 
	// received loadlevels yet.
	node->node_balances = malloc(sizeof(balance_t) * (_node_count+1+1));
	for (n=0; n<_node_count+1; n++) {
		node->node_balances[n].primary = 0;
		node->node_balances[n].secondary = 0;
	}

	
	
	node->balance.primary = 0;
	node->balance.secondary = 0;

	_nodes[_node_count] = node;
	_node_count ++;
	
}

/*
 * This handler will add a node to the cluster.  It will setup the appropriate timeout events for 
 * the node, especially to send out loadlevel requests to the other nodes.
 */
static void next_node_handler(int fd, short int flags, void *arg)
{
	assert(fd == -1);
	assert(flags & EV_TIMEOUT);
	assert(arg == NULL);

	add_node();

	if (_node_count < MAX_NODES) {
		
		struct timeval tm = {.tv_sec = 1, .tv_usec = 0};
		tm.tv_sec = random() % MAX_NODE_DELAY;
		evtimer_add(_next_node_event, &tm);
		
		printf(" * Adding node to cluster.  Next node in %d seconds.\n", tm.tv_sec);
	}
	else {
		event_free(_next_node_event);
		_next_node_event = NULL;
		printf(" * Adding node to cluster.\n");
	}
	
	printout();
}


int main(void) 
{
	int i;
	
	
	// display rules of the simulation.
	printf(
			"OpenCluster Bucket Migration Simulation.\n\n"
			"Terms:\n"
			" Source Node: The node processing the information\n"
			" Target Node: The node that is a potential recipient of a bucket.\n"
			" Backup Node: Node that contains the backup copy of a bucket.\n"
			"\n"
			"Migration Rules:\n"
			"  1. Promote a backup bucket if target has more secondary than primary buckets.\n"
			"  2. Node can only transfer or receive one bucket at a time.\n"
			"  3. If target node is not full, send it any bucket that does not have a backup node.\n"
			"  4. Transferring a bucket to a node, automatically makes the new node the primary.\n"
			"  5. Transferring a bucket to a node, automatically makes the old node the backup.\n"
			"  6. Transfer a bucket if target has less total buckets than source\n"
			"  7. Only transfer a bucket if source has at least one more primary bucket than target.\n"
			"  8. Only transfer a bucket if Backup has more total buckets than target.\n"
			"  9. Source node can only transfer a primary bucket.\n"
			" 10. Do not transfer a bucket, if node does not have an adequate number of buckets already\n"
			" 11. Do not transfer the last bucket in a node that was transferred.\n"
			"\n"
		);
	
	
	
	// setup the evbase.
	_evbase = event_base_new();
	assert(_evbase);

	
	// setup the first node.
	assert(_nodes == NULL);
	assert(_node_count == 0);
	add_node();
	assert(_nodes);
	assert(_node_count > 0);
	
	// create the buckets on the first node.
	for (i=0; i<=_mask; i++) {
		_nodes[0]->buckets[i].level = 0;
		_nodes[0]->balance.primary = _mask + 1;
		_nodes[0]->balance.secondary = 0;
	}
	
	// print the current state with one node.
	printout();

	// setup a timeout for when to introduce the next node;
	_next_node_event = evtimer_new(_evbase, next_node_handler, NULL);
	assert(_next_node_event);
	evtimer_add(_next_node_event, &_timeout_second_node);


	// run the event loop (exit uncleanly with Ctrl-C, its ok).
	assert(_evbase);
	event_base_dispatch(_evbase);

	printf(	"\n"
			"------------------------------------------------------------\n"
			"ALL NODES BALANCED.\n"
			"------------------------------------------------------------\n"
			"\n"
		  );
	
	return 0;
}