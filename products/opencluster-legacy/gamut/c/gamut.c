/*
	Tool that tests the functionality of the libopencluster library.
*/


#include <assert.h>
#include <opencluster.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <glib.h>
#include <unistd.h>


#define GET_LIMIT 400000


int main(int argc, char **argv)
{
	OPENCLUSTER cluster;
	GTimer *timer;
	int nodes;
	register int i;
	gdouble sec;
	gulong msec;
	char key_buffer[128];
	conninfo_t *conninfo;
	int result;

	hash_t map_hash;
	hash_t key_hash;

	timer = g_timer_new();
	
	printf("Initialising the cluster library\n");
	cluster = cluster_init();
	
	conninfo = conninfo_load("gamut.conninfo");
	if (conninfo == NULL) {
		printf("Unable to open gamut.conninfo file\n");
		exit(1);
	}
	
	assert(conninfo);
	printf("Adding server to the server list.\n");
	cluster_addserver(cluster, conninfo);
	
	nodes = cluster_connect(cluster);
	printf("Connected servers: %d\n", nodes);
	
	if (nodes > 0) {
	
		// set the map and key labels to the cluster.  This isn't necessary, but in this case we are demonstrating functionality.
		map_hash = cluster_setlabel(cluster, "testdata", 0);
		key_hash = cluster_setlabel(cluster, "testint", 0);

		printf("Using Map: %s\n", cluster_getlabel(cluster, map_hash));
		printf("Using Key: %s\n", cluster_getlabel(cluster, key_hash));
		
		printf("Setting data in the cluster (integer) [%#llx/%#llx]\n", 
			   (long long unsigned int) map_hash, 
			   (long long unsigned int) key_hash);
		cluster_setint(cluster, map_hash, key_hash, 45, 0);

		printf("Getting int data from the cluster [%#llx/%#llx]\n", 
			   (long long unsigned int) map_hash, 
			   (long long unsigned int) key_hash);
		result = cluster_getint(cluster, map_hash, key_hash);
		printf("result: %s='%d'\n", cluster_getlabel(cluster, key_hash), result);

		
		// set an item in the cluster with some data.
		
		key_hash = cluster_hash_str("teststr");
		printf("Setting data in the cluster (string) [%#llx/%#llx]\n", 
			   (long long unsigned int) map_hash, 
			   (long long unsigned int) key_hash);
		cluster_setstr(cluster, map_hash, key_hash, "Bill Grady", 0);

		printf("Getting str data from the cluster [%#llx/%#llx]\n", 
			   (long long unsigned int) map_hash, 
			   (long long unsigned int) key_hash);
		char *client_name = cluster_getstr(cluster, map_hash, key_hash);
		assert(client_name);
		printf("result='%s'\n", client_name);
		free(client_name);  client_name = NULL;
		
		// pull some data out of the cluster.
		printf("Getting data from the cluster.\n");
		key_hash = cluster_hash_str("testint");
		g_timer_start(timer);
		for (i=0; i<GET_LIMIT; i++) {
			result = cluster_getint(cluster, map_hash, key_hash);
			assert(result == 45);
		}
		g_timer_stop(timer);
		printf ("Result of 'testint' in cache.  result=%d\n", result);
		sec = g_timer_elapsed(timer, &msec);
		printf("Timing of %d gets(int). %f  [Gets per second=%f]\n", GET_LIMIT, sec, GET_LIMIT/sec);
	
// 		printf("Setting 6000 items of data\n");
// 		g_timer_start(timer);
// 		for (i=0; i<6000; i++) {
// 			cluster_setstr(cluster, 
// 						   map_hash, 
// 						   i, 
// 						   "Bill Grady", 0);
// 		}
// 		g_timer_stop(timer);
// 		sec = g_timer_elapsed(timer, &msec);
// 		printf("Timing of 6000 sets. %lf\nSets per second: %0.2lf\n", sec, 6000 / sec);
		
 		sleep(1);
		
		printf("Disconnecting from the cluster,\n");
		cluster_disconnect(cluster);		
	}
	
	printf("Shutting down\n");
	cluster_free(cluster);
	
	
	return(0);
}

