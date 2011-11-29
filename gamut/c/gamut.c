/*
	Tool that tests the functionality of the libopencluster library.
*/


#include <assert.h>
#include <opencluster.h>
#include <stdio.h>
#include <sys/time.h>
#include <glib.h>


#define GET_LIMIT 40000


int main(int argc, char **argv)
{
	cluster_t *cluster;
	GTimer *timer;
	int data;
	int result;
	int nodes;
	register int i;
	gdouble sec;
	gulong msec;

	timer = g_timer_new();
	
	printf("Initialising the cluster library\n");
	cluster = cluster_init();
	
	printf("Adding 127.0.0.1:13600 to the server list.\n");
	cluster_addserver(cluster, "127.0.0.1:13600");
	
	nodes = cluster_connect(cluster);
	printf("Connected servers: %d\n", nodes);
	
	if (nodes > 0) {
	
		// set an item in the cluster with some data.
		printf("Setting data in the cluster (integer)\n");
		cluster_setint(cluster, "testdata", 45, 0);

		// set an item in the cluster with some data.
		printf("Setting data in the cluster (string)\n");
		cluster_setstr(cluster, "clientname", "Bill Grady", 0);
		
		
		// pull some data out of the cluster.
		data = 0;
		printf("Getting data from the cluster.\n");
		g_timer_start(timer);
		for (i=0; i<GET_LIMIT; i++) {
			result = cluster_getint(cluster, "testdata", &data);
			assert(result == 0);
		}
		g_timer_stop(timer);
		printf ("Result of 'testdata' in cache.  data=%d\n", data);
		sec = g_timer_elapsed(timer, &msec);
		printf("Timing of %d gets. %f\n", GET_LIMIT, sec);
		
// 		sleep(30);
		
		printf("Disconnecting from the cluster,\n");
		cluster_disconnect(cluster);		
	}
	
	printf("Shutting down\n");
	cluster_free(cluster);
	
	
	return(0);
}

