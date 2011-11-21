/*
	Tool that tests the functionality of the libopencluster library.
*/


#include <assert.h>
#include <opencluster.h>
#include <stdio.h>
#include <sys/time.h>


#define GET_LIMIT 40000


int main(int argc, char **argv)
{
	cluster_t *cluster;
	int data;
	int result;
	int nodes;
	register int i;
	struct timeval start_time = {0,0};
	struct timeval stop_time = {0,0};


	printf("Initialising the cluster library\n");
	cluster = cluster_init();
	
	printf("Adding 127.0.0.1:13600 to the server list.\n");
	cluster_addserver(cluster, "127.0.0.1:13600");
	
	nodes = cluster_connect(cluster);
	printf("Connected servers: %d\n", nodes);
	
	if (nodes > 0) {
	
		// set an item in the cluster with some data.
		printf("Setting data in the cluster\n");
		cluster_setint(cluster, "testdata", 45, 0);
		
		// pull some data out of the cluster.
		data = 0;
		printf("Getting data from the cluster.\n");
		gettimeofday(&start_time, NULL);
		for (i=0; i<GET_LIMIT; i++) {
			result = cluster_getint(cluster, "testdata", &data);
			assert(result == 0);
		}
		gettimeofday(&stop_time, NULL);
		printf ("Result of 'testdata' in cache.  data=%d\n", data);
		if (stop_time.tv_usec < start_time.tv_usec) {
			stop_time.tv_usec += 1000000000;
			stop_time.tv_sec --;
		}
		printf("Timing of %d gets. %d.%09d\n", GET_LIMIT, (stop_time.tv_sec - start_time.tv_sec), (stop_time.tv_usec - start_time.tv_usec));
		
		printf("Disconnecting from the cluster,\n");
		cluster_disconnect(cluster);		
	}
	
	printf("Shutting down\n");
	cluster_free(cluster);
	
	
	return(0);
}

