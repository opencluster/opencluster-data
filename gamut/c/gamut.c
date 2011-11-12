/*
	Tool that tests the functionality of the libopencluster library.
*/


#include <assert.h>
#include <opencluster.h>
#include <stdio.h>


int main(int argc, char **argv)
{
	cluster_t *cluster;
	int data;
	int result;

	printf("Initialising the cluster library\n");
	cluster = cluster_init();
	
	printf("Adding 127.0.0.1:13600 to the server list.\n");
	cluster_addserver(cluster, "127.0.0.1:13600");
	
	if (cluster_connect(cluster)) {
	
		// set an item in the cluster with some data.
		printf("Setting data in the cluster\n");
		cluster_setint(cluster, "testdata", 45);
		
		// pull some data out of the cluster.
		data = 0;
		printf("Getting data from the cluster.\n");
		result = cluster_getint(cluster, "testdata", &data);
		printf ("Result of 'testdata' in cache.  date=%d\n", data);
		
		printf("Disconnecting from the cluster,\n");
		cluster_disconnect(cluster);		
	}
	
	printf("Shutting down\n");
	cluster_free(cluster);
	
	
	return(0);
}

