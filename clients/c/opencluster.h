#ifndef __OPENCLUSTER_H
#define __OPENCLUSTER_H


// This version indicates the version of the library so that developers of
// services can ensure that the correct version is installed.
// This version number should be incremented with every change that would
// effect logic.
#define LIBOPENCLUSTER_VERSION 0x00000100
#define LIBOPENCLUSTER_VERSION_NAME "v0.01.00"



// Since we will be using a number of bit masks to check for data status's and
// so on, we should include some macros to make it easier.
#define BIT_TEST(arg,val) (((arg) & (val)) == (val))
#define BIT_SET(arg,val) ((arg) |= (val))
#define BIT_CLEAR(arg,val) ((arg) &= ~(val))
#define BIT_TOGGLE(arg,val) ((arg) ^= (val))



// global constants and other things go here.
#define OPENCLUSTER_DEFAULT_PORT (13600)

// start out with an 1kb buffer.  Whenever it is full, we will double the
// buffer, so this is just a minimum starting point.
#define OPENCLUSTER_DEFAULT_BUFFSIZE (1024)


#define OC_BLOCKING      0
#define OC_NON_BLOCKING  1




typedef struct {
	
	// initially we will be only doing one request at a time, but eventually 
	// we will be doing async requests as well. so we will need a list of 
	// replies.
	int msg_count;
	void **messages;
	
	int server_count;
	void **servers;

	unsigned int mask;
	void **hashmasks;
	
	void *payload;
	int payload_max;
	int payload_length;
	
	
} cluster_t;


cluster_t * cluster_init(void);
void cluster_free(cluster_t *cluster);

void cluster_addserver(cluster_t *cluster, const char *host);
int cluster_connect(cluster_t *cluster);
void cluster_disconnect(cluster_t *cluster);

void cluster_pending(cluster_t *cluster, int blocking);


int cluster_setint(cluster_t *cluster, const char *name, const int value, const int expires);
int cluster_getint(cluster_t *cluster, const char *name, int *value);

int cluster_setstr(cluster_t *cluster, const char *name, const char *value, const int expires);
int cluster_getstr(cluster_t *cluster, const char *name, char **value, int *length);


#endif
