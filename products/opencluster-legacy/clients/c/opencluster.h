/*
 * Simplified Opencluster client library
 * 
 * This library is intended to be used by regular clients that need access to the cluster.  It does 
 * not provide event-based activity, or low-latency connections.
 * 
 * It is simplified as much as possible for general use.
 * 
 * If you need a client library that connects to all servers and sends requests directly to the 
 * server that has the data, then please use the libopencluster-ll library.
 * 
 * If you need a client library that is libevent based that calls callback routines when data 
 * arrives, please check out the libopencluster-event library.
 * 
 * 
 */




#ifndef __OPENCLUSTER_H
#define __OPENCLUSTER_H


#include <conninfo.h>
#include <stdint.h>

// This version indicates the version of the library so that developers of
// services can ensure that the correct version is installed.
// This version number should be incremented with every change that would
// effect logic.
#define LIBOPENCLUSTER_VERSION 0x00000100
#define LIBOPENCLUSTER_VERSION_NAME "v0.01.00"



// // Since we will be using a number of bit masks to check for data status's and
// // so on, we should include some macros to make it easier.
// #define BIT_TEST(arg,val) (((arg) & (val)) == (val))
// #define BIT_SET(arg,val) ((arg) |= (val))
// #define BIT_CLEAR(arg,val) ((arg) &= ~(val))
// #define BIT_TOGGLE(arg,val) ((arg) ^= (val))


// global constants and other things go here.
#define OPENCLUSTER_DEFAULT_PORT (13600)



// #define OC_BLOCKING      0
// #define OC_NON_BLOCKING  1


typedef uint64_t hash_t;
typedef void * OPENCLUSTER;




OPENCLUSTER cluster_init(void);
void cluster_free(OPENCLUSTER cluster);

// 'conninfo' is controlled by the cluster after this function.  If it is a duplicate of an existing entry, it will be discarded.
void cluster_addserver(OPENCLUSTER cluster, conninfo_t *conninfo);


int cluster_connect(OPENCLUSTER cluster);
void cluster_disconnect(OPENCLUSTER cluster);
int cluster_servercount(OPENCLUSTER cluster);

void cluster_pending(OPENCLUSTER cluster);

void cluster_debug_on(OPENCLUSTER cluster);
void cluster_debug_off(OPENCLUSTER cluster);

void cluster_setkeyvalue(OPENCLUSTER cluster, hash_t key_hash, const char *name);

int cluster_setint(OPENCLUSTER cluster, hash_t map_hash, hash_t key_hash, const int value, const int expires);
int cluster_getint(OPENCLUSTER cluster, hash_t map_hash, hash_t key_hash);

int cluster_setstr(OPENCLUSTER cluster, hash_t map_hash, hash_t key_hash, const char *value, const int expires);
int cluster_setbin(OPENCLUSTER cluster, hash_t map_hash, hash_t key_hash, const char *value, const int length, const int expires);
char * cluster_getstr(OPENCLUSTER cluster, hash_t map_hash, hash_t key_hash);

hash_t cluster_hash_str(const char *str);
hash_t cluster_hash_bin(const char *str, const int length);
hash_t cluster_hash_int(const int key);
hash_t cluster_hash_long(const long long key);

hash_t cluster_setlabel(OPENCLUSTER cluster_ptr, char *label, int expires);
const char * cluster_getlabel(OPENCLUSTER cluster_ptr, hash_t hash);


#endif
