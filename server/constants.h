// constants.h

#ifndef __CONSTANTS_H
#define __CONSTANTS_H

#define PACKAGE 						"ocd"
#define VERSION 						"0.10"



#define DEFAULT_BUFSIZE 4096

#ifndef INVALID_HANDLE
#define INVALID_HANDLE -1
#endif


#define HEADER_SIZE 12

#define STARTING_MASK  0x0F


#define CLIENT_TIMEOUT_LIMIT 6

// number of items to send to another node during sync or migrate.
#define TRANSIT_MIN 0
#define TRANSIT_MAX 1


// minimum number of buckets that a node should have before it splits the buckets.  This means that 
// if some action causes the server to get less than this many buckets (but not if the server never 
// had this many to begin with), then the buckets need to be split.  This would only occur if a 
// bucket needs to be transferred.  It would normally be triggered when a new node is attached to 
// the cluster and the 'ideal' becomes less than this value.
#define MIN_BUCKETS 6



#endif