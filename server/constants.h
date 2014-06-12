// constants.h

#ifndef __CONSTANTS_H
#define __CONSTANTS_H

#define PACKAGE 						"ocd"
#define VERSION 						"0.10"



#define DEFAULT_BUFSIZE 4096

#ifndef INVALID_HANDLE
#define INVALID_HANDLE -1
#endif

// client protocol header size in bytes.
#define HEADER_SIZE 12

#define STARTING_MASK  0x0F


#define CLIENT_TIMEOUT_LIMIT 6

// number of items to send to another node during sync or migrate.
#define TRANSIT_MIN 0
#define TRANSIT_MAX 1


// When data is received on a socket, and processed, the offset is where processed items are still 
// in the incoming buffer.  This occurs if data arrives fragmented.  If the server receives a lot 
// of rapid messages from a client, the offset can creep up to unmanageable levels.  So we set a 
// maximum offset.  This means that if the offset gets this high, then it needs to move the data 
// up the buffer.   This is not something we want to happen a lot because it impacts performance 
// to be moving data in memory like that.
#define MAX_INCOMING_OFFSET  65536

// minimum number of buckets that a node should have before it splits the buckets.  This means that 
// if some action causes the server to get less than this many buckets (but not if the server never 
// had this many to begin with), then the buckets need to be split.  This would only occur if a 
// bucket needs to be transferred.  It would normally be triggered when a new node is attached to 
// the cluster and the 'ideal' becomes less than this value.
#define MIN_BUCKETS 6



#endif