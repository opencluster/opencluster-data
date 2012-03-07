// header.h

#ifndef __HEADER_H
#define __HEADER_H



typedef struct {
	short command;
	short repcmd;
	int userid;
	int length;
} header_t;

// this structure is not packed on word boundaries, so it should represent the 
// data received over the network.
#pragma pack(push,1)
typedef struct {
	short command;
	short repcmd;
	int userid;
	int length;
} raw_header_t;
#pragma pack(pop)



#endif
