// header.h

#ifndef __HEADER_H
#define __HEADER_H

#include <endian.h>


typedef struct {
	uint16_t command;
	uint16_t response_code;
	uint32_t userid;
	uint32_t length;
} header_t;

// this structure is not packed on word boundaries, so it should represent the 
// data received over the network.
#pragma pack(push,1)
typedef struct {
	uint16_t command;
	uint16_t response_code;
	uint32_t userid;
	uint32_t length;
} raw_header_t;
#pragma pack(pop)



#endif
