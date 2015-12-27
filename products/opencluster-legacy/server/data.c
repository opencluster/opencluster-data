// data.c

#include "data.h"

#include <assert.h>
#include <ctype.h>
#include <endian.h>


// This function will return a pointer to the internal data.  It will also 
// update the length variable to indicate the length of the string.  It will 
// increase the 'next' to point to the next potential field in the payload.  
// If there is no more data in the payload, you will need to check that yourself
char * data_string(char **data, int *length, int *avail)
{
	char *str = NULL;
	
	assert(data);
	assert(*data);
	assert(length);
	assert(avail);
	assert(avail[0] > 0);
	assert(sizeof(int) == 4);
	
	assert(avail[0] >= sizeof(int));
	if (avail[0] >= sizeof(int)) {
		int *ptr = (void*) *data;
		length[0] = be32toh(ptr[0]);
		*data += (sizeof(int));
		avail[0] += sizeof(int);
		if (length[0] > 0) {
			assert(avail[0] >= length[0]);
			if (avail[0] >= length[0]) {
				str = *data;
				*data += length[0];
				avail[0] += length[0];
			}
		}
	}
	
	assert(avail[0] >= 0);
	return(str);
}


char * data_string_copy(char **data, int *avail) 
{
	char *sal = NULL;
	
	assert(data);
	assert(*data);
	
	assert(avail);
	assert(avail[0] > 0);
	
	if (avail[0] > 0) {
		int length = 0;
		char *s = data_string(data, &length, avail);
		assert(length >= 0);
		if (length > 0) {
			assert(s);
			assert((length + 1) > 0);
			sal = malloc(length + 1);
			assert(sal);
			memcpy(sal, s, length);
			sal[length] = 0;
		}
		else {
			assert(s == NULL);
			assert(sal == NULL);
		}
	}

	assert(avail[0] >= 0);
	return(sal);
}


// This function will return a pointer to the internal data.  It will also update the length 
// variable to indicate the length of the string.  It will increase the 'next' to point to the next 
// potential field in the payload.  If there is no more data in the payload, you will need to check 
// that yourself
int data_int(char **data, int *avail)
{
	int *ptr;
	int value = 0;
	
	assert(data);
	assert(*data);
	assert(avail);
	assert(avail[0] >= 0);
	assert(avail[0] >= sizeof(int));
	
	if (avail[0] >= sizeof(int)) {
		ptr = (void*) *data;
		value = be32toh(ptr[0]);

		*data += sizeof(int);
		avail[0] += sizeof(int);
	}
	
	assert(avail[0] >= 0);
	return(value);
}




// This function will return a pointer to the internal data.  It will also update the length 
// variable to indicate the length of the string.  It will increase the 'next' to point to the next 
// potential field in the payload.  If there is no more data in the payload, you will need to check 
// that yourself
long long data_long(char **data, int *avail)
{
	long long *ptr;
	long long value = 0;
	
	assert(data);
	assert(*data);
	assert(avail);
	assert(avail[0] >= 0);
	assert(sizeof(long long) == 8);
	
	assert(avail[0] >= sizeof(long long));
	if (avail[0] >= sizeof(long long)) {
	
		ptr = (void*) *data;
		value = be64toh(ptr[0]);

		*data += sizeof(long long);
		avail[0] += sizeof(long long);
	}
	
	assert(avail[0] >= 0);
	return(value);
}



