// payload.h

#ifndef __PAYLOAD_H
#define __PAYLOAD_H


void payload_init(void);
void payload_free(void);


int payload_length(void);
void payload_clear(void);
void * payload_ptr(void);

void payload_int(int value);
void payload_string(const char *str);
void payload_data(int length, void *data);




#endif