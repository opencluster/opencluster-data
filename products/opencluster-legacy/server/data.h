// data.h

#ifndef __DATA_H
#define __DATA_H

// functions that are used to pull data out of the communication channel.  Since both 'commands' and 
// 'process' needs to use this, we will put them in 'client' because they are both subsets of the client operation.
int data_int(char **data, int *avail);
long long data_long(char **data, int *avail);
char * data_string_copy(char **data, int *avail);
char * data_string(char **data, int *length, int *avail);



#endif