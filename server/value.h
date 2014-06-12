// value.h

#ifndef __VALUE_H
#define __VALUE_H


#define VALUE_DELETED  0
#define VALUE_LONG     1
#define VALUE_STRING   2
//#define VALUE_LIST     3


typedef struct {
} value_list_entry_t;

typedef struct {
	short type;
	long long valuehash;
	union {
		long long l;			// long
		struct {
			int length;
			char *data;
		} s;					// string
		struct {
			int count;
			value_list_entry_t **array;
		} list;
	} data;
} value_t;


void value_clear(value_t *value);
void value_free(value_t *value);
void value_move(value_t *dest, value_t *src);

#endif