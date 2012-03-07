// value.h

#ifndef __VALUE_H
#define __VALUE_H


#define VALUE_DELETED  0
#define VALUE_SHORT    1
#define VALUE_INT      2
#define VALUE_LONG     3
#define VALUE_STRING   4
typedef struct {
	short type;
	union {
		int i;					// integer
		long long l;			// long
		struct {
			int length;
			char *data;
		} s;					// string
	} data;
} value_t;


void value_clear(value_t *value);
void value_free(value_t *value);
void value_move(value_t *dest, value_t *src);

#endif