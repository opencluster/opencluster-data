// value.c

#include "value.h"

#include <assert.h>
#include <stdlib.h>



// assumes that the value object has valid data already in it.
void value_clear(value_t *value)
{
	assert(value);

	if (value->type == VALUE_STRING) {
		assert(value->data.s.data);
		free(value->data.s.data);
		value->data.s.data = NULL;
	}
	
	value->type = VALUE_DELETED;
}


void value_free(value_t *value)
{
	assert(value);
	value_clear(value);
	free(value);
}



// move the data from the src to the dest.  Src will be empty after this operation.  
void value_move(value_t *dest, value_t *src)
{
	assert(dest);
	assert(src);
	
	value_clear(dest);
	
	dest->type = src->type;

	switch(src->type) {
		case VALUE_INT:
		case VALUE_SHORT:
			dest->data.i = src->data.i;
			break;
			
		case VALUE_LONG:
			dest->data.l = src->data.l;
			break;
			
		case VALUE_STRING:
			dest->data.s.data = src->data.s.data;
			dest->data.s.length = src->data.s.length;
			src->data.s.data = NULL;
			src->data.s.length = 0;
			src->type = VALUE_DELETED;
			break;
			
		default:
			assert(0);
			break;
	}
}




