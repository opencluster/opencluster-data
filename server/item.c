// item.c

#include "item.h"

#include <assert.h>
#include <stdlib.h>



void item_destroy(item_t *item) 
{
	assert(item);
	
	assert(item->name);
	free(item->name);
	item->name = NULL;

	assert(item->value);
	value_clear(item->value);
	free(item->value);
	item->value = NULL;

	free(item);
}


