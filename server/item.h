#ifndef __ITEM_H
#define __ITEM_H

#include "hash.h"
#include "value.h"

typedef struct {
	hash_t item_key;
	hash_t map_key;
	int expires;
	value_t *value;
	int migrate;
	void *maplist;
} item_t;





void item_destroy(item_t *item);



#endif
