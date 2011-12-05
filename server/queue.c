/*
 * single-linked list used as a queue.  
 * Push things at the head, and pop them from the tail.
 * Very limited queue, intended to be used only as a queue of items.  
 * 
 * (c) Copyright Clinton Webb, 2011
 * 
 * Released under GPL 3 and later.
 * 
 */

#include "queue.h"

#include <assert.h>
#include <stdlib.h>

/*
 * Create a new queue object and initialise it.  Should be freed by queue_free().
 */
queue_t * queue_new(void)
{
	queue_t *q;
	
	q = malloc(sizeof(queue_t));
	assert(q);
	
	q->head = NULL;
	q->tail = NULL;
	q->avail = NULL;
	q->count = 0;
	
	return(q);
}

/*
 * Free the queue object from memory.  The queue should be empty.
 */
void queue_free(queue_t *q)
{
	queue_item_t *item;
	assert(q);
	
	assert(q->head == NULL);
	assert(q->tail == NULL);
	assert(q->count == 0);
	
	while(q->avail) {
		item = q->avail;
		q->avail = item->next;
		
		free(item);
	}
	
	free(q);
}


void queue_push(queue_t *q, void *ptr)
{
	queue_item_t *item;
	
	assert(q);
	assert(ptr);
	
	// first get a free item.
	if (q->avail) {
		item = q->avail;
		q->avail = item->next;
		item->next = NULL;
		assert(item->ptr == NULL);
	}
	else {
		item = calloc(1, sizeof(queue_item_t));
		assert(item->ptr == NULL);
		assert(item->next == NULL);
	}
	assert(item);

	// add the pointer payload to the item.
	item->ptr = ptr;
	
	
	// now add this item to the end of the list.
	if (q->head == NULL) {
		assert(q->tail == NULL);
		assert(q->count == 0);
		
		q->head = item;
		assert(item->next == NULL);
		q->tail = item;
	}
	else {
		assert(q->tail);
		assert(q->tail->next == NULL);
		q->tail->next = item;
		assert(item->next == NULL);
	}
	
	// increment the count of items in the queue.
	q->count ++;
	assert(q->count > 0);
}

/*
 * Remove an item from the queue.
 */
void * queue_pop(queue_t *q)
{
	queue_item_t *item;
	void *ptr;
	
	assert(q);
	
	if (q->head == NULL) {
		assert(q->tail == NULL);
		assert(q->count == 0);
		return(NULL);
	}
	else {
		assert(q->tail);
		assert(q->count > 0);
		
		item = q->head;
		q->head = item->next;
		
		// if the head is now empty, tail needs to be too.
		if (q->head == NULL) q->tail = NULL;
		
		q->count --;
		assert(q->count >= 0);
		
		ptr = item->ptr;
		assert(ptr);
		
		item->ptr = NULL;
		item->next = q->avail;
		q->avail = item;
		
		return(ptr);
	}
}

int queue_count(queue_t *q)
{
	assert(q);
	
	assert(q->count >= 0);
	assert((q->count == 0 && q->head == NULL && q->tail == NULL) || (q->count > 0 && q->head && q->tail));
	return(q->count);
}

