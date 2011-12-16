// single-linked list used as a queue.  Push things at the head, and pop them from the tail.
#ifndef __QUEUE_H
#define __QUEUE_H

typedef struct __queue_item_t {
	void *ptr;
	struct __queue_item_t *next;
} queue_item_t;

typedef struct {
	queue_item_t *head;
	queue_item_t *tail;
	queue_item_t *avail;
	int count;
} queue_t;


queue_t * queue_new(void);
void queue_free(queue_t *q);

void queue_push(queue_t *q, void *ptr);
void * queue_pop(queue_t *q);

int queue_count(queue_t *q);



#endif