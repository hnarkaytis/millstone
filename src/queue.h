#ifndef _QUEUE_H_
#define _QUEUE_H_

#include <pthread.h>
#include <semaphore.h>

#include <millstone.h>

TYPEDEF_STRUCT (queue_t,
		(mr_rarray_t *, array),
		(size_t, elem_size),
		int count,
		int used,
		int head,
		int tail,
		(pthread_mutex_t, mutex),
		(pthread_cond_t, full),
		(pthread_cond_t, empty),
		)

extern status_t queue_init (queue_t * queue, mr_rarray_t * array, size_t elem_size);
extern void queue_push (queue_t * queue, void * element);
extern void queue_pop (queue_t * queue, void * element);

#endif /* _QUEUE_H_ */
