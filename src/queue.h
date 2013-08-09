#ifndef _QUEUE_H_
#define _QUEUE_H_

#include <stddef.h> /* size_t */
#include <pthread.h>

#include <metaresc.h>

#include <millstone.h> /* status_t */

TYPEDEF_STRUCT (queue_t,
		(mr_rarray_t *, array),
		(size_t, elem_size),
		int count,
		int used,
		int head,
		int tail,
		bool cancel,
		(pthread_mutex_t, mutex),
		(pthread_cond_t, full),
		(pthread_cond_t, empty),
		)

extern void queue_init (queue_t * queue, mr_rarray_t * array, size_t elem_size);
extern status_t queue_push (queue_t * queue, void * element);
extern status_t queue_pop (queue_t * queue, void * element);
extern void queue_cancel (queue_t * queue);

#endif /* _QUEUE_H_ */
