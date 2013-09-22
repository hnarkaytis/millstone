#ifndef _QUEUE_H_
#define _QUEUE_H_

#ifdef HAVE_CONFIG_H
#include <config.h>
#endif /* HAVE_CONFIG_H */
#include <millstone.h> /* status_t */

#include <stddef.h> /* size_t */
#include <pthread.h>

#include <metaresc.h>

TYPEDEF_STRUCT (queue_t,
		(void *, array),
		(char *, elem_type),
		RARRAY (mr_ptr_t, typed_array, "elem_type"),
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

extern status_t queue_init (queue_t * queue, size_t elem_size, int count, char * elem_type);
extern status_t queue_push (queue_t * queue, void * element);
extern status_t queue_pop (queue_t * queue, void * element);
extern status_t queue_pop_bulk (queue_t * queue, void * buf, size_t * buf_size);
extern void queue_cancel (queue_t * queue);

#endif /* _QUEUE_H_ */
