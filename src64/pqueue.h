#ifndef _PQUEUE_H_
#define _PQUEUE_H_

#include <block.h> /* block_id_t */

#include <stddef.h> /* size_t, ssize_t */
#include <stdbool.h> /* bool */

#include <pthread.h>

#include <metaresc.h>

TYPEDEF_STRUCT (pqueue_t,
		(pthread_mutex_t, mutex),
		(pthread_cond_t, cond),
		(bool, cancel),
		(char *, elem_type),
		(size_t, elem_size),
		(mr_compar_fn_t, compar_fn),
		(void *, context),
		RARRAY (mr_ptr_t, heap, "elem_type"),
		)

extern void pqueue_init (pqueue_t * pqueue, size_t elem_size, char * elem_type, mr_compar_fn_t compar_fn, void * context);
extern void pqueue_cancel (pqueue_t * pqueue);
extern status_t pqueue_push (pqueue_t * pqueue, void * elem);
extern status_t pqueue_pop (pqueue_t * pqueue, void * elem);

#endif /* _PQUEUE_H_ */
