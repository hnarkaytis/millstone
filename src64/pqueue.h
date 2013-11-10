#ifndef _PQUEUE_H_
#define _PQUEUE_H_

#include <block.h> /* block_id_t */

#include <stddef.h> /* size_t, ssize_t */
#include <stdbool.h> /* bool */

#include <pthread.h>

#include <metaresc.h>

TYPEDEF_STRUCT (task_t,
		(size_t, size),
		(block_id_t, block_id),
		)

TYPEDEF_STRUCT (pqueue_t,
		(pthread_mutex_t, mutex),
		(pthread_cond_t, cond),
		(bool, cancel),
		RARRAY (task_t, heap),
		)

extern void pqueue_init (pqueue_t * pqueue);
extern void pqueue_cancel (pqueue_t * pqueue);
extern status_t pqueue_push (pqueue_t * pqueue, task_t * task);
extern status_t pqueue_pop (pqueue_t * pqueue, task_t * task);

#endif /* _PQUEUE_H_ */
