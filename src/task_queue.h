#ifndef _TASK_QUEUE_H_
#define _TASK_QUEUE_H_

#ifdef HAVE_CONFIG_H
#include <config.h>
#endif /* HAVE_CONFIG_H */
#include <millstone.h> /* status_t */
#include <block.h> /* block_id_t */

#include <stddef.h> /* size_t, ssize_t */

#include <pthread.h>

TYPEDEF_STRUCT (task_t,
		(block_id_t, block_id),
		(size_t, size),
		(task_t *, prev),
		(task_t *, next),
		)

TYPEDEF_STRUCT (task_queue_t,
		(task_t, queue),
		(size_t, count),
		(bool, cancel),
		(pthread_mutex_t, mutex),
		(pthread_cond_t, empty),
		)

extern void task_queue_init (task_queue_t * task_queue);
extern status_t task_queue_push (task_queue_t * task_queue, task_t * task);
extern status_t task_queue_pop (task_queue_t * task_queue, task_t * task);
extern void task_queue_cancel (task_queue_t * task_queue);

#endif /* _TASK_QUEUE_H_ */
