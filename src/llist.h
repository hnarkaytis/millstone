#ifndef _LLIST_H_
#define _LLIST_H_

#ifdef HAVE_CONFIG_H
#include <config.h>
#endif /* HAVE_CONFIG_H */
#include <millstone.h> /* status_t */

#include <stddef.h> /* size_t, ssize_t */
#include <stdbool.h> /* bool */

#include <pthread.h>

TYPEDEF_STRUCT (llist_slot_t,
		(llist_slot_t *, prev),
		(llist_slot_t *, next),
		(mr_ptr_t, ext, , "elem_type"),
		NONE (char, elem, []),
		)

TYPEDEF_STRUCT (llist_t,
		(llist_slot_t, queue),
		(size_t, max_count),
		(size_t, count),
		(size_t, elem_size),
		(char *, elem_type),
		(bool, cancel),
		(pthread_mutex_t, mutex),
		(pthread_cond_t, full),
		(pthread_cond_t, empty),
		)
  
extern void llist_init (llist_t * llist, size_t elem_size, char * elem_type, size_t max_count);
extern status_t llist_push (llist_t * llist, void * elem);
extern status_t llist_pop (llist_t * llist, void * elem);
extern status_t llist_pop_bulk (llist_t * llist, void * buf, size_t * buf_size);
extern void llist_cancel (llist_t * llist);

#define LLIST_INIT(LLIST, ELEM_TYPE, MAX_COUNT) llist_init (LLIST, sizeof (ELEM_TYPE), #ELEM_TYPE, MAX_COUNT)

#endif /* _LLIST_H_ */
