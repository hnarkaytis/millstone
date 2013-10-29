#ifndef _SYNC_STORAGE_H_
#define _SYNC_STORAGE_H_

#ifdef HAVE_CONFIG_H
#include <config.h>
#endif /* HAVE_CONFIG_H */
#include <millstone.h> /* status_t */

#include <metaresc.h>
#include <mr_ic.h>

#define HASH_TABLE_SIZE (17)

TYPEDEF_STRUCT (sync_ic_t,
		(mr_ic_t, mr_ic),
		(pthread_mutex_t, mutex),
		)

TYPEDEF_FUNC (void, synchronized_matched_handler_t, (mr_ptr_t /* found */, mr_ptr_t /* searched */, void * /* context */))

TYPEDEF_STRUCT (sync_storage_t,
		(sync_ic_t, table, [HASH_TABLE_SIZE]),
		(char *, key_type),
		(mr_compar_fn_t, compar_fn),
		(mr_hash_fn_t, hash_fn),
		(mr_free_fn_t, free_fn),
		(void *, context),
		)

extern mr_ptr_t * sync_storage_add (sync_storage_t * sync_storage, mr_ptr_t mr_ptr);
extern status_t sync_storage_del (sync_storage_t * sync_storage, mr_ptr_t mr_ptr, synchronized_matched_handler_t smh);
extern mr_ptr_t * sync_storage_find (sync_storage_t * sync_storage, mr_ptr_t mr_ptr, synchronized_matched_handler_t smh);
extern void sync_storage_init (sync_storage_t * sync_storage, mr_compar_fn_t compar_fn, mr_hash_fn_t hash_fn, mr_free_fn_t free_fn, char * key_type, void * context);
extern void sync_storage_free (sync_storage_t * sync_storage);
extern status_t sync_storage_yeld (sync_storage_t * sync_storage, mr_visit_fn_t visit_fn);

#endif /* _SYNC_STORAGE_H_ */
