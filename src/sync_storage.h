#ifndef _SYNC_STORAGE_H_
#define _SYNC_STORAGE_H_

#ifdef HAVE_CONFIG_H
#include <config.h>
#endif /* HAVE_CONFIG_H */
#include <millstone.h> /* status_t */

#include <metaresc.h>

#define HASH_TABLE_SIZE (127)

TYPEDEF_STRUCT (sync_rb_tree_t,
		(mr_red_black_tree_node_t *, tree),
		(pthread_mutex_t, mutex),
		)

TYPEDEF_STRUCT (sync_storage_t,
		(sync_rb_tree_t, table, [HASH_TABLE_SIZE]),
		(char *, key_type),
		(mr_compar_fn_t, compar_fn),
		(mr_hash_fn_t, hash_fn),
		(mr_free_fn_t, free_fn),
		(void *, context),
		)

extern status_t sync_storage_add (sync_storage_t * sync_storage, mr_ptr_t mr_ptr);
extern status_t sync_storage_del (sync_storage_t * sync_storage, mr_ptr_t mr_ptr);
extern mr_ptr_t * sync_storage_find (sync_storage_t * sync_storage, mr_ptr_t mr_ptr, mr_compar_fn_t found_fn);
extern void sync_storage_init (sync_storage_t * sync_storage, mr_compar_fn_t compar_fn, mr_hash_fn_t hash_fn, mr_free_fn_t free_fn, char * key_type, void * context);
extern void sync_storage_free (sync_storage_t * sync_storage);
extern status_t sync_storage_yeld (sync_storage_t * sync_storage, mr_visit_fn_t visit_fn);

#endif /* _SYNC_STORAGE_H_ */
