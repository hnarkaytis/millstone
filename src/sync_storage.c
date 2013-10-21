#include <millstone.h>
#include <logging.h>
#include <sync_storage.h>

#include <setjmp.h>
#include <pthread.h>
#include <metaresc.h>

static sync_rb_tree_t *
get_backet (sync_storage_t * sync_storage, mr_ptr_t mr_ptr)
{
  size_t size = (sizeof (sync_storage->table) / sizeof (sync_storage->table[0]));
  mr_hash_value_t hash_val = sync_storage->hash_fn (mr_ptr, sync_storage->context);
  return (&sync_storage->table[hash_val % size]);
}

mr_ptr_t *
sync_storage_add (sync_storage_t * sync_storage, mr_ptr_t mr_ptr)
{
  sync_rb_tree_t * bucket = get_backet (sync_storage, mr_ptr);
  pthread_mutex_lock (&bucket->mutex);
  mr_ptr_t * find = mr_tsearch (mr_ptr, &bucket->tree, sync_storage->compar_fn, sync_storage->context);
  pthread_mutex_unlock (&bucket->mutex);
  return (find);
}

status_t
sync_storage_del (sync_storage_t * sync_storage, mr_ptr_t mr_ptr, synchronized_matched_handler_t smh)
{
  status_t status = ST_FAILURE;
  sync_rb_tree_t * bucket = get_backet (sync_storage, mr_ptr);
  pthread_mutex_lock (&bucket->mutex);
  mr_ptr_t * found = mr_tfind (mr_ptr, &bucket->tree, sync_storage->compar_fn, sync_storage->context);
  if (found != NULL)
    {
      mr_ptr_t found_node = *found;
      if (smh != NULL)
	smh (found_node, mr_ptr, sync_storage->context);
      
      mr_tdelete (mr_ptr, &bucket->tree, sync_storage->compar_fn, sync_storage->context);
      if (sync_storage->free_fn != NULL)
	sync_storage->free_fn (found_node, sync_storage->context);
      status = ST_SUCCESS;
    }
  pthread_mutex_unlock (&bucket->mutex);
  return (status);
}

mr_ptr_t *
sync_storage_find (sync_storage_t * sync_storage, mr_ptr_t mr_ptr, synchronized_matched_handler_t smh)
{
  sync_rb_tree_t * bucket = get_backet (sync_storage, mr_ptr);
  pthread_mutex_lock (&bucket->mutex);
  mr_ptr_t * found = mr_tfind (mr_ptr, &bucket->tree, sync_storage->compar_fn, sync_storage->context);
  if ((found != NULL) && (smh != NULL))
    smh (*found, mr_ptr, sync_storage->context);
  pthread_mutex_unlock (&bucket->mutex);
  return (found);
}

void
sync_storage_init (sync_storage_t * sync_storage, mr_compar_fn_t compar_fn, mr_hash_fn_t hash_fn, mr_free_fn_t free_fn, char * key_type, void * context)
{
  int i;
  memset (sync_storage, 0, sizeof (sync_storage));
  for (i = 0; i < sizeof (sync_storage->table) / sizeof (sync_storage->table[0]); ++i)
    {
      sync_storage->table[i].tree = NULL;
      pthread_mutex_init (&sync_storage->table[i].mutex, NULL);
    }
  sync_storage->compar_fn = compar_fn;
  sync_storage->hash_fn = hash_fn;
  sync_storage->free_fn = free_fn;
  sync_storage->key_type = key_type;
  sync_storage->context = context;
}

void
dummy_free_fn (mr_ptr_t key, const void * context)
{
}

TYPEDEF_STRUCT (sync_storage_rbtree_foreach_context_t,
		(sync_storage_t *, sync_storage),
		(mr_visit_fn_t, visit_fn),
		(jmp_buf, jmp_buf))

static void
visit_node (const mr_red_black_tree_node_t * node, mr_rb_visit_order_t order, int level, const void * context)
{
  sync_storage_rbtree_foreach_context_t * ssrbtfc = (void*)context;
  if ((MR_RB_VISIT_POSTORDER == order) || (MR_RB_VISIT_LEAF == order))
    if (MR_SUCCESS != ssrbtfc->visit_fn (node->key, ssrbtfc->sync_storage->context))
      longjmp (ssrbtfc->jmp_buf, !0);
}

status_t
sync_storage_yeld (sync_storage_t * sync_storage, mr_visit_fn_t visit_fn)
{
  int i;
  mr_free_fn_t free_fn = sync_storage->free_fn;
  sync_storage_rbtree_foreach_context_t ssrbtfc = {
    .sync_storage = sync_storage,
    .visit_fn = visit_fn,
  };
  
  if (NULL == free_fn)
    free_fn = dummy_free_fn;
  
  for (i = 0; i < sizeof (sync_storage->table) / sizeof (sync_storage->table[0]); ++i)
    {
      pthread_mutex_lock (&sync_storage->table[i].mutex);
      if (visit_fn != NULL)
	{
	  if (0 != setjmp (ssrbtfc.jmp_buf))
	    visit_fn = NULL;
	  else
	    mr_twalk (sync_storage->table[i].tree, visit_node, &ssrbtfc);
	}
      mr_tdestroy (sync_storage->table[i].tree, free_fn, sync_storage->context);
      sync_storage->table[i].tree = NULL;
      pthread_mutex_unlock (&sync_storage->table[i].mutex);
    }
  return ((visit_fn == NULL) ? ST_FAILURE : ST_SUCCESS);
}

void
sync_storage_free (sync_storage_t * sync_storage)
{
  sync_storage_yeld (sync_storage, NULL);
}
