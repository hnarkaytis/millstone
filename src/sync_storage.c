#include <millstone.h>
#include <logging.h>
#include <sync_storage.h>

#include <pthread.h>
#include <metaresc.h>

static sync_ic_t *
get_backet (sync_storage_t * sync_storage, mr_ptr_t mr_ptr)
{
  size_t size = (sizeof (sync_storage->table) / sizeof (sync_storage->table[0]));
  mr_hash_value_t hash_val = sync_storage->hash_fn (mr_ptr, sync_storage->context);
  return (&sync_storage->table[hash_val % size]);
}

mr_ptr_t *
sync_storage_add (sync_storage_t * sync_storage, mr_ptr_t mr_ptr)
{
  sync_ic_t * bucket = get_backet (sync_storage, mr_ptr);
  pthread_mutex_lock (&bucket->mutex);
  mr_ptr_t * add = mr_ic_add (&bucket->mr_ic, mr_ptr, sync_storage->context);
  pthread_mutex_unlock (&bucket->mutex);
  return (add);
}

status_t
sync_storage_del (sync_storage_t * sync_storage, mr_ptr_t mr_ptr, synchronized_matched_handler_t smh)
{
  status_t status = ST_FAILURE;
  sync_ic_t * bucket = get_backet (sync_storage, mr_ptr);
  pthread_mutex_lock (&bucket->mutex);
  mr_ptr_t * found = mr_ic_find (&bucket->mr_ic, mr_ptr, sync_storage->context);
  if (found != NULL)
    {
      mr_ptr_t found_node = *found;
      if (smh != NULL)
	smh (found_node, mr_ptr, sync_storage->context);
      
      if (MR_SUCCESS != mr_ic_del (&bucket->mr_ic, found_node, sync_storage->context))
	ERROR_MSG ("Failed to delete element that really exist.");
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
  sync_ic_t * bucket = get_backet (sync_storage, mr_ptr);
  pthread_mutex_lock (&bucket->mutex);
  mr_ptr_t * found = mr_ic_find (&bucket->mr_ic, mr_ptr, sync_storage->context);
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
      mr_ic_new (&sync_storage->table[i].mr_ic, hash_fn, compar_fn, key_type, MR_IC_HASH_NEXT);
      pthread_mutex_init (&sync_storage->table[i].mutex, NULL);
    }
  sync_storage->compar_fn = compar_fn;
  sync_storage->hash_fn = hash_fn;
  sync_storage->free_fn = free_fn;
  sync_storage->key_type = key_type;
  sync_storage->context = context;
}

static mr_status_t
free_node (mr_ptr_t node, const void * context)
{
  const sync_storage_t * sync_storage = context;
  sync_storage->free_fn (node, sync_storage->context);
  return (MR_SUCCESS);
}

status_t
sync_storage_yeld (sync_storage_t * sync_storage, mr_visit_fn_t visit_fn)
{
  int i;
  for (i = 0; i < sizeof (sync_storage->table) / sizeof (sync_storage->table[0]); ++i)
    {
      pthread_mutex_lock (&sync_storage->table[i].mutex);
      if (visit_fn != NULL)
	if (MR_SUCCESS != mr_ic_foreach (&sync_storage->table[i].mr_ic, visit_fn, sync_storage->context))
	  visit_fn = NULL;
      if (sync_storage->free_fn)
	mr_ic_foreach (&sync_storage->table[i].mr_ic, free_node, sync_storage);
      mr_ic_reset (&sync_storage->table[i].mr_ic);
      pthread_mutex_unlock (&sync_storage->table[i].mutex);
    }
  return ((visit_fn == NULL) ? ST_FAILURE : ST_SUCCESS);
}

void
sync_storage_free (sync_storage_t * sync_storage)
{
  int i;
  sync_storage_yeld (sync_storage, NULL);
  for (i = 0; i < sizeof (sync_storage->table) / sizeof (sync_storage->table[0]); ++i)
    {
      pthread_mutex_lock (&sync_storage->table[i].mutex);
      mr_ic_free (&sync_storage->table[i].mr_ic);
      pthread_mutex_unlock (&sync_storage->table[i].mutex);
    }
}
