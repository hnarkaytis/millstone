#include <millstone.h>
#include <logging.h>
#include <sync_storage.h>

#include <pthread.h>
#include <metaresc.h>

static sync_rb_tree_t *
get_backet (sync_storage_t * sync_storage, mr_ptr_t mr_ptr)
{
  size_t size = (sizeof (sync_storage->table) / sizeof (sync_storage->table[0]));
  mr_hash_value_t hash_val = sync_storage->hash_fn (mr_ptr, sync_storage->context);
  return (&sync_storage->table[hash_val % size]);
}

status_t
sync_storage_add (sync_storage_t * sync_storage, mr_ptr_t mr_ptr)
{
  status_t status = ST_SUCCESS;
  sync_rb_tree_t * bucket = get_backet (sync_storage, mr_ptr);
  pthread_mutex_lock (&bucket->mutex);
  void * find = mr_tsearch (mr_ptr, &bucket->tree, sync_storage->compar_fn, sync_storage->context);
  pthread_mutex_unlock (&bucket->mutex);
  if (NULL == find)
    {
      FATAL_MSG ("Out of memory.");
      status = ST_FAILURE;
    }
  return (status);
}

void
sync_storage_del (sync_storage_t * sync_storage, mr_ptr_t mr_ptr)
{
  sync_rb_tree_t * bucket = get_backet (sync_storage, mr_ptr);
  pthread_mutex_lock (&bucket->mutex);
  if (sync_storage->free_fn == NULL)
    mr_tdelete (mr_ptr, &bucket->tree, sync_storage->compar_fn, sync_storage->context);
  else
    {
      mr_ptr_t * find = mr_tfind (mr_ptr, &bucket->tree, sync_storage->compar_fn, sync_storage->context);
      if (find != NULL)
	{
	  mr_ptr_t found_node = *find;
	  mr_tdelete (mr_ptr, &bucket->tree, sync_storage->compar_fn, sync_storage->context);
	  sync_storage->free_fn (found_node, sync_storage->context);
	}
    }
  pthread_mutex_unlock (&bucket->mutex);
}

mr_ptr_t *
sync_storage_find (sync_storage_t * sync_storage, mr_ptr_t mr_ptr)
{
  sync_rb_tree_t * bucket = get_backet (sync_storage, mr_ptr);
  pthread_mutex_lock (&bucket->mutex);
  mr_ptr_t * find = mr_tfind (mr_ptr, &bucket->tree, sync_storage->compar_fn, sync_storage->context);
  pthread_mutex_unlock (&bucket->mutex);
  return (find);
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

void
sync_storage_free (sync_storage_t * sync_storage)
{
  int i;
  mr_free_fn_t free_fn = sync_storage->free_fn;
  
  if (NULL == free_fn)
    free_fn = dummy_free_fn;
  
  for (i = 0; i < sizeof (sync_storage->table) / sizeof (sync_storage->table[0]); ++i)
    {
      pthread_mutex_lock (&sync_storage->table[i].mutex);
      mr_tdestroy (sync_storage->table[i].tree, free_fn, sync_storage->context);
      pthread_mutex_unlock (&sync_storage->table[i].mutex);
    }
  memset (sync_storage, 0, sizeof (sync_storage));
}  
