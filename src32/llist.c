#include <millstone.h> /* status_t */
#include <logging.h>
#include <llist.h>

#include <stddef.h> /* size_t, ssize_t */
#include <stdbool.h> /* TRUE, FALSE */
#include <string.h> /* memset, memcpy */

#include <metaresc.h>

#undef MR_CHECK_TYPES
#define MR_CHECK_TYPES(...)
#undef MR_SAVE
#define MR_SAVE MR_SAVE_STR_TYPED

void
llist_init (llist_t * llist, size_t elem_size, char * elem_type, size_t max_count)
{
  memset (llist, 0, sizeof (*llist));
  llist->max_count = max_count;
  llist->count = 0;
  pthread_mutex_init (&llist->mutex, NULL);
  pthread_cond_init (&llist->full, NULL);
  pthread_cond_init (&llist->empty, NULL);
  llist->queue.next = &llist->queue;
  llist->queue.prev = &llist->queue;
  llist->elem_size = elem_size;
  llist->elem_type = elem_type;
  llist->cancel = FALSE;
}

status_t
llist_push (llist_t * llist, void * elem)
{
  if (llist->cancel)
    return (ST_FAILURE);

  status_t status = ST_FAILURE;
  llist_slot_t * llist_slot = MR_MALLOC (sizeof (*llist_slot) + llist->elem_size);
  
  if (llist_slot == NULL)
    {
      ERROR_MSG ("Out of memory.");
      return (ST_FAILURE);
    }

  llist_slot->ext.ptr = llist_slot->elem;
  memcpy (llist_slot->elem, elem, llist->elem_size);
      
  pthread_mutex_lock (&llist->mutex);
  while ((llist->count == llist->max_count) && (!llist->cancel))
    pthread_cond_wait (&llist->full, &llist->mutex);
      
  if (!llist->cancel)
    {
      if ((NULL != llist->elem_type) &&
	  (MR_SUCCESS != MR_COPY_RECURSIVELY (llist->elem_type, elem, llist_slot->elem)))
	status = ST_FAILURE;
      else
	{
	  status = ST_SUCCESS;
	  llist_slot->prev = &llist->queue;
	  llist_slot->next = llist->queue.next;
	  llist->queue.next->prev = llist_slot;
	  llist->queue.next = llist_slot;
	  if (llist->count++ == 0)
	    pthread_cond_broadcast (&llist->empty);
	}
    }
  pthread_mutex_unlock (&llist->mutex);

  if (ST_SUCCESS != status)
    MR_FREE (llist_slot);

  return (status);
}

status_t
llist_pop_bulk (llist_t * llist, void * buf, size_t * buf_size)
{
  char * char_buf = buf;
  llist_slot_t * first_slot = NULL;
  status_t status = ST_FAILURE;
  size_t count = *buf_size / llist->elem_size;

  if (count <= 0)
    {
      *buf_size = 0;
      return (ST_SUCCESS);
    }
  
  pthread_mutex_lock (&llist->mutex);
  while ((llist->count == 0) && (!llist->cancel))
    pthread_cond_wait (&llist->empty, &llist->mutex);

  if (!llist->cancel)
    {
      llist_slot_t * last_slot = first_slot = llist->queue.prev;

      if (llist->count == llist->max_count)
	pthread_cond_broadcast (&llist->full);

      for ( ; (count > 0) && (llist->count > 0); --count)
	{
	  last_slot = last_slot->prev;
	  --llist->count;
	}
      last_slot->next->prev = NULL;
      last_slot->next = &llist->queue;
      llist->queue.prev = last_slot;
      status = ST_SUCCESS;
    }
  pthread_mutex_unlock (&llist->mutex);

  *buf_size = 0;
  while (first_slot != NULL)
    {
      void * slot = first_slot;
      memcpy (&char_buf[*buf_size], first_slot->elem, llist->elem_size);
      *buf_size += llist->elem_size;
      first_slot = first_slot->prev;
      MR_FREE (slot);
    }
  
  return (status);
}

status_t
llist_pop (llist_t * llist, void * elem)
{
  size_t buf_size = llist->elem_size;
  return (llist_pop_bulk (llist, elem, &buf_size));
}

void
llist_cancel (llist_t * llist)
{
  if (!llist->cancel)
    {
      llist->cancel = TRUE;
      pthread_cond_broadcast (&llist->full);
      pthread_cond_broadcast (&llist->empty);
      
      pthread_mutex_lock (&llist->mutex);
      while (llist->queue.prev != &llist->queue)
	{
	  llist_slot_t * llist_slot = llist->queue.prev;
	  llist_slot->prev->next = &llist->queue;
	  llist->queue.prev = llist_slot->prev;
	  if (llist->elem_type)
	    MR_FREE_RECURSIVELY (llist->elem_type, llist_slot->elem);
	  MR_FREE (llist_slot);
	  --llist->count;
	}
      pthread_mutex_unlock (&llist->mutex);
    }
}
