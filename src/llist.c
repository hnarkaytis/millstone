#include <millstone.h> /* status_t */
#include <logging.h>
#include <llist.h>

#include <stddef.h> /* size_t, ssize_t */
#include <string.h> /* memset, memcpy */

void
llist_init (llist_t * llist, size_t elem_size, char * elem_type)
{
  memset (llist, 0, sizeof (*llist));
  llist->count = 0;
  pthread_mutex_init (&llist->mutex, NULL);
  pthread_cond_init (&llist->empty, NULL);
  llist->queue.next = &llist->queue;
  llist->queue.prev = &llist->queue;
  llist->elem_size = elem_size;
  llist->elem_type = elem_type;
}

status_t
llist_push (llist_t * llist, void * elem)
{
  if (llist->cancel)
    return (ST_FAILURE);

  status_t status = ST_FAILURE;
  llist_slot_t * llist_slot = MR_MALLOC (sizeof (*llist_slot) + llist->elem_size);
  
  if (llist_slot == NULL)
    ERROR_MSG ("Out of memory.");
  else
    {
      llist_slot->ext.ptr = llist_slot->elem;
      memcpy (llist_slot->elem, elem, llist->elem_size);
      
      pthread_mutex_lock (&llist->mutex);
      if (!llist->cancel)
	{
	  llist_slot->prev = &llist->queue;
	  llist_slot->next = llist->queue.next;
	  llist->queue.next->prev = llist_slot;
	  llist->queue.next = llist_slot;
	  status = ST_SUCCESS;
	  if (llist->count++ == 0)
	    pthread_cond_broadcast (&llist->empty);
	}
      pthread_mutex_unlock (&llist->mutex);

      if (ST_SUCCESS != status)
	MR_FREE (llist_slot);
    }
  return (status);
}

status_t
llist_pop (llist_t * llist, void * elem)
{
  llist_slot_t * llist_slot = &llist->queue;
  status_t status = ST_FAILURE;
  
  pthread_mutex_lock (&llist->mutex);
  while ((llist->count == 0) && (!llist->cancel))
    pthread_cond_wait (&llist->empty, &llist->mutex);

  if (!llist->cancel)
    {
      llist_slot = llist->queue.prev;
      llist_slot->prev->next = &llist->queue;
      llist->queue.prev = llist_slot->prev;
      --llist->count;
      status = ST_SUCCESS;
    }
  pthread_mutex_unlock (&llist->mutex);

  if (llist_slot != &llist->queue)
    {
      memcpy (elem, llist_slot->elem, llist->elem_size);
      MR_FREE (llist_slot);
    }
  
  return (status);
}

void
llist_cancel (llist_t * llist)
{
  if (!llist->cancel)
    {
      llist->cancel = TRUE;
      pthread_cond_broadcast (&llist->empty);
      pthread_mutex_lock (&llist->mutex);

      DUMP_VAR (llist_t, llist);
      
      while (llist->queue.prev != &llist->queue)
	{
	  llist_slot_t * llist_slot = llist->queue.prev;
	  llist_slot->prev->next = &llist->queue;
	  llist->queue.prev = llist_slot->prev;
	  MR_FREE (llist_slot);
	  --llist->count;
	}
      pthread_mutex_unlock (&llist->mutex);
    }
}
