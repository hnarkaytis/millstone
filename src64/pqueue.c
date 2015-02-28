#include <millstone.h> /* status_t */
#include <pqueue.h> /* pqueue_t, task_t */

#include <stddef.h> /* size_t, ssize_t */
#include <stdbool.h> /* bool */
#include <string.h> /* memset, strerror */

#include <pthread.h>

#include <metaresc.h>

#undef MR_CHECK_TYPES
#define MR_CHECK_TYPES(...)
#undef MR_SAVE
#define MR_SAVE MR_SAVE_STR_TYPED

void
pqueue_init (pqueue_t * pqueue, size_t elem_size, char * elem_type, mr_compar_fn_t compar_fn, void * context)
{
  memset (pqueue, 0, sizeof (*pqueue));
  pthread_mutex_init (&pqueue->mutex, NULL);
  pthread_cond_init (&pqueue->cond, NULL);
  pqueue->elem_size = elem_size;
  pqueue->elem_type = elem_type;
  pqueue->compar_fn = compar_fn;
  pqueue->context = context;
}

void
pqueue_cancel (pqueue_t * pqueue)
{
  int i;
  
  pthread_mutex_lock (&pqueue->mutex);
  pqueue->cancel = TRUE;
  for (i = pqueue->heap_size / sizeof (pqueue->heap[0]) - 1; i >= 0; --i)
    {
      if (pqueue->elem_type)
	MR_FREE_RECURSIVELY (pqueue->elem_type, pqueue->heap[i].ptr);
      MR_FREE (pqueue->heap[i].ptr);
    }
  if (pqueue->heap)
    MR_FREE (pqueue->heap);
  pqueue->heap = NULL;
  pqueue->heap_size = 0;
  pqueue->heap_alloc_size = 0;
  pthread_cond_broadcast (&pqueue->cond);
  pthread_mutex_unlock (&pqueue->mutex);
}

status_t
pqueue_push (pqueue_t * pqueue, void * elem)
{
  status_t status = ST_FAILURE;
  void * slot = MR_MALLOC (pqueue->elem_size);
  if (NULL == slot)
    return (status);
  memcpy (slot, elem, pqueue->elem_size);
  
  pthread_mutex_lock (&pqueue->mutex);
  if (!pqueue->cancel)
    {
      mr_ptr_t * add = mr_rarray_allocate_element ((void**)&pqueue->heap, &pqueue->heap_size, &pqueue->heap_alloc_size, sizeof (pqueue->heap[0]));
      if (NULL != add)
	{
	  if ((NULL != pqueue->elem_type) &&
	      (MR_SUCCESS != MR_COPY_RECURSIVELY (pqueue->elem_type, elem, slot)))
	    pqueue->heap_size -= sizeof (pqueue->heap[0]);
	  else
	    {
	      status = ST_SUCCESS;
	  
	      int idx = pqueue->heap_size / sizeof (pqueue->heap[0]) - 1;
	      if (0 == idx)
		pthread_cond_broadcast (&pqueue->cond);
	      
	      while (idx > 0)
		{
		  int parent = (idx - 1) >> 1;
		  if (pqueue->compar_fn (pqueue->heap[parent], slot, pqueue->context) <= 0)
		    break;
		  pqueue->heap[idx] = pqueue->heap[parent];
		  idx = parent;
		}
	      pqueue->heap[idx].ptr = slot;
	    }
	}
    }
  pthread_mutex_unlock (&pqueue->mutex);

  if (ST_SUCCESS != status)
    MR_FREE (slot);
  
  return (status);
}

status_t
pqueue_pop (pqueue_t * pqueue, void * elem)
{
  status_t status = ST_FAILURE;
  pthread_mutex_lock (&pqueue->mutex);
  while ((0 == pqueue->heap_size) && (!pqueue->cancel))
    pthread_cond_wait (&pqueue->cond, &pqueue->mutex);
  
  if (!pqueue->cancel)
    {
      status = ST_SUCCESS;
      memcpy (elem, pqueue->heap[0].ptr, pqueue->elem_size);
      MR_FREE (pqueue->heap[0].ptr);
      pqueue->heap_size -= sizeof (pqueue->heap[0]);
      int heap_size = pqueue->heap_size / sizeof (pqueue->heap[0]);
      int idx = 0;
      for (;;)
	{
	  int child = (idx << 1) + 1;
	  if (child >= heap_size)
	    break;
	  if ((child + 1 < heap_size) &&
	      (pqueue->compar_fn (pqueue->heap[child + 1], pqueue->heap[child], pqueue->context) <= 0))
	    ++child;
	  if (pqueue->compar_fn (pqueue->heap[heap_size], pqueue->heap[child], pqueue->context) <= 0)
	    break;
	  pqueue->heap[idx] = pqueue->heap[child];
	  idx = child;
	}
      pqueue->heap[idx] = pqueue->heap[heap_size];
    }
  pthread_mutex_unlock (&pqueue->mutex);
  return (status);
}
