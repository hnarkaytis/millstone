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
  for (i = pqueue->heap.size / sizeof (pqueue->heap.data[0]) - 1; i >= 0; --i)
    {
      if (pqueue->elem_type)
	MR_FREE_RECURSIVELY (pqueue->elem_type, pqueue->heap.data[i].ptr);
      MR_FREE (pqueue->heap.data[i].ptr);
    }
  if (pqueue->heap.data)
    MR_FREE (pqueue->heap.data);
  pqueue->heap.data = NULL;
  pqueue->heap.size = 0;
  pqueue->heap.alloc_size = 0;
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
      mr_ptr_t * add = mr_rarray_append ((void*)&pqueue->heap, sizeof (pqueue->heap.data[0]));
      if (NULL != add)
	{
	  if ((NULL != pqueue->elem_type) &&
	      (MR_SUCCESS != MR_COPY_RECURSIVELY (pqueue->elem_type, elem, slot)))
	    pqueue->heap.size -= sizeof (pqueue->heap.data[0]);
	  else
	    {
	      status = ST_SUCCESS;
	      add->ptr = slot;
	  
	      int idx = pqueue->heap.size / sizeof (pqueue->heap.data[0]) - 1;
	      if (0 == idx)
		pthread_cond_broadcast (&pqueue->cond);
	      
	      while (idx > 0)
		{
		  int parent = (idx - 1) >> 1;
		  if (pqueue->compar_fn (pqueue->heap.data[parent], pqueue->heap.data[idx], pqueue->context) <= 0)
		    break;
		  pqueue->heap.data[idx] = pqueue->heap.data[parent];
		  idx = parent;
		}
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
  while ((0 == pqueue->heap.size) && (!pqueue->cancel))
    pthread_cond_wait (&pqueue->cond, &pqueue->mutex);
  
  if (!pqueue->cancel)
    {
      status = ST_SUCCESS;
      memcpy (elem, pqueue->heap.data[0].ptr, pqueue->elem_size);
      pqueue->heap.size -= sizeof (pqueue->heap.data[0]);
      int heap_size = pqueue->heap.size / sizeof (pqueue->heap.data[0]);
      pqueue->heap.data[0] = pqueue->heap.data[heap_size];
      int idx = 0;
      for (;;)
	{
	  int child = (idx << 1) + 1;
	  if (child >= heap_size)
	    break;
	  if ((child + 1 < heap_size) &&
	      (pqueue->compar_fn (pqueue->heap.data[child + 1], pqueue->heap.data[child], pqueue->context) <= 0))
	    ++child;
	  if (pqueue->compar_fn (pqueue->heap.data[idx], pqueue->heap.data[child], pqueue->context) <= 0)
	    break;
	}
    }
  pthread_mutex_unlock (&pqueue->mutex);
  return (status);
}
