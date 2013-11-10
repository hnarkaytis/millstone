#include <millstone.h> /* status_t */
#include <pqueue.h> /* pqueue_t, task_t */

#include <stddef.h> /* size_t, ssize_t */
#include <stdbool.h> /* bool */
#include <string.h> /* memset, strerror */

#include <pthread.h>

#include <metaresc.h>

void
pqueue_init (pqueue_t * pqueue)
{
  memset (pqueue, 0, sizeof (*pqueue));
  pthread_mutex_init (&pqueue->mutex, NULL);
  pthread_cond_init (&pqueue->cond, NULL);
}

void
pqueue_cancel (pqueue_t * pqueue)
{
  pthread_mutex_lock (&pqueue->mutex);
  pqueue->cancel = TRUE;
  if (pqueue->heap.data)
    MR_FREE (pqueue->heap.data);
  pqueue->heap.data = NULL;
  pqueue->heap.size = 0;
  pqueue->heap.alloc_size = 0;
  pthread_cond_broadcast (&pqueue->cond);
  pthread_mutex_unlock (&pqueue->mutex);
}

status_t
pqueue_push (pqueue_t * pqueue, task_t * task)
{
  status_t status = ST_FAILURE;
  pthread_mutex_lock (&pqueue->mutex);
  if (!pqueue->cancel)
    {
      task_t * add = mr_rarray_append ((void*)&pqueue->heap, sizeof (pqueue->heap.data[0]));
      if (NULL != add)
	{
	  status = ST_SUCCESS;
	  *add = *task;
	  
	  int idx = pqueue->heap.size / sizeof (pqueue->heap.data[0]) - 1;
	  if (0 == idx)
	    pthread_cond_broadcast (&pqueue->cond);
	      
	  while (idx > 0)
	    {
	      int parent = (idx - 1) >> 1;
	      if (pqueue->heap.data[parent].size <= pqueue->heap.data[idx].size)
		break;
	      pqueue->heap.data[idx] = pqueue->heap.data[parent];
	      idx = parent;
	    }
	}
    }
  pthread_mutex_unlock (&pqueue->mutex);
  return (status);
}

status_t
pqueue_pop (pqueue_t * pqueue, task_t * task)
{
  status_t status = ST_FAILURE;
  pthread_mutex_lock (&pqueue->mutex);
  while ((0 == pqueue->heap.size) && (!pqueue->cancel))
    pthread_cond_wait (&pqueue->cond, &pqueue->mutex);
  
  if (!pqueue->cancel)
    {
      status = ST_SUCCESS;
      *task = pqueue->heap.data[0];
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
	      (pqueue->heap.data[child + 1].size < pqueue->heap.data[child].size))
	    ++child;
	  if (pqueue->heap.data[idx].size <= pqueue->heap.data[child].size)
	    break;
	}
    }
  pthread_mutex_unlock (&pqueue->mutex);
  return (status);
}
