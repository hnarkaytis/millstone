#include <millstone.h> /* status_t */
#include <block.h> /* block_id_t */
#include <task_queue.h> /* block_id_t */

void
task_queue_init (task_queue_t * task_queue)
{
  task_queue->count = 0;
  pthread_mutex_init (&task_queue->mutex, NULL);
  pthread_cond_init (&task_queue->empty, NULL);
  task_queue->queue.next = &task_queue->queue;
  task_queue->queue.prev = &task_queue->queue;
}

status_t
task_queue_push (task_queue_t * task_queue, task_t * task)
{
  status_t status = ST_FAILURE;
  pthread_mutex_lock (&task_queue->mutex);
  if (!task_queue->cancel)
    {
      task_t * slot = MR_MALLOC (sizeof (*slot));
      if (slot != NULL)
	{
	  *slot = *task;
	  slot->prev = &task_queue->queue;
	  slot->next = task_queue->queue.next;
	  task_queue->queue.next->prev = slot;
	  task_queue->queue.next = slot;
	  status = ST_SUCCESS;
	  if (task_queue->count++ == 0)
	    pthread_cond_broadcast (&task_queue->empty);
	}
    }
  pthread_mutex_unlock (&task_queue->mutex);
  return (status);
}

status_t
task_queue_pop (task_queue_t * task_queue, task_t * task)
{
  pthread_mutex_lock (&task_queue->mutex);
  while ((task_queue->count == 0) && (!task_queue->cancel))
    pthread_cond_wait (&task_queue->empty, &task_queue->mutex);

  if (!task_queue->cancel)
    {
      task_t * slot = task_queue->queue.prev;
      *task = *slot;
      slot->prev->next = &task_queue->queue;
      task_queue->queue.prev = slot->prev;
      --task_queue->count;
      MR_FREE (slot);
    }

  pthread_mutex_unlock (&task_queue->mutex);
  return (task_queue->cancel ? ST_FAILURE : ST_SUCCESS);
}

void
task_queue_cancel (task_queue_t * task_queue)
{
  task_queue->cancel = TRUE;
  pthread_cond_broadcast (&task_queue->empty);
  pthread_mutex_lock (&task_queue->mutex);
  while (task_queue->queue.prev != &task_queue->queue)
    {
      task_t * slot = task_queue->queue.prev;
      slot->prev->next = &task_queue->queue;
      task_queue->queue.prev = slot->prev;
      MR_FREE (slot);
    }
  pthread_mutex_unlock (&task_queue->mutex);
}
