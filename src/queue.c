#include <millstone.h>
#include <logging.h>
#include <queue.h>

#include <stddef.h>
#include <string.h>

#include <pthread.h>
#include <metaresc.h>

status_t
queue_init (queue_t * queue, size_t elem_size, int count, char * elem_type)
{
  memset (queue, 0, sizeof (queue));
  queue->head = 0;
  queue->tail = 0;
  queue->elem_size = elem_size;
  queue->count = count;
  queue->used = 0;
  queue->cancel = FALSE;
  pthread_mutex_init (&queue->mutex, NULL);
  pthread_cond_init (&queue->full, NULL);
  pthread_cond_init (&queue->empty, NULL);

  int typed_array_size = 0;
  if (queue->elem_type != NULL)
    typed_array_size = queue->count * sizeof (queue->typed_array.data[0]);
  queue->array = MR_MALLOC (queue->count * queue->elem_size + typed_array_size);
  if (NULL == queue->array)
    {
      ERROR_MSG ("Out of memory.");
      return (ST_FAILURE);
    }
  memset (queue->array, 0, queue->count * queue->elem_size + typed_array_size);
  if (queue->elem_type != NULL)
    {
      int i;
      char * array_char = queue->array;
      queue->typed_array.data = (void*)&array_char[queue->count * queue->elem_size];
      queue->typed_array.size = typed_array_size;
      queue->typed_array.alloc_size = -1;
      for (i = 0; i < queue->count; ++i)
	queue->typed_array.data[i].ptr = &array_char[queue->elem_size * i];
    }
  return (ST_SUCCESS);
}

status_t
queue_push (queue_t * queue, void * elem)
{
  char * array = queue->array;

  pthread_mutex_lock (&queue->mutex);
  while ((queue->used == queue->count) && (!queue->cancel))
    pthread_cond_wait (&queue->full, &queue->mutex);

  if (!queue->cancel)
    {
      memcpy (&array[queue->head * queue->elem_size], elem, queue->elem_size);

      if (++queue->head == queue->count)
	queue->head = 0;
      if (queue->used++ == 0)
	pthread_cond_broadcast (&queue->empty);
    }
  pthread_mutex_unlock (&queue->mutex);

  return (queue->cancel ? ST_FAILURE : ST_SUCCESS);
}

status_t
queue_pop_bulk (queue_t * queue, void * buf, size_t * buf_size)
{
  char * array = queue->array;
  int count = *buf_size / queue->elem_size;

  if (count <= 0)
    {
      *buf_size = 0;
      return (ST_SUCCESS);
    }

  pthread_mutex_lock (&queue->mutex);
  while ((queue->used == 0) && (!queue->cancel))
    pthread_cond_wait (&queue->empty, &queue->mutex);

  if (!queue->cancel)
    {
      if (queue->used < count)
	count = queue->used;

      if (count <= queue->count - queue->tail)
	memcpy (buf, &array[queue->tail * queue->elem_size], queue->elem_size * count);
      else
	{
	  char * buf_char = buf;
	  size_t first_part_size = queue->elem_size * (queue->count - queue->tail);
	  memcpy (buf, &array[queue->tail * queue->elem_size], first_part_size);
	  memcpy (&buf_char[first_part_size], array, queue->elem_size * count - first_part_size);
	}
      *buf_size = queue->elem_size * count;

      queue->tail += count;
      if (queue->tail >= queue->count)
	queue->tail -= queue->count;
      if (queue->used == queue->count)
	pthread_cond_broadcast (&queue->full);
      queue->used -= count;
    }

  pthread_mutex_unlock (&queue->mutex);

  return (queue->cancel ? ST_FAILURE : ST_SUCCESS);
}

status_t
queue_pop (queue_t * queue, void * elem)
{
  size_t buf_size = queue->elem_size;
  return (queue_pop_bulk (queue, elem, &buf_size));
}

void
queue_cancel (queue_t * queue)
{
  queue->cancel = TRUE;
  pthread_cond_broadcast (&queue->full);
  pthread_cond_broadcast (&queue->empty);
  
  pthread_mutex_lock (&queue->mutex);
  if (queue->array != NULL)
    MR_FREE (queue->array);
  queue->array = NULL;
  pthread_mutex_unlock (&queue->mutex);
}
