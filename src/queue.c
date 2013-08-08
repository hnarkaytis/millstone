#include <millstone.h>
#include <logging.h>
#include <queue.h>

#include <stddef.h>
#include <string.h>

#include <pthread.h>
#include <metaresc.h>

void
queue_init (queue_t * queue, mr_rarray_t * array, size_t elem_size)
{
  queue->array = array;
  queue->head = 0;
  queue->tail = 0;
  queue->elem_size = elem_size;
  queue->count = array->size / elem_size;
  queue->used = 0;
  memset (array->data, 0, array->size);
  pthread_mutex_init (&queue->mutex, NULL);
  pthread_cond_init (&queue->full, NULL);
  pthread_cond_init (&queue->empty, NULL);
}

void
queue_push (queue_t * queue, void * element)
{
  char * array = queue->array->data;

  pthread_mutex_lock (&queue->mutex);
  while (queue->used == queue->count)
    pthread_cond_wait (&queue->full, &queue->mutex);
  
  memcpy (&array[queue->head * queue->elem_size], element, queue->elem_size);

  if (++queue->head == queue->count)
    queue->head = 0;
  if (queue->used++ == 0)
    pthread_cond_broadcast (&queue->empty);
  pthread_mutex_unlock (&queue->mutex);
}

void
queue_pop (queue_t * queue, void * element)
{
  char * array = queue->array->data;

  pthread_mutex_lock (&queue->mutex);
  while (queue->used == 0)
    pthread_cond_wait (&queue->empty, &queue->mutex);
  
  memcpy (element, &array[queue->tail * queue->elem_size], queue->elem_size);
  
  if (++queue->tail == queue->count)
    queue->tail = 0;
  if (queue->used-- == queue->count)
    pthread_cond_broadcast (&queue->full);
  pthread_mutex_unlock (&queue->mutex);
}

