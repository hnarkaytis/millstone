#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <pthread.h>
#include <semaphore.h>

#include "logging.h"
#include "queue.h"

status_t
queue_init (queue_t * queue, size_t count, size_t elem_size)
{
  queue->head = 0;
  queue->tail = 0;
  queue->count = count;
  queue->elem_size = elem_size;
  pthread_mutex_init (&queue->head_mutex, NULL);
  pthread_mutex_init (&queue->tail_mutex, NULL);
  sem_init (&queue->full, 0, 0);
  sem_init (&queue->empty, 0, count);
  queue->queue = malloc (count * elem_size);

  if (NULL == queue->queue)
    {
      ERROR_MSG ("Memory allocation failed.");
      return (ST_FAIL);
    }
  else
    return (ST_OK);
}

void
queue_free (queue_t * queue)
{
  if (queue->queue)
    free (queue->queue);
  queue->queue = NULL;
  queue->count = 0;
  queue->elem_size = 0;
}

void
queue_push (queue_t * queue, void * element)
{
  sem_wait (&queue->empty);
  pthread_mutex_lock (&queue->head_mutex);
  memcpy (&queue->queue[queue->head * queue->elem_size],
          element, queue->elem_size);

  ++queue->head;
  if (queue->head == queue->count)
    queue->head = 0;
  pthread_mutex_unlock (&queue->head_mutex);
  sem_post (&queue->full);
}

void
queue_pop (queue_t * queue, void * element)
{
  sem_wait (&queue->full);
  pthread_mutex_lock (&queue->tail_mutex);
  memcpy (element, &queue->queue[queue->tail * queue->elem_size],
          queue->elem_size);
  ++queue->tail;
  if (queue->tail == queue->count)
    queue->tail = 0;
  pthread_mutex_unlock (&queue->tail_mutex);
  sem_post (&queue->empty);
}

// vim: ts=2 sw=2 et cc=81 listchars=eol\:$,tab\:>-,trail\:X cinoptions=>4,n-2,{2,^-2,\:2,=2,g0,h2,p5,t0,+2,(0,u0,w1,m1
