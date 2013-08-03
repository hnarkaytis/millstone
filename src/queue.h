#ifndef _QUEUE_H_
#define _QUEUE_H_

#include <pthread.h>
#include <semaphore.h>

#define QUEUE_SIZE (4)

typedef struct {
  char * queue;
  size_t count;
  size_t elem_size;
  int head;
  int tail;
  pthread_mutex_t head_mutex;
  pthread_mutex_t tail_mutex;
  sem_t full;
  sem_t empty;
} queue_t;

extern status_t queue_init (queue_t * queue, size_t count, size_t elem_size);
extern void queue_free (queue_t * queue);
extern void queue_push (queue_t * queue, void * element);
extern void queue_pop (queue_t * queue, void * element);

#endif /* _QUEUE_H_ */
