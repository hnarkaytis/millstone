#define _GNU_SOURCE
#include <unistd.h> /* TEMP_FAILURE_RETRY */
#include <errno.h> /* errno for TEMP_FAILURE_RETRY */

#include <millstone.h> /* status_t */
#include <logging.h>
#include <queue.h> /* queue_t */
#include <msg.h> /* msg_t */

status_t
msg_recv (int fd, msg_t * msg)
{
  ssize_t len = sizeof (*msg);
  ssize_t rv = TEMP_FAILURE_RETRY (read (fd, msg, len));
  return ((rv == len) ? ST_SUCCESS : ST_FAILURE);
}

status_t
msg_send (int fd, msg_t * msg)
{
  ssize_t len = sizeof (*msg);
  ssize_t rv = TEMP_FAILURE_RETRY (write (fd, msg, len));
  return ((rv == len) ? ST_SUCCESS : ST_FAILURE);
}

void
msg_queue_init (msg_queue_t * msg_queue, msg_t * array, size_t size)
{
  msg_queue->array.data = array;
  msg_queue->array.size = size;
  msg_queue->array.alloc_size = -1;
  queue_init (&msg_queue->queue, (mr_rarray_t*)&msg_queue->array, sizeof (msg_queue->array.data[0]));
}
