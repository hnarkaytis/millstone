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
  char * msg_buf = (char*)msg;
  ssize_t size = 0;
  while (size != sizeof (*msg))
    {
      ssize_t rv = TEMP_FAILURE_RETRY (read (fd, &msg_buf[size], sizeof (*msg) - size));
      if (rv <= 0)
	return (ST_FAILURE);
      size += rv;
    }
  return (ST_SUCCESS);
}

status_t
msg_send (int fd, msg_t * msg)
{
  char * msg_buf = (char*)msg;
  ssize_t size = 0;
  while (size != sizeof (*msg))
    {
      ssize_t rv = TEMP_FAILURE_RETRY (write (fd, &msg_buf[size], sizeof (*msg) - size));
      if (rv <= 0)
	return (ST_FAILURE);
      size += rv;
    }
  return (ST_SUCCESS);
}

void
msg_queue_init (msg_queue_t * msg_queue, msg_t * array, size_t size)
{
  msg_queue->array.data = array;
  msg_queue->array.size = size;
  msg_queue->array.alloc_size = -1;
  queue_init (&msg_queue->queue, (mr_rarray_t*)&msg_queue->array, sizeof (msg_queue->array.data[0]));
}
