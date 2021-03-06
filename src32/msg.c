#define _GNU_SOURCE /* TEMP_FAILURE_RETRY */

#include <unistd.h> /* TEMP_FAILURE_RETRY */
#include <errno.h> /* errno for TEMP_FAILURE_RETRY */

#include <millstone.h> /* status_t */
#include <logging.h>
#include <msg.h> /* msg_t */

status_t
msg_recv (int fd, msg_t * msg)
{
  char * msg_buf = (char*)msg;
  ssize_t size, rv;
  for (size = 0; size != sizeof (*msg); size += rv)
    {
      rv = TEMP_FAILURE_RETRY (read (fd, &msg_buf[size], sizeof (*msg) - size));
      if (rv <= 0)
	return (ST_FAILURE);
    }
  return (ST_SUCCESS);
}

status_t
msg_send (int fd, char * msg_buf, size_t size)
{
  ssize_t sent, rv;
  for (sent = 0; sent != size; sent += rv)
    {
      rv = TEMP_FAILURE_RETRY (write (fd, &msg_buf[sent], size - sent));
      if (rv <= 0)
	return (ST_FAILURE);
    }
  return (ST_SUCCESS);
}
