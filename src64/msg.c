#define _GNU_SOURCE /* TEMP_FAILURE_RETRY */

#include <millstone.h> /* status_t */
#include <logging.h>
#include <msg.h> /* msg_t */

#include <stddef.h> /* size_t, ssize_t */
#include <string.h> /* memset */
#include <inttypes.h> /* uint64_t */
#include <unistd.h> /* TEMP_FAILURE_RETRY, read */
#include <errno.h> /* errno for TEMP_FAILURE_RETRY */
#include <sys/uio.h> /* writev, struct iovec */

#include <pthread.h>

#include <metaresc.h>

//#define DEBUG_PROTOCOL
#ifdef DEBUG_PROTOCOL
#define SERIALIZE MR_SAVE_CINIT_RA
#define DESERIALIZE MR_LOAD_CINIT_RA
#else
#define SERIALIZE MR_SAVE_XDR_RA
#define DESERIALIZE MR_LOAD_XDR_RA
#endif

static volatile uint64_t bytes_sent = 0;
static volatile uint64_t bytes_recv = 0;
static pthread_mutex_t bytes_sent_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t bytes_recv_mutex = PTHREAD_MUTEX_INITIALIZER;

uint64_t
stat_bytes_sent (uint64_t bytes)
{
  pthread_mutex_lock (&bytes_sent_mutex);
  bytes_sent += bytes;
  pthread_mutex_unlock (&bytes_sent_mutex);
  return (bytes_sent);
}

uint64_t
stat_bytes_recv (uint64_t bytes)
{
  pthread_mutex_lock (&bytes_recv_mutex);
  bytes_recv += bytes;
  pthread_mutex_unlock (&bytes_recv_mutex);
  return (bytes_recv);
}

status_t
buf_recv (int fd, void * buf, size_t size)
{
  char * char_buf = buf;
  ssize_t bytes, rv;

  for (bytes = 0; bytes != size; bytes += rv)
    {
      rv = TEMP_FAILURE_RETRY (read (fd, &char_buf[bytes], size - bytes));
      if (rv <= 0)
	return (ST_FAILURE);
    }
  stat_bytes_recv (size);
  return (ST_SUCCESS);
}

status_t
msg_recv (int fd, msg_t * msg)
{
  mr_rarray_t rarray;
  memset (&rarray, 0, sizeof (rarray));
  
  status_t status = buf_recv (fd, &rarray.size, sizeof (rarray.size));
  if (ST_SUCCESS != status)
    return (ST_FAILURE);
  
  char data[rarray.size];
  status = buf_recv (fd, data, rarray.size);
  if (ST_SUCCESS != status)
    return (ST_FAILURE);
  
  rarray.data = data;
  mr_status_t mr_status = DESERIALIZE (msg_t, &rarray, msg);
  return ((MR_SUCCESS == mr_status) ? ST_SUCCESS : ST_FAILURE);
}

status_t
buf_send (int fd, struct iovec * iov, size_t count)
{
  for (;;)
    {
      ssize_t rv = TEMP_FAILURE_RETRY (writev (fd, iov, count));
      if (rv <= 0)
	return (ST_FAILURE);

      stat_bytes_sent (rv);

      while (rv >= iov->iov_len)
	{
	  rv -= iov->iov_len;
	  ++iov;
	  if (0 == --count)
	    break;
	}
      if (0 == count)
	break;
      iov->iov_len -= rv;
      iov->iov_base = ((char*)iov->iov_base) + rv;
    }
  return (ST_SUCCESS);
}

status_t
msg_send (int fd, msg_t * msg)
{
  mr_rarray_t rarray = SERIALIZE (msg_t, msg);
  if (NULL == rarray.data)
    return (ST_FAILURE);

  struct iovec iov[] = {
    { .iov_len = sizeof (rarray.size), .iov_base = &rarray.size, },
    { .iov_len = rarray.size, .iov_base = rarray.data, },
  };

  status_t status = buf_send (fd, iov, sizeof (iov) / sizeof (iov[0]));

  MR_FREE (rarray.data);

  return (status);
}
