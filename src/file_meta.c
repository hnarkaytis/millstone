#define _GNU_SOURCE /* TEMP_FAILURE_RETRY */

#include <millstone.h>
#include <logging.h>
#include <file_meta.h>

#include <unistd.h> /* TEMP_FAILURE_RETRY, sysconf, close, ftruncate64 */
#include <errno.h> /* errno for TEMP_FAILURE_RETRY */
#include <fcntl.h> /* open64, lseek64, SEEK_END */
#include <string.h> /* memset, setlen, strerror */
#include <errno.h> /* errno */
#include <stdbool.h> /* bool */
#include <limits.h> /* PATH_MAX */
#include <sys/stat.h> /* S_IRUSR, S_IWUSR */
#include <sys/uio.h> /* writev, struct iovec */
#include <sys/mman.h> /* mmap64, unmap */

status_t
read_file_meta (connection_t * connection)
{
  ssize_t len, rv;
  
  len = sizeof (connection->file->size);
  rv = TEMP_FAILURE_RETRY (read (connection->cmd_fd, &connection->file->size, len));
  if (rv != len)
    {
      ERROR_MSG ("Failed to read file size from client.");
      return (ST_FAILURE);
    }
  
  len = sizeof (connection->remote.sin_port);
  rv = TEMP_FAILURE_RETRY (read (connection->cmd_fd, &connection->remote.sin_port, len));
  if (rv != len)
    {
      ERROR_MSG ("Failed to read UDP data port number from client.");
      return (ST_FAILURE);
    }
  
  char dst_file[PATH_MAX + (1 << 10)];
  int count = 0;
  do {
    if (sizeof (dst_file) == count)
      {
	ERROR_MSG ("Destination file name is too long.");
	return (ST_FAILURE);
      }
    len = sizeof (dst_file[0]);
    rv = TEMP_FAILURE_RETRY (read (connection->cmd_fd, &dst_file[count], len));
    if (rv != len)
      {
	ERROR_MSG ("Failed to read file name.");
	return (ST_FAILURE);
      }
  } while (dst_file[count++] != 0);

  DEBUG_MSG ("Got from client: port %04x size:%zd filename '%s'.",
	     connection->remote.sin_port, connection->file->size, dst_file);
  
  rv = access (dst_file, F_OK);
  connection->file->file_exists = (0 == rv);
  if (connection->file->file_exists)
    {
      rv = access (dst_file, R_OK | W_OK);
      if (rv != 0)
	{
	  ERROR_MSG ("File (%s) access rights are not 'rw'.", dst_file);
	  return (ST_FAILURE);
	}
    }
  
  connection->file->fd = open64 (dst_file, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
  if (connection->file->fd <= 0)
    {
      ERROR_MSG ("Can't open/create destination file '%s.'", dst_file);
      return (ST_FAILURE);
    }

  rv = ftruncate64 (connection->file->fd, connection->file->size);
  if (rv != 0)
    {
      ERROR_MSG ("Failed to set new size (%d) to the file '%s'.", connection->file->size, dst_file);
      close (connection->file->fd);
      return (ST_FAILURE);
    }

  return (ST_SUCCESS);
}

status_t
send_file_meta (connection_t * connection)
{
  const struct iovec iov[] = {
    { .iov_len = sizeof (connection->file->size), .iov_base = &connection->file->size, },
    { .iov_len = sizeof (connection->local.sin_port), .iov_base = &connection->local.sin_port, },
    { .iov_len = strlen (connection->file->config->dst_file) + 1, .iov_base = connection->file->config->dst_file, },
  };

  ssize_t rv = TEMP_FAILURE_RETRY (writev (connection->cmd_fd, iov, sizeof (iov) / sizeof (iov[0])));
  ssize_t i, len = 0;
  for (i = 0; i < sizeof (iov) / sizeof (iov[0]); ++i)
    len += iov[i].iov_len;

  status_t status = ST_SUCCESS;
  if (rv != len)
    {
      ERROR_MSG ("Failed to send hand shake message (sent %d bytes, but expexted %d bytes)", rv, len);
      status = ST_FAILURE;
    }
  return (status);
}
