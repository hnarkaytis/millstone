#ifdef HAVE_CONFIG_H
#include <config.h>
#endif /* HAVE_CONFIG_H */
#include <millstone.h> /* config_t, status_t */
#include <logging.h>
#include <sync_storage.h> /* sync_storage_t */
#include <file_pool.h>

#include <errno.h> /* errno */
#include <string.h> /* memset, strerror */
#include <fcntl.h> /* open64 */
#include <inttypes.h> /* int64_t, SCNx64 */
#include <unistd.h> /* close, lseek64, SEEK_END */
#include <sys/mman.h> /* mmap64, munmap */

TYPEDEF_UNION (fd_ptr_t,
	       ATTRIBUTES (, "this type should not be copied by MR_COPY_RECURSIVELY, but pointers should be resolved for existing objects"),
	       (void *, ptr),
	       (fd_t *, fd),
	       )

int
fd_compar (const mr_ptr_t x, const mr_ptr_t y, const void * context)
{
  const fd_t * x_fd = x.ptr;
  const fd_t * y_fd = y.ptr;
  return (x_fd->file.file_id.id - y_fd->file.file_id.id);
}

mr_hash_value_t
fd_hash (const mr_ptr_t x, const void * context)
{
  const fd_t * x_fd = x.ptr;
  return (x_fd->file.file_id.id);
}

void
file_pool_init (file_pool_t * file_pool)
{
  int i, count = sizeof (file_pool->fds) / sizeof (file_pool->fds[0]);
  
  memset (file_pool, 0, sizeof (*file_pool));
  file_pool->file_id.id = 0;

  sync_storage_init (&file_pool->fd_map, fd_compar, fd_hash, NULL, "fd_t", file_pool);
  LLIST_INIT (&file_pool->pool, fd_ptr_t, count);
  for (i = 0; i < count; ++i)
    {
      fd_ptr_t fd_ptr = { .fd = &file_pool->fds[i], };
      if (ST_SUCCESS != llist_push (&file_pool->pool, &fd_ptr))
	break;
    }
}

void
file_pool_finalize (file_pool_t * file_pool)
{
  int i, count = sizeof (file_pool->fds) / sizeof (file_pool->fds[0]);
  for (i = 0; i < count; ++i)
    {
      fd_ptr_t fd_ptr;
      if (ST_SUCCESS != llist_pop (&file_pool->pool, &fd_ptr))
	break;
    }
}

static status_t
fd_close (file_pool_t * file_pool, fd_t * fd)
{
  fd_ptr_t fd_ptr = { .fd = fd, };
  if (fd->data != NULL)
    munmap (fd->data, fd->file.size);
  fd->data = NULL;
  close (fd->fd);
  memset (fd, 0, sizeof (*fd));
  return (llist_push (&file_pool->pool, &fd_ptr));
}

fd_t *
server_open_file (file_pool_t * file_pool, file_t * file)
{
  fd_ptr_t fd_ptr;
  int fd = open64 (file->file_name, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
  
  if (fd <= 0)
    {
      ERROR_MSG ("Can't open/create destination file '%s.'", file->file_name);
      return (NULL);
    }

  int rv = ftruncate64 (fd, file->size);
  if (rv != 0)
    {
      ERROR_MSG ("Failed to set new size (%d) to the file '%s'.", file->size, file->file_name);
      close (fd);
      return (NULL);
    }

  if (ST_SUCCESS != llist_pop (&file_pool->pool, &fd_ptr))
    return (NULL);

  memset (fd_ptr.fd, 0, sizeof (*fd_ptr.fd));

  fd_ptr.fd->file = *file;
  fd_ptr.fd->fd = fd;
  fd_ptr.fd->data = NULL;
  unsigned char * data = mmap64 (NULL, fd_ptr.fd->file.size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  if (-1 == (long)data)
    {
      FATAL_MSG ("Failed to map file `%s` into memory. Error (%d) %s.", file->file_name, errno, strerror (errno));
      fd_close (file_pool, fd_ptr.fd);
      return (NULL);
    }

  fd_ptr.fd->data = data;
  fd_ptr.fd->ref_count = 0;
  pthread_mutex_init (&fd_ptr.fd->ref_count_mutex, NULL);
  sync_storage_add (&file_pool->fd_map, fd_ptr.fd);
  return (fd_ptr.fd);
}

fd_t *
client_open_file (file_pool_t * file_pool, char * file_name)
{
  fd_ptr_t fd_ptr;
  int fd = open64 (file_name, O_RDONLY);
  if (fd <= 0)
    {
      ERROR_MSG ("Can't open source file '%s'.", file_name);
      return (NULL);
    }
  
  if (ST_SUCCESS != llist_pop (&file_pool->pool, &fd_ptr))
    return (NULL);

  memset (fd_ptr.fd, 0, sizeof (*fd_ptr.fd));

  fd_ptr.fd->file.file_name = file_name;
  fd_ptr.fd->fd = fd;
  fd_ptr.fd->file.size = lseek64 (fd, 0, SEEK_END);
  if (fd_ptr.fd->file.size < 0)
    {
      FATAL_MSG ("Failed to get size for file `%s` into memory. Error (%d) %s.", file_name, errno, strerror (errno));
      fd_close (file_pool, fd_ptr.fd);
      return (NULL);
    }
  
  fd_ptr.fd->data = NULL;
  unsigned char * data = mmap64 (NULL, fd_ptr.fd->file.size, PROT_READ, MAP_PRIVATE, fd, 0);
  if (-1 == (long)data)
    {
      FATAL_MSG ("Failed to map file `%s` into memory. Error (%d) %s.", file_name, errno, strerror (errno));
      fd_close (file_pool, fd_ptr.fd);
      return (NULL);
    }
  
  fd_ptr.fd->data = data;
  fd_ptr.fd->ref_count = 0;
  pthread_mutex_init (&fd_ptr.fd->ref_count_mutex, NULL);
  fd_ptr.fd->sent_blocks = 0;
  pthread_mutex_init (&fd_ptr.fd->sent_blocks_mutex, NULL);
  fd_ptr.fd->file.file_id.id = ++file_pool->file_id.id; /* suppose to never overlap */
  sync_storage_add (&file_pool->fd_map, fd_ptr.fd);
  return (fd_ptr.fd);
}

void
file_pool_fd_close (file_pool_t * file_pool, fd_t * fd)
{
  sync_storage_del (&file_pool->fd_map, fd, NULL);
  fd_close (file_pool, fd);
}

int64_t
file_pool_ref_fd (file_pool_t * file_pool, fd_t * fd, int64_t delta)
{
  pthread_mutex_lock (&fd->ref_count_mutex);
  fd->ref_count += delta;
  int64_t ref_count = fd->ref_count;
  pthread_mutex_unlock (&fd->ref_count_mutex);

  TRACE_MSG ("Ref_count %" SCNx64, ref_count);

  return (ref_count);
}

void
file_sent_block (fd_t * fd)
{
  pthread_mutex_lock (&fd->sent_blocks_mutex);
  ++fd->sent_blocks;
  pthread_mutex_unlock (&fd->sent_blocks_mutex);
}

void
file_pool_cancel (file_pool_t * file_pool)
{
  llist_cancel (&file_pool->pool);
}

static mr_status_t
fd_close_wrapper (mr_ptr_t node, const void * context)
{
  fd_close ((file_pool_t*)context, node.ptr);
  return (MR_SUCCESS);
}

void
file_pool_cleanup (file_pool_t * file_pool)
{
  llist_cancel (&file_pool->pool);
  sync_storage_yeld (&file_pool->fd_map, fd_close_wrapper);
  sync_storage_free (&file_pool->fd_map);
}

fd_t *
file_pool_get_fd (file_pool_t * file_pool, file_id_t * file_id)
{
  fd_t fd;
  fd.file.file_id = *file_id;
  mr_ptr_t * find = sync_storage_find (&file_pool->fd_map, &fd, NULL);
  return (find ? find->ptr : NULL);
}
