#define _GNU_SOURCE /* TEMP_FAILURE_RETRY */

#ifdef HAVE_CONFIG_H
#include <config.h>
#endif /* HAVE_CONFIG_H */
#include <millstone.h>
#include <logging.h>
#include <block.h>
#include <sync_storage.h>
#include <file.h>

#include <fcntl.h> /* off64_t */
#include <stdbool.h> /* bool */
#include <inttypes.h> /* SCNx64 */
#include <sys/mman.h> /* mmap64, unmap */
#include <errno.h> /* errno */
#include <string.h> /* memset, setlen, strerror */

#include <pthread.h>

#include <metaresc.h>

static off64_t
chunk_get_id (const file_t * file, off64_t offset)
{
  return (offset / file->chunk_size);
}

int
chunk_compar (const mr_ptr_t x, const mr_ptr_t y, const void * context)
{
  const file_t * file = context;
  const chunk_t * x_ = x.ptr;
  const chunk_t * y_ = y.ptr;
  off64_t x_id = chunk_get_id (file, x_->block_id.offset);
  off64_t y_id = chunk_get_id (file, y_->block_id.offset);
  return ((x_id > y_id) - (x_id < y_id));
}

mr_hash_value_t
chunk_hash (const mr_ptr_t x, const void * context)
{
  const file_t * file = context;
  const chunk_t * chunk = x.ptr;
  return (chunk_get_id (file, chunk->block_id.offset));
}

static chunk_t *
chunk_map (file_t * file, off64_t offset)
{
  chunk_t * chunk = NULL;

  if (ST_SUCCESS == llist_pop (&file->chunks_pool, &chunk))
    {
      chunk->block_id.offset = offset - offset % file->chunk_size;
      chunk->block_id.size = file->chunk_size;
      if (chunk->block_id.size > file->size - chunk->block_id.offset)
	chunk->block_id.size = file->size - chunk->block_id.offset;

      chunk->data = NULL;
      chunk->ref_count = 1;

      if (chunk->block_id.size > 0)
	{
	  DUMP_VAR (chunk_t, chunk);

	  unsigned char * data = mmap64 (NULL, chunk->block_id.size,
					 file->protect, file->flags,
					 file->fd, chunk->block_id.offset);
	  if (-1 == (long)data)
	    {
	      FATAL_MSG ("Failed to map file into memory. Error (%d) %s.", errno, strerror (errno));
	      llist_push (&file->chunks_pool, &chunk);
	      chunk = NULL;
	    }
	  else
	    {
	      chunk->data = data;
	      sync_storage_add (&file->chunks_index, chunk);
	    }
	}
    }

  DUMP_VAR (file_t, file);

  return (chunk);
}

static void
inc_ref_count (mr_ptr_t found, mr_ptr_t orig, void * context)
{
  chunk_t * chunk = found.ptr;
  ++chunk->ref_count;
}

chunk_t *
chunk_ref (file_t * file, off64_t offset)
{
  chunk_t chunk_id = { .block_id = { .offset = offset - offset % file->chunk_size, }, };
  mr_ptr_t * find = sync_storage_find (&file->chunks_index, &chunk_id, inc_ref_count);
  chunk_t * chunk = (NULL == find) ? chunk_map (file, offset) : find->ptr;
  
  if (file->cancel)
    chunk = NULL;
  
  return (chunk);
}

static void
dec_ref_count (mr_ptr_t found, mr_ptr_t orig, void * context)
{
  chunk_t * chunk = found.ptr;
  chunk_t * chunk_id = orig.ptr;
  --chunk->ref_count;
  *chunk_id = *chunk;
}

static status_t
chunk_release (file_t * file, chunk_t * chunk)
{
  if (file->chunk_release)
    file->chunk_release (chunk, file->context);
	  
  sync_storage_del (&file->chunks_index, chunk, NULL);
  munmap (chunk->data, chunk->block_id.size);
  chunk->data = NULL;
  return (llist_push (&file->chunks_pool, &chunk));
}

status_t
chunk_unref (file_t * file, off64_t offset)
{
  status_t status = ST_SUCCESS;
  chunk_t chunk_id = { .block_id = { .offset = offset - offset % file->chunk_size, }, };
  mr_ptr_t * found = sync_storage_find (&file->chunks_index, &chunk_id, dec_ref_count);
  
  if (found != NULL)
    {
      chunk_t * chunk = found->ptr;
      status = ST_SUCCESS;
      if (0 == chunk_id.ref_count)
	status = chunk_release (file, chunk);
    }
  
  if (file->cancel)
    status = ST_FAILURE;
  
  return (status);
}

void *
file_chunks_get_addr (file_t * file, off64_t offset)
{
  chunk_t chunk_id = { .block_id = { .offset = offset - offset % file->chunk_size, }, };
  mr_ptr_t * find = sync_storage_find (&file->chunks_index, &chunk_id, inc_ref_count);
  chunk_t * chunk = (NULL == find) ? NULL : find->ptr;
  return ((NULL == chunk) ? NULL : &chunk->data[offset - chunk->block_id.offset]);
}

TYPEDEF_STRUCT (chunk_ptr_t, ATTRIBUTES (__attribute__ ((packed))),
		(chunk_t *, ptr),
		)

void
file_chunks_init (file_t * file, int protect, int flags, size_t size)
{
  int i;

  sync_storage_init (&file->chunks_index, chunk_compar, chunk_hash, NULL, "chunk_t", file);
  LLIST_INIT (&file->chunks_pool, chunk_ptr_t, sizeof (file->chunks) / sizeof (file->chunks[0]));
  memset (&file->chunks, 0, sizeof (file->chunks));

  file->chunk_size = size - size % PAGE_SIZE;
  file->chunk_release = NULL;
  file->context = NULL;
  file->cancel = FALSE;
  file->protect = protect;
  file->flags = flags;
  
  for (i = 0; i < sizeof (file->chunks) / sizeof (file->chunks[0]); ++i)
    {
      chunk_t * chunk = &file->chunks[i];
      chunk->ref_count = 0;
      llist_push (&file->chunks_pool, &chunk);
    }
}

void
file_chunks_cancel (file_t * file)
{
  file->cancel = TRUE;
  llist_cancel (&file->chunks_pool);
}

static mr_status_t
chunk_free (const mr_ptr_t node, const void * context)
{
  chunk_t * chunk = node.ptr;
  if (chunk->data != NULL)
    {
      munmap (chunk->data, chunk->block_id.size);
      chunk->data = NULL;
    }
  return (MR_SUCCESS);
}

void
file_chunks_free (file_t * file)
{
  sync_storage_yeld (&file->chunks_index, chunk_free);
}

void
file_chunks_finilize (file_t * file)
{
  int i;
  chunk_t * chunk;
  for (i = 0; i < sizeof (file->chunks) / sizeof (file->chunks[0]); ++i)
    if (ST_SUCCESS != llist_pop (&file->chunks_pool, &chunk))
      break;
}
  
void
file_chunks_set_release_handler (file_t * file, chunk_release_t chunk_release, void * context)
{
  file->chunk_release = chunk_release;
  file->context = context;
}

void
file_set_chunks_size (file_t * file, size_t chunk_size)
{
  file->chunk_size = chunk_size;
}
