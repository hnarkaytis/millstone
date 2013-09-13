#ifdef HAVE_CONFIG_H
#include <config.h>
#endif /* HAVE_CONFIG_H */
#include <millstone.h>
#include <logging.h>
#include <block.h>
#include <mapped_region.h>

#include <sys/mman.h> /* mmap64, unmap */
#include <errno.h> /* errno */
#include <string.h> /* memset, setlen, strerror */

#include <pthread.h>

void
mapped_region_init (mapped_region_t * mapped_region, int protect, int flags)
{
  memset (mapped_region, 0, sizeof (*mapped_region));
  mapped_region->protect = protect;
  mapped_region->flags = flags;
  mapped_region->ref_count = 0;
  pthread_mutex_init (&mapped_region->mutex, NULL);
  pthread_cond_init (&mapped_region->cond, NULL);
}

void
mapped_region_unmap (mapped_region_t * mapped_region)
{
  if (mapped_region->data != NULL)
    {
      /* wait until there will be no active users for existing mapping */
      while (mapped_region->ref_count != 0)
	pthread_cond_wait (&mapped_region->cond, &mapped_region->mutex);
      if (0 != munmap (mapped_region->data, mapped_region->size))
	ERROR_MSG ("Failed to unmap memory. Error (%d) %s.\n", errno, strerror (errno));
      mapped_region->data = NULL;
    }
}

void
mapped_region_free (mapped_region_t * mapped_region)
{
  pthread_mutex_lock (&mapped_region->mutex);
  mapped_region_unmap (mapped_region);
  pthread_mutex_unlock (&mapped_region->mutex);
  memset (mapped_region, 0, sizeof (*mapped_region));
}

void
mapped_region_unref (mapped_region_t * mapped_region)
{
  pthread_mutex_lock (&mapped_region->mutex);
  if (--mapped_region->ref_count == 0)
    pthread_cond_signal (&mapped_region->cond);
  pthread_mutex_unlock (&mapped_region->mutex);
}

unsigned char *
mapped_region_get_addr (context_t * context, block_id_t * block_id)
{
  mapped_region_t * mapped_region = &context->mapped_region;
  unsigned char * addr = NULL;

  pthread_mutex_lock (&mapped_region->mutex);
  if ((NULL == mapped_region->data) ||
      (block_id->offset < mapped_region->offset) ||
      (block_id->offset + block_id->size > mapped_region->offset + mapped_region->size))
    {
      mapped_region_unmap (mapped_region);
      
      /* allign mapping region offset on a boundary of MAX_BLOCK_SIZE */
      mapped_region->offset = block_id->offset - (block_id->offset % MAX_BLOCK_SIZE);
      /* set mapping size to maximum possible */
      mapped_region->size = MAX_BLOCK_SIZE;
      /* extend mapping size if requested block exceeds expected maximum */
      if (mapped_region->size < block_id->size + (block_id->offset - mapped_region->offset))
	mapped_region->size = block_id->size + (block_id->offset - mapped_region->offset);
      /* trim mapping size by file size */
      if (mapped_region->size > context->size - mapped_region->offset)
	mapped_region->size = context->size - mapped_region->offset;
  
      unsigned char * data = mmap64 (NULL, mapped_region->size,
				     mapped_region->protect, mapped_region->flags,
				     context->file_fd, mapped_region->offset);
  
      if (-1 == (long)data)
	FATAL_MSG ("Failed to map file into memory. Error (%d) %s.\n", errno, strerror (errno));
      else
	mapped_region->data = data;
    }
  
  if (NULL != mapped_region->data)
    {
      addr = &mapped_region->data[block_id->offset - mapped_region->offset];
      ++mapped_region->ref_count;
    }

  pthread_mutex_unlock (&mapped_region->mutex);
  return (addr);
}
