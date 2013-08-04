#include <millstone.h>
#include <logging.h>
#include <block.h>
#include <calc_digest.h>

#include <sys/mman.h> /* mmap64, unmap */
#include <errno.h> /* errno, strerror */

status_t
calc_digest (block_digest_t * block_digest, int fd)
{
  status_t status = ST_FAILURE;
  unsigned char * data = mmap64 (NULL, block_digest->block_id.size, PROT_READ, MAP_PRIVATE,
				 fd, block_digest->block_id.offset);
  if (-1 == (long)data)
    FATAL_MSG ("Failed to map file into memory. Error (%d) %s.\n", errno, strerror (errno));
  else
    {
      SHA1 (data, block_digest->block_id.size, (unsigned char*)block_digest->digest);
      
      if (0 != munmap (data, block_digest->block_id.size))
	ERROR_MSG ("Failed to unmap memory. Error (%d) %s.\n", errno, strerror (errno));
      else
	status = ST_SUCCESS;
    }
  return (status);
}
