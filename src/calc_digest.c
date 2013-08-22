#include <millstone.h>
#include <logging.h>
#include <block.h>
#include <calc_digest.h>

#include <sys/mman.h> /* mmap64, unmap */
#include <string.h> /* strerror */
#include <errno.h> /* errno */
#include <sys/user.h> /* PAGE_SIZE */

#include <openssl/sha.h> /* SHA1 */

status_t
calc_digest (block_digest_t * block_digest, int fd)
{
  status_t status = ST_FAILURE;
  off64_t offset = block_digest->block_id.offset & ~(PAGE_SIZE - 1);
  size_t shift = block_digest->block_id.offset & (PAGE_SIZE - 1);
  unsigned char * data = mmap64 (NULL, block_digest->block_id.size + shift, PROT_READ, MAP_PRIVATE, fd, offset);
  
  if (-1 == (long)data)
    FATAL_MSG ("Failed to map file into memory. Error (%d) %s.\n", errno, strerror (errno));
  else
    {
      SHA1 (&data[shift], block_digest->block_id.size, (unsigned char*)block_digest->digest);
      
      if (0 != munmap (data, block_digest->block_id.size))
	ERROR_MSG ("Failed to unmap memory. Error (%d) %s.\n", errno, strerror (errno));
      else
	status = ST_SUCCESS;
    }
  return (status);
}
