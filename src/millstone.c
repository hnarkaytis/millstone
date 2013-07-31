#define _LARGEFILE64_SOURCE
#include <stdlib.h>

#include <block.h>

int main (int argc, char * argv[])
{
  DUMP_VAR (mr_td_t, &MR_DESCRIPTOR_PREFIX (block_id_t));
  return (EXIT_SUCCESS);
}
