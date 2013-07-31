#define _LARGEFILE64_SOURCE
#include <stdlib.h>

#include <metaresc.h>

TYPEDEF_STRUCT (segment_id_t,
		(off64_t, offset),
		(size_t, size),
		)

#define DUMP_VAR(TYPE, VAR) ({			\
      char * dump = MR_SAVE_CINIT (TYPE, VAR);	\
      if (dump)					\
	{					\
	  printf ("%s\n", dump);		\
	  MR_FREE (dump);			\
	}					\
    })

int main (int argc, char * argv[])
{
  DUMP_VAR (mr_td_t, &MR_DESCRIPTOR_PREFIX (segment_id_t));
  return (EXIT_SUCCESS);
}
