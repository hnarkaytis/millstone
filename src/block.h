#ifndef _BLOCK_H_
#define _BLOCK_H_

#define _LARGEFILE64_SOURCE
#include <stdlib.h>

#include <metaresc.h>
#include <openssl/sha.h>

TYPEDEF_STRUCT (block_id_t,
		(off64_t, offset),
		(size_t, size),
		)

TYPEDEF_STRUCT (block_digest_t,
		(block_id_t, block_id),
		(uint32_t, digest, [SHA_DIGEST_LENGTH / sizeof (uint32_t)]),
		)

#define DUMP_VAR(TYPE, VAR) ({			\
      char * dump = MR_SAVE_CINIT (TYPE, VAR);	\
      if (dump)					\
	{					\
	  printf ("%s\n", dump);		\
	  MR_FREE (dump);			\
	}					\
    })

#endif /* _BLOCK_H_ */
