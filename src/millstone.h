#ifndef _MILLSTONE_H_
#define _MILLSTONE_H_

#define _LARGEFILE64_SOURCE
#include <stdlib.h>
#include <stddef.h>
#include <stdbool.h>
#include <inttypes.h>

#include <metaresc.h>
#include <openssl/sha.h>

TYPEDEF_ENUM (status_t, ST_SUCCESS, ST_FAILURE)

TYPEDEF_ENUM (run_mode_t, RM_SERVER, RM_CLIENT)

TYPEDEF_STRUCT (config_t,
		(run_mode_t, run_mode),
		(char *, src),
		(char *, dst),
		)

TYPEDEF_STRUCT (block_id_t,
		(size_t, size),
		(off64_t, offset),
		)

TYPEDEF_STRUCT (block_digest_t,
		(block_id_t, block_id),
		(uint32_t, digest, [(SHA_DIGEST_LENGTH + sizeof (uint32_t) - 1) / sizeof (uint32_t)]),
		)

TYPEDEF_STRUCT (block_matched_t,
		(block_id_t, block_id),
		(bool, matched),
		)

TYPEDEF_STRUCT (split_task_t,
		(block_id_t, block_id),
		(size_t, size),
		)

TYPEDEF_UNION (msg_data_t,
	       (block_id_t, block_id),
	       (block_matched_t, block_matched),
	       (block_digest_t, block_digest),
	       )

TYPEDEF_ENUM (msg_type_t,
	      (MT_BLOCK_REQUEST, , "block_id"),
	      (MT_BLOCK_SENT, , "block_id"),
	      (MT_BLOCK_MATCHED, , "block_matched"),
	      (MT_BLOCK_DIGEST, , "block_digest"),
	      )

TYPEDEF_STRUCT (msg_t,
		(msg_type_t, msg_type),
		(msg_data_t, msg_data, , "msg_type"),
		)

#define DUMP_VAR(TYPE, VAR) ({			\
      char * dump = MR_SAVE_CINIT (TYPE, VAR);	\
      if (dump)					\
	{					\
	  printf ("%s\n", dump);		\
	  MR_FREE (dump);			\
	}					\
    })

#endif /* _MILLSTONE_H_ */
