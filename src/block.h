#ifndef _BLOCK_H_
#define _BLOCK_H_

#ifndef _LARGEFILE64_SOURCE
#define _LARGEFILE64_SOURCE
#endif /* _LARGEFILE64_SOURCE */

#include <fcntl.h> /* off64_t */
#include <stddef.h> /* size_t */
#include <stdbool.h> /* bool */
#include <inttypes.h> /* uint32_t */

#include <openssl/sha.h> /* SHA_DIGEST_LENGTH */
#include <metaresc.h>

TYPEDEF_STRUCT (block_id_t, ATTRIBUTES (__attribute__ ((packed))),
		(off64_t, offset),
		(uint32_t, size),
		)

TYPEDEF_STRUCT (block_digest_t, ATTRIBUTES (__attribute__ ((packed))),
		(block_id_t, block_id),
		(uint32_t, digest, [(SHA_DIGEST_LENGTH + sizeof (uint32_t) - 1) / sizeof (uint32_t)]),
		)

TYPEDEF_STRUCT (block_matched_t, ATTRIBUTES (__attribute__ ((packed))),
		(block_id_t, block_id),
		(block_id_t, duplicate_block_id),
		BITFIELD (bool, matched, :1),
		BITFIELD (bool, duplicate, :1),
		)

#endif /* _BLOCK_H_ */
