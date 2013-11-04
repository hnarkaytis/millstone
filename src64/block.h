#ifndef _BLOCK_H_
#define _BLOCK_H_

#ifndef _LARGEFILE64_SOURCE
#define _LARGEFILE64_SOURCE
#endif /* _LARGEFILE64_SOURCE */

#include <fcntl.h> /* off64_t */
#include <stdbool.h> /* bool */
#include <inttypes.h> /* uint32_t */

#include <file.h> /* file_id_t */

#include <openssl/sha.h> /* SHA_DIGEST_LENGTH */
#include <metaresc.h>

TYPEDEF_STRUCT (block_id_t,
		(off64_t, offset),
		(uint64_t, size),
		(file_id_t, file_id),
		)

TYPEDEF_STRUCT (block_digest_t,
		(block_id_t, block_id),
		(uint32_t, digest, [(SHA_DIGEST_LENGTH + sizeof (uint32_t) - 1) / sizeof (uint32_t)]),
		)

TYPEDEF_STRUCT (block_matched_t,
		(block_id_t, block_id),
		(bool, matched),
		)

#endif /* _BLOCK_H_ */
