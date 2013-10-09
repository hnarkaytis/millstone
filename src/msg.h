#ifndef _MSG_H_
#define _MSG_H_

#ifdef HAVE_CONFIG_H
#include <config.h>
#endif /* HAVE_CONFIG_H */
#include <millstone.h> /* status_t */
#include <block.h> /* block_*_t */

#include <metaresc.h>

TYPEDEF_ENUM (msg_type_t, ATTRIBUTES (__attribute__ ((packed))),
	      (MT_BLOCK_REQUEST, , "block_id"),
	      (MT_BLOCK_SENT, , "block_id"),
	      (MT_BLOCK_SEND_ERROR, , "block_id"),
	      (MT_BLOCK_REF, , "block_id"),
	      (MT_BLOCK_UNREF, , "block_id"),
	      (MT_BLOCK_MATCHED, , "block_matched"),
	      (MT_BLOCK_DIGEST, , "block_digest"),
	      )

TYPEDEF_STRUCT (msg_t, ATTRIBUTES (__attribute__ ((packed))),
		ANON_UNION (msg_data, __attribute__ ((packed))),
		NONE (char, empty),
		(block_id_t, block_id),
		(block_matched_t, block_matched),
		(block_digest_t, block_digest),
		END_ANON_UNION ("msg_type"),
		(msg_type_t, msg_type),
		)

extern status_t msg_recv (int fd, msg_t * msg);
extern status_t msg_send (int fd, char * msg_buf, size_t size);

#endif /* _MSG_H_ */
