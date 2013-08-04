#ifndef _MSG_H_
#define _MSG_H_

#include <metaresc.h>

#include <millstone.h> /* status_t */
#include <block.h> /* block_*_t */
#include <queue.h> /* queue_t */

#define MSG_OUT_QUEUE_SIZE (2)
#define MSG_IN_QUEUE_SIZE (16)

#define MSG_QUEUE_INIT(MSG_QUEUE, ARRAY) msg_queue_init (MSG_QUEUE, ARRAY, sizeof (ARRAY))

TYPEDEF_UNION (msg_data_t,
	       (block_id_t, block_id),
	       (block_matched_t, block_matched),
	       (block_digest_t, block_digest),
	       )

TYPEDEF_ENUM (msg_type_t,
	      MT_TERMINATE,
	      (MT_BLOCK_REQUEST, , "block_id"),
	      (MT_BLOCK_SENT, , "block_id"),
	      (MT_BLOCK_SEND_ERROR, , "block_id"),
	      (MT_BLOCK_MATCHED, , "block_matched"),
	      (MT_BLOCK_DIGEST, , "block_digest"),
	      )

TYPEDEF_STRUCT (msg_t,
		(msg_type_t, msg_type),
		(msg_data_t, msg_data, , "msg_type"),
		)

TYPEDEF_STRUCT (msg_queue_t,
		(queue_t, queue),
		RARRAY (msg_t, array),
		)

extern status_t msg_queue_init (msg_queue_t * msg_queue, msg_t * array, size_t size);
extern status_t msg_send (int fd, msg_t * msg);
extern status_t msg_recv (int fd, msg_t * msg);

#endif /* _MSG_H_ */
