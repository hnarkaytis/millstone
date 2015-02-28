#ifndef _MSG_H_
#define _MSG_H_

#ifdef HAVE_CONFIG_H
#include <config.h>
#endif /* HAVE_CONFIG_H */
#include <millstone.h> /* status_t */
#include <file.h> /* file_id_t, file_t */
#include <block.h> /* block_digest_t, block_matched_t */
#include <client.h> /* client_id_t */

#include <stddef.h> /* size_t, ssize_t */
#include <sys/uio.h> /* struct iovec */

#include <metaresc.h>

TYPEDEF_ENUM (msg_type_t,
	      (MT_CMD_CONNECTION_START),
	      (MT_CMD_CONNECTION_STARTED),
	      (MT_CMD_CONNECTION_END),
	      (MT_DATA_CONNECTION_START, , "client_id"),
	      (MT_BLOCK_DIGEST, , "block_digest"),
	      (MT_BLOCK_MATCHED, , "block_matched"),
	      (MT_OPEN_FILE, , "file"),
	      (MT_CLOSE_FILE, , "close_file_info"),
	      (MT_OPEN_FILE_SUCCESS, , "file_id"),
	      (MT_OPEN_FILE_NEW, , "file_id"),
	      (MT_OPEN_FILE_FAILURE, , "file_id"),
	      )

TYPEDEF_STRUCT (msg_t, 
		(msg_type_t, msg_type),
		ANON_UNION (msg_data),
		VOID (char, empty),
		(file_t, file),
		(client_id_t, client_id),
		(block_digest_t, block_digest),
		(block_matched_t, block_matched),
		(file_id_t, file_id),
		(close_file_info_t, close_file_info),
		END_ANON_UNION ("msg_type"),
		)

extern status_t msg_recv (int fd, msg_t * msg);
extern status_t msg_send (int fd, msg_t * msg);
extern status_t buf_recv (int fd, void * buf, size_t size);
extern status_t buf_send (int fd, struct iovec * iov, size_t count);

extern uint64_t stat_bytes_sent (uint64_t bytes);
extern uint64_t stat_bytes_recv (uint64_t bytes);

#endif /* _MSG_H_ */
