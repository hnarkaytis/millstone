#ifndef _FILE_POOL_H_
#define _FILE_POOL_H_

#include <file.h> /* file_t */
#include <llist.h> /* llist_t */
#include <sync_storage.h> /* sync_storage_t */

#include <inttypes.h> /* uint8_t */

#include <pthread.h> /* pthread_mutex_t */

#define FILE_POOL_SIZE (4)

TYPEDEF_STRUCT (fd_t,
		(file_t, file),
		(int, fd),
		(int64_t, ref_count),
		(pthread_mutex_t, ref_count_mutex),
		(int64_t, sent_blocks),
		(pthread_mutex_t, sent_blocks_mutex),
		(uint8_t *, data),
		)

TYPEDEF_STRUCT (file_pool_t,
		(fd_t, fds, [FILE_POOL_SIZE]),
		(sync_storage_t, fd_map),
		(llist_t, pool),
		(file_id_t, file_id),
		)

extern void file_pool_init (file_pool_t * file_pool);
extern fd_t * server_open_file (file_pool_t * file_pool, file_t * file);
extern fd_t * client_open_file (file_pool_t * file_pool, char * file_name);
extern int64_t file_pool_ref_fd (file_pool_t * file_pool, fd_t * fd, int64_t delta);
extern void file_pool_fd_close (file_pool_t * file_pool, fd_t * fd);
extern void file_pool_cancel (file_pool_t * file_pool);
extern void file_pool_cleanup (file_pool_t * file_pool);
extern void file_sent_block (fd_t * fd);
extern fd_t * file_pool_get_fd (file_pool_t * file_pool, file_id_t * file_id);
extern void file_pool_finalize (file_pool_t * file_pool);

#endif /* _FILE_POOL_H_ */
