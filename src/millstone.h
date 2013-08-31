#ifndef _MILLSTONE_H_
#define _MILLSTONE_H_

#ifndef _LARGEFILE64_SOURCE
#define _LARGEFILE64_SOURCE
#endif /* _LARGEFILE64_SOURCE */

#define _GNU_SOURCE /* TEMP_FAILURE_RETRY */
#include <fcntl.h> /* off64_t */
#include <stddef.h> /* size_t */
#include <stdbool.h> /* bool */
#include <netinet/in.h> /* struct sockaddr_in */
#include <sys/user.h> /* PAGE_SIZE */

#include <metaresc.h>

#include <logging.h>

#define DEFAULT_LISTEN_PORT (31415)

#define SPLIT_RATIO (1 << 7)
#define TRANSFER_BLOCK_SIZE (1 << 10)
#define MIN_BLOCK_SIZE (PAGE_SIZE)
#define MAX_BLOCK_SIZE (MIN_BLOCK_SIZE * SPLIT_RATIO * SPLIT_RATIO)

#ifndef SD_BOTH
#define SD_BOTH (2)
#endif /* SD_BOTH */

TYPEDEF_ENUM (status_t, ST_SUCCESS, ST_FAILURE)

TYPEDEF_ENUM (run_mode_t, RM_SERVER, RM_CLIENT)

TYPEDEF_STRUCT (config_t,
		(run_mode_t, run_mode),
		string_t src_file,
		string_t dst_file,
		string_t dst_host,
		int dst_port,
		int listen_port,
		int mem_threshold,
		int32_t compress_level,
		)

TYPEDEF_STRUCT (mapped_region_t,
		(off64_t, offset),
		(size_t, size),
		int protect,
		int flags,
		NONE (uint8_t *, data),
		)		

TYPEDEF_STRUCT (context_t,
		(config_t *, config),
		(bool, file_exists),
		int file_fd,
		(size_t, size),
		(mapped_region_t, mapped_region),
		)

TYPEDEF_STRUCT (connection_t,
		(context_t *, context),
		(struct sockaddr_in, local),
		(struct sockaddr_in, remote),
		int cmd_fd,
		int data_fd,
		)

#define DUMP_VAR_(OUTPUT_MSG, TYPE, VAR) ({			\
      char * dump = MR_SAVE_CINIT (TYPE, VAR);			\
      if (dump)							\
	{							\
	  OUTPUT_MSG ("\n(" #TYPE ")*" #VAR " = %s", dump);	\
	  MR_FREE (dump);					\
	}							\
    })

#if defined COMPILE_LOG_LEVEL_LL_ALL || defined COMPILE_LOG_LEVEL_LL_TRACE || defined COMPILE_LOG_LEVEL_LL_DEBUG
#defined DUMP_VAR(...) DUMP_VAR_ (DEBUG_MSG, __VA_ARGS__)
#else
#define DUMP_VAR _OFF_MSG
#endif

#endif /* _MILLSTONE_H_ */
