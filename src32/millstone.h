#ifndef _MILLSTONE_H_
#define _MILLSTONE_H_

#ifndef _LARGEFILE64_SOURCE
#define _LARGEFILE64_SOURCE
#endif /* _LARGEFILE64_SOURCE */

#include <metaresc.h>

#include <logging.h>

TYPEDEF_ENUM (status_t, ST_SUCCESS, ST_FAILURE)

TYPEDEF_ENUM (run_mode_t, RM_SERVER, RM_CLIENT)

TYPEDEF_STRUCT (config_t,
		(run_mode_t, run_mode),
		(char **, src_files),
		string_t dst_file,
		string_t dst_host,
		int dst_port,
		int listen_port,
		int mem_threshold,
		int32_t compress_level,
		int workers_number,
		int data_connections,
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
#define DUMP_VAR(...) DUMP_VAR_ (DEBUG_MSG, __VA_ARGS__)
#else
#define DUMP_VAR _OFF_MSG
#endif

extern status_t start_threads (void * (* handler) (void *), int count, status_t (*nested_handler) (void *), void * arg);

#endif /* _MILLSTONE_H_ */
