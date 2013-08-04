#ifndef _MILLSTONE_H_
#define _MILLSTONE_H_

#ifndef _LARGEFILE64_SOURCE
#define _LARGEFILE64_SOURCE
#endif /* _LARGEFILE64_SOURCE */

#include <stddef.h> /* size_t */
#include <stdbool.h> /* bool */
#include <netinet/in.h> /* struct sockaddr_in */

#include <metaresc.h>

#define DEFAULT_LISTEN_PORT (31415)

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
		)

TYPEDEF_STRUCT (context_t,
		(config_t *, config),
		(bool, file_exists),
		int file_fd,
		(size_t, size),
		)

TYPEDEF_STRUCT (connection_t,
		(context_t *, context),
		(struct sockaddr_in, name),
		int cmd_fd,
		int data_fd,
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
