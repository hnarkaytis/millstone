#ifndef _FILE_H_
#define _FILE_H_

#ifndef _LARGEFILE64_SOURCE
#define _LARGEFILE64_SOURCE
#endif /* _LARGEFILE64_SOURCE */

#include <fcntl.h> /* off64_t */
#include <inttypes.h> /* uint32_t */

#include <metaresc.h>

TYPEDEF_STRUCT (file_id_t, uint32_t id)

TYPEDEF_STRUCT (file_t,
		(file_id_t, file_id),
		(off64_t, size),
		(char *, file_name),
		)

TYPEDEF_STRUCT (close_file_info_t,
		(file_id_t, file_id),
		(int64_t, sent_blocks),
		)

#endif /* _FILE_H_ */
