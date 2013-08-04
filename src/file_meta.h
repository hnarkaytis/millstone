#ifndef _FILE_META_H_
#define _FILE_META_H_

#include <millstone.h> /* status_t, connection_t  */

extern status_t read_file_meta (connection_t * connection);
extern status_t send_file_meta (connection_t * connection);

#endif /* _FILE_META_H_ */
