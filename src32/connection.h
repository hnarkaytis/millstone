#ifndef _CONNECTION_H_
#define _CONNECTION_H_

#include <file.h> /* file_t */

#include <netinet/in.h> /* struct sockaddr_in */

TYPEDEF_STRUCT (connection_t,
		(file_t *, file),
		(struct sockaddr_in, local),
		(struct sockaddr_in, remote),
		int cmd_fd,
		int data_fd,
		)

#define DEFAULT_LISTEN_PORT (31415)

#define EXPECTED_PACKET_SIZE (1500 - 72) /* IPv6, TCP timestamps - 72 bytes */

#endif /* _CONNECTION_H_ */
