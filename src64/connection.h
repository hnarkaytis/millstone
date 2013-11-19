#ifndef _CONNECTION_H_
#define _CONNECTION_H_

#include <millstone.h> /* config_t */
#include <msg.h> /* msg_t */
#include <llist.h> /* llist_t */

#include <netinet/in.h> /* struct sockaddr_in */

#include <pthread.h> /* struct sockaddr_in */

TYPEDEF_STRUCT (connection_t,
		(config_t *, config),
		(int, cmd_fd),
		(struct sockaddr_in, local),
		(struct sockaddr_in, remote),
		(llist_t, cmd_out),
		(int, ref_count),
		(pthread_cond_t, cond),
		(pthread_mutex_t, mutex),
		)

#define DEFAULT_LISTEN_PORT (31415)

#define EXPECTED_PACKET_SIZE (9000 - 72) /* Jumbo frames with IPv6, TCP timestamps - 72 bytes */
#define CONNECTION_QUEUE_SIZE (EXPECTED_PACKET_SIZE / sizeof (msg_t))

extern void connection_init (connection_t * connection, config_t * config);
extern status_t connection_cmd_writer (connection_t * connection);
extern void connection_cancel (connection_t * connection);
extern status_t connection_msg_push (connection_t * connection, msg_t * msg);
extern void connection_finalize (connection_t * connection);

#endif /* _CONNECTION_H_ */
