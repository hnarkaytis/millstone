#ifndef _CLOSE_CONNECTION_H_
#define _CLOSE_CONNECTION_H_

#include <sys/socket.h> /* shutdown */
#include <unistd.h> /* close */

#ifndef SD_BOTH
#define SD_BOTH (2)
#endif /* SD_BOTH */

static inline void
close_connection (int fd)
{
  shutdown (fd, SD_BOTH);
  close (fd);
}

#endif /* _CLOSE_CONNECTION_H_ */
