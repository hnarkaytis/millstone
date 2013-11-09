#include <connection.h> /* connection_t */
#include <llist.h> /* llist_t */
#include <msg.h> /* msg_t */
#include <millstone.h> /* status_t */

#include <string.h> /* memset, strerror */
#include <sys/socket.h> /* shutdown */

#include <pthread.h>

#include <metaresc.h>

status_t
connection_cmd_writer (connection_t * connection)
{
  status_t status = ST_SUCCESS;
  msg_t msg;
  
  memset (&msg, 0, sizeof (msg));
  
  for (;;)
    {
      status = llist_pop (&connection->cmd_out, &msg);
      if (ST_SUCCESS != status)
	break;
      status = msg_send (connection->cmd_fd, &msg);
      MR_FREE_RECURSIVELY (msg_t, &msg);
      
      pthread_mutex_lock (&connection->mutex);
      if (0 == --connection->ref_count)
	pthread_cond_broadcast (&connection->cond);
      pthread_mutex_unlock (&connection->mutex);
      
      if (ST_SUCCESS != status)
	break;
    }

  return (status);
}

void
connection_init (connection_t * connection, config_t * config)
{
  memset (connection, 0, sizeof (*connection));
  connection->config = config;
  LLIST_INIT (&connection->cmd_out, msg_t, CONNECTION_QUEUE_SIZE);
  connection->ref_count = 0;
  pthread_mutex_init (&connection->mutex, NULL);
  pthread_cond_init (&connection->cond, NULL);
}

status_t
connection_msg_push (connection_t * connection, msg_t * msg)
{
  pthread_mutex_lock (&connection->mutex);
  ++connection->ref_count;
  pthread_mutex_unlock (&connection->mutex);
  return (llist_push (&connection->cmd_out, msg));
}

void
connection_cancel (connection_t * connection)
{
  llist_cancel (&connection->cmd_out);
  shutdown (connection->cmd_fd, SD_BOTH);
  
  pthread_mutex_lock (&connection->mutex);
  connection->ref_count = 0;
  pthread_cond_broadcast (&connection->cond);
  pthread_mutex_unlock (&connection->mutex);
}

void
connection_finalize (connection_t * connection)
{
  pthread_mutex_lock (&connection->mutex);
  while (connection->ref_count != 0)
    pthread_cond_wait (&connection->cond, &connection->mutex);
  pthread_mutex_unlock (&connection->mutex);
}
