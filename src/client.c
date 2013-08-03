#define _LARGEFILE64_SOURCE
#define _GNU_SOURCE
#include <unistd.h>
#include <fcntl.h>
#include <netdb.h>
#include <errno.h>

#include <pthread.h>

#include <client.h>
#include <queue.h>
#include <logging.h>

TYPEDEF_STRUCT (client_t,
		(connection_t *, connection),
		(queue_t, data_in),
		(queue_t, cmd_in),
		(queue_t, cmd_out),
		)

#ifndef SD_BOTH
#define SD_BOTH (2)
#endif /* SD_BOTH */

static status_t
send_file_meta (connection_t * connection)
{
  const struct iovec iov[] = {
    { .iov_len = sizeof (connection->context->size), .iov_base = &connection->context->size },
    { .iov_len = strlen (connection->context->config->dst_file) + 1, .iov_base = connection->context->config->dst_file },
  };

  int rv = TEMP_FAILURE_RETRY (writev (connection->cmd_fd, iov, sizeof (iov) / sizeof (iov[0])));
  int i, len = 0;
  for (i = 0; i < sizeof (iov) / sizeof (iov[0]); ++i)
    len = iov[i].iov_len;

  status_t status = ST_SUCCESS;
  if (rv != len)
    {
      ERROR_MSG ("Failed to send hand shake message (sent %d bytes, but expexted %d bytes)", rv, len);
      status = ST_FAILURE;
    }
  return (status);
}

static status_t
msg_recv (int fd, msg_t * msg)
{
  int len = sizeof (*msg);
  int rv = TEMP_FAILURE_RETRY (read (fd, msg, len));
  status_t status = ST_SUCCESS;
  if (rv != len)
    {
      ERROR_MSG ("Failed to reveive message (got %d bytes, but expexted %d bytes)", rv, len);
      status = ST_FAILURE;
    }
  return (status);
}

static status_t
msg_send (int fd, msg_t * msg)
{
  int len = sizeof (*msg);
  int rv = TEMP_FAILURE_RETRY (write (fd, msg, len));
  status_t status = ST_SUCCESS;
  if (rv != len)
    {
      ERROR_MSG ("Failed to send message (got %d bytes, but expexted %d bytes)", rv, len);
      status = ST_FAILURE;
    }
  return (status);
}

static status_t
reader (client_t * client)
{
  status_t status;
  for (;;)
    {
      msg_t msg;
      status = msg_recv (client->connection->cmd_fd, &msg);
      if (ST_SUCCESS != status)
	break;
      if (MT_TERMINATE == msg.msg_type)
	break;
      switch (msg.msg_type)
	{
	case MT_BLOCK_DIGEST:
	  break;
	default:
	  status = ST_FAILURE;
	  break;
	}
      if (ST_SUCCESS != status)
	break;
    }
  return (status);
}

static void *
data_writer (void * arg)
{
  client_t * client = arg;
  client->connection->cmd_fd = 0;
  return (NULL);
}

static void *
cmd_writer (void * arg)
{
  client_t * client = arg;
  for (;;)
    {
      msg_t msg;
      //queue_pop (&client->cmd_out, &msg);
      memset (&msg, 0, sizeof (msg));
      status_t status = msg_send (client->connection->cmd_fd, &msg);
      if (ST_SUCCESS != status)
	break;
    }
  shutdown (client->connection->cmd_fd, SD_BOTH);
  close (client->connection->cmd_fd);
  return (NULL);
}

static status_t
start_session (connection_t * connection)
{
  int rv;
  pthread_t cmd_writer_id, data_writer_id;
  client_t client = { .connection = connection };
  status_t status = send_file_meta (connection);
  
  if (ST_SUCCESS != status)
    return (ST_FAILURE);

  rv = pthread_create (&cmd_writer_id, NULL, cmd_writer, &client);
  if (rv != 0)
    {
      ERROR_MSG ("Failed to start command writer thread");
      return (ST_FAILURE);
    }
  
  rv = pthread_create (&data_writer_id, NULL, data_writer, &client);
  if (rv != 0)
    {
      ERROR_MSG ("Failed to start command writer thread");
      return (ST_FAILURE);
    }
  
  status = reader (&client);
  
  pthread_cancel (cmd_writer_id);
  pthread_join (cmd_writer_id, NULL);
  pthread_cancel (data_writer_id);
  pthread_join (data_writer_id, NULL);
  
  return (status);
}

static status_t
open_data_connection (connection_t * connection, struct sockaddr_in * name)
{
  status_t status;
  connection->data_fd = socket (PF_INET, SOCK_DGRAM, 0);
  if (connection->data_fd <= 0)
    {
      ERROR_MSG ("Data socket failed errno(%d) '%s'.", errno, strerror (errno));
      return (ST_FAILURE);
    }

  pthread_cleanup_push ((void (*) (void*))close, (void*)(long)connection->data_fd);

  int rv = TEMP_FAILURE_RETRY (connect (connection->data_fd, (struct sockaddr *)name, sizeof (*name)));
  if (-1 != rv)
    status = start_session (connection);
  else
    {
      ERROR_MSG ("Connect failed errno(%d) '%s'.", errno, strerror (errno));
      return (ST_FAILURE);
    }
  
  pthread_cleanup_pop (!0);

  return (status);
}

static status_t
connect_to_server (context_t * context)
{
  status_t status;
  struct sockaddr_in name;
  connection_t connection = {
    .context = context,
  };

  connection.cmd_fd = socket (PF_INET, SOCK_STREAM, 0);
  if (connection.cmd_fd <= 0)
    {
      ERROR_MSG ("Command socket failed errno(%d) '%s'.", errno, strerror (errno));
      return (ST_FAILURE);
    }

  pthread_cleanup_push ((void (*) (void*))close, (void*)(long)connection.cmd_fd);

  struct hostent * hostinfo = gethostbyname (context->config->dst_host);
  if (hostinfo != NULL)
    name.sin_addr.s_addr = *((in_addr_t*) hostinfo->h_addr);
  else
    name.sin_addr.s_addr = htonl (INADDR_ANY);

  name.sin_family = AF_INET;
  name.sin_port = htons (context->config->dst_port);

  int rv = TEMP_FAILURE_RETRY (connect (connection.cmd_fd, (struct sockaddr *)&name, sizeof (name)));
  if (-1 != rv)
    status = open_data_connection (&connection, &name);
  else
    {
      ERROR_MSG ("Connect failed errno(%d) '%s'.", errno, strerror (errno));
      status = ST_FAILURE;
    }
  
  pthread_cleanup_pop (!0);
  
  return (status);
}

status_t
start_client (config_t * config)
{
  status_t status = ST_FAILURE;
  context_t context = { .config = config };
  
  context.file_fd = open64 (config->src_file, O_RDONLY);
  if (context.file_fd <= 0)
    {
      ERROR_MSG ("Can't open source file '%s'", config->src_file);
      return (ST_FAILURE);
    }

  context.size = lseek64 (context.file_fd, 0, SEEK_END);
  
  pthread_cleanup_push ((void (*) (void*))close, (void*)(long)context.file_fd);
  status = connect_to_server (&context);
  pthread_cleanup_pop (!0);

  return (status);
}

