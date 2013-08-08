#include <millstone.h>
#include <logging.h>
#include <block.h>
#include <msg.h>
#include <file_meta.h>
#include <calc_digest.h>
#include <client.h>

#include <unistd.h> /* TEMP_FAILURE_RETRY, sysconf, close, lseek64, SEEK_END */
#include <errno.h> /* errno */
#include <fcntl.h> /* open64 */
#include <string.h> /* memset, setlen, strerror */
#include <netdb.h> /* gethostbyname */
#include <netinet/in.h> /* htonl, struct sockaddr_in */
#include <sys/mman.h> /* mmap64, unmap */
#include <sys/uio.h> /* writev, struct iovec */
#include <sys/socket.h> /* socket, shutdown, connect */

#include <pthread.h>

TYPEDEF_STRUCT (client_t,
		(connection_t *, connection),
		(msg_queue_t, data_in),
		(msg_queue_t, cmd_in),
		(msg_queue_t, cmd_out),
		)

static status_t
send_block (client_t * client, block_id_t * block_id)
{
  DUMP_VAR (block_id_t, block_id);
  unsigned char * data = mmap64 (NULL, block_id->size, PROT_READ, MAP_PRIVATE,
				 client->connection->context->file_fd, block_id->offset);
  if (-1 == (long)data)
    {
      FATAL_MSG ("Failed to map file into memory. Error (%d) %s.\n", errno, strerror (errno));
      return (ST_FAILURE);
    }
  
  const struct iovec iov[] = {
    { .iov_len = sizeof (*block_id), .iov_base = block_id },
    { .iov_len = block_id->size, .iov_base = data },
  };

  ssize_t rv = TEMP_FAILURE_RETRY (writev (client->connection->data_fd, iov, sizeof (iov) / sizeof (iov[0])));
  ssize_t i, len = 0;
  for (i = 0; i < sizeof (iov) / sizeof (iov[0]); ++i)
    len += iov[i].iov_len;

  DEBUG_MSG ("Wrote packet of %zd bytes. Expected to write %zd.", rv, len);

  status_t status = ST_SUCCESS;
  if (rv != len)
    {
      ERROR_MSG ("Failed to send data block (sent %d bytes, but expexted %d bytes)", rv, len);
      status = ST_FAILURE;
    }

  if (0 != munmap (data, block_id->size))
    {
      ERROR_MSG ("Failed to unmap memory. Error (%d) %s.\n", errno, strerror (errno));
      status = EXIT_FAILURE;
    }
  
  return (status);
}

static void *
client_data_writer (void * arg)
{
  client_t * client = arg;
  for (;;)
    {
      msg_t msg;
      queue_pop (&client->data_in.queue, &msg);
      DUMP_VAR (msg_t, &msg);
      status_t status = send_block (client, &msg.msg_data.block_id);
      DEBUG_MSG ("Data block send status %d.", status);
      if (ST_SUCCESS != status)
	msg.msg_type = MT_BLOCK_SEND_ERROR;
      else
	msg.msg_type = MT_BLOCK_SENT;
      queue_push (&client->cmd_out.queue, &msg);
    }
  return (NULL);
}

static void *
client_cmd_writer (void * arg)
{
  client_t * client = arg;
  for (;;)
    {
      msg_t msg;
      queue_pop (&client->cmd_out.queue, &msg);
      DUMP_VAR (msg_t, &msg);
      status_t status = msg_send (client->connection->cmd_fd, &msg);
      if (ST_SUCCESS != status)
	break;
      DEBUG_MSG ("Message sent.");
    }
  shutdown (client->connection->cmd_fd, SD_BOTH);
  return (NULL);
}

static void *
digest_calculator (void * arg)
{
  client_t * client = arg;
  block_digest_t block_digest;
  status_t status;
  msg_t msg;

  DEBUG_MSG ("Entered digest calculator.");
  memset (&block_digest, 0, sizeof (block_digest));
  for (;;)
    {
      queue_pop (&client->cmd_in.queue, &msg);

      DUMP_VAR (msg_t, &msg);
      
      block_digest.block_id = msg.msg_data.block_digest.block_id;
      status = calc_digest (&block_digest, client->connection->context->file_fd);

      if (ST_SUCCESS != status)
	msg.msg_type = MT_BLOCK_SEND_ERROR;
      else
	{
	  msg.msg_type = MT_BLOCK_MATCHED;
	  msg.msg_data.block_matched.matched = !memcmp (block_digest.digest, msg.msg_data.block_digest.digest, sizeof (block_digest.digest));
	}
      DEBUG_MSG ("Push message:");
      DUMP_VAR (msg_t, &msg);
      queue_push (&client->cmd_out.queue, &msg);
      DEBUG_MSG ("Message pushed.");
    }
  return (NULL);
}

static status_t
client_cmd_reader (client_t * client)
{
  status_t status;

  for (;;)
    {
      msg_t msg;
      DEBUG_MSG ("Read from fd %d.", client->connection->cmd_fd);
      status = msg_recv (client->connection->cmd_fd, &msg);
      DEBUG_MSG ("Got message with status %d.", status);
      if (ST_SUCCESS != status)
	break;
      DUMP_VAR (msg_t, &msg);
      if (MT_TERMINATE == msg.msg_type)
	{
	  DEBUG_MSG ("Got termination command.");
	  break;
	}
      
      switch (msg.msg_type)
	{
	case MT_BLOCK_DIGEST:
	  queue_push (&client->cmd_in.queue, &msg);
	  break;
	case MT_BLOCK_REQUEST:
	  queue_push (&client->data_in.queue, &msg);
	  break;
	default:
	  ERROR_MSG ("Unexpected message type %d.", msg.msg_type);
	  status = ST_FAILURE;
	  break;
	}
      if (ST_SUCCESS != status)
	break;
    }
  return (status);
}

static status_t
start_data_writer (client_t * client)
{
  pthread_t id;
  int rv = pthread_create (&id, NULL, client_data_writer, client);
  if (rv != 0)
    {
      ERROR_MSG ("Failed to start command writer thread");
      return (ST_FAILURE);
    }

  DEBUG_MSG ("Started data writer %d.", rv);
  status_t status = client_cmd_reader (client);
  
  DEBUG_MSG ("Canceling data writer.");
  pthread_cancel (id);
  pthread_join (id, NULL);
  DEBUG_MSG ("Command data canceled.");

  return (status);
}

static status_t
start_cmd_writer (client_t * client)
{
  pthread_t id;
  int rv = pthread_create (&id, NULL, client_cmd_writer, client);
  if (rv != 0)
    {
      ERROR_MSG ("Failed to start command writer thread");
      return (ST_FAILURE);
    }
  DEBUG_MSG ("Client command writer started %d.", rv);

  status_t status = start_data_writer (client);
  
  DEBUG_MSG ("Canceling command writer.");
  pthread_cancel (id);
  pthread_join (id, NULL);
  DEBUG_MSG ("Command writer canceled.");
  
  return (status);
}

static status_t
start_digest_calculators (client_t * client)
{
  int i, ncpu = (long) sysconf (_SC_NPROCESSORS_ONLN);
  pthread_t ids[ncpu];
  status_t status = ST_FAILURE;

  DEBUG_MSG ("Starting digest calculators %d.", ncpu);
  for (i = 0; i < ncpu; ++i)
    {
      int rv = pthread_create (&ids[i], NULL, digest_calculator, client);
      if (rv != 0)
	break;
    }

  DEBUG_MSG ("Started %d.", i);
  if (i > 0)
    status = start_cmd_writer (client);

  DEBUG_MSG ("Canceling digest calculators.");
  for (--i ; i >= 0; --i)
    {
      pthread_cancel (ids[i]);
      pthread_join (ids[i], NULL);
    }
  DEBUG_MSG ("Digest calculators canceled.");
  
  return (status);
}

static status_t
run_session (connection_t * connection)
{
  client_t client;
  msg_t cmd_out_array_data[MSG_OUT_QUEUE_SIZE];
  msg_t cmd_in_array_data[MSG_IN_QUEUE_SIZE];
  msg_t data_in_array_data[MSG_IN_QUEUE_SIZE];

  memset (&client, 0, sizeof (client));
  client.connection = connection;
  
  DEBUG_MSG ("Sending file meata data.");
  status_t status = send_file_meta (connection);
  if (ST_SUCCESS != status)
    return (status);
  
  MSG_QUEUE_INIT (&client.cmd_out, cmd_out_array_data);
  MSG_QUEUE_INIT (&client.cmd_in, cmd_in_array_data);
  MSG_QUEUE_INIT (&client.data_in, data_in_array_data);

  DEBUG_MSG ("Session inited.");
  status = start_digest_calculators (&client);
  DEBUG_MSG ("Session done.");

  return (status);
}

static status_t
configure_data_connection (connection_t * connection)
{
  int rv = TEMP_FAILURE_RETRY (connect (connection->data_fd, (struct sockaddr *)&connection->remote, sizeof (connection->remote)));
  if (rv < 0)
    {
      ERROR_MSG ("Connect failed errno(%d) '%s'.", errno, strerror (errno));
      return (ST_FAILURE);
    }
  
  socklen_t socklen = sizeof (connection->local); 
  getsockname (connection->data_fd, (struct sockaddr *)&connection->local, &socklen); 

  DEBUG_MSG ("Connected data socket. Local port is %04x.", connection->local.sin_port);
  status_t status = run_session (connection);
  shutdown (connection->data_fd, SD_BOTH);
  DEBUG_MSG ("Closed data socket.");

  return (status);
}

static status_t
create_data_socket (connection_t * connection)
{
  connection->data_fd = socket (PF_INET, SOCK_DGRAM, IPPROTO_UDP);
  if (connection->data_fd <= 0)
    {
      ERROR_MSG ("Data socket failed errno(%d) '%s'.", errno, strerror (errno));
      return (ST_FAILURE);
    }

  DEBUG_MSG ("Created data socket.");
  status_t status = configure_data_connection (connection);
  close (connection->data_fd);
  DEBUG_MSG ("Closed data socket.");
  
  return (status);
}

static status_t
connect_to_server (connection_t * connection)
{
  struct hostent * hostinfo = gethostbyname (connection->context->config->dst_host);
  if (hostinfo != NULL)
    connection->remote.sin_addr.s_addr = *((in_addr_t*) hostinfo->h_addr);
  else
    connection->remote.sin_addr.s_addr = htonl (INADDR_ANY);

  connection->remote.sin_family = AF_INET;
  connection->remote.sin_port = htons (connection->context->config->dst_port);

  DEBUG_MSG ("Connect to %08x:%04x.", connection->remote.sin_addr.s_addr, connection->remote.sin_port);

  int rv = TEMP_FAILURE_RETRY (connect (connection->cmd_fd, (struct sockaddr *)&connection->remote, sizeof (connection->remote)));
  if (rv < 0)
    {
      ERROR_MSG ("Connect failed errno(%d) '%s'.", errno, strerror (errno));
      return (ST_FAILURE);
    }

  DEBUG_MSG ("Connected to server successfully.");
  
  status_t status = create_data_socket (connection);
  if (ST_SUCCESS == status)
    shutdown (connection->cmd_fd, SD_BOTH);

  DEBUG_MSG ("Shutdown command socket.");

  return (status);
}

static status_t
create_server_socket (context_t * context)
{
  connection_t connection;

  memset (&connection, 0, sizeof (connection));
  connection.context = context;

  connection.cmd_fd = socket (PF_INET, SOCK_STREAM, IPPROTO_TCP);
  if (connection.cmd_fd < 0)
    {
      ERROR_MSG ("Command socket failed errno(%d) '%s'.", errno, strerror (errno));
      return (ST_FAILURE);
    }

  DEBUG_MSG ("Created command socket.");
  status_t status = connect_to_server (&connection);
  close (connection.cmd_fd);

  DEBUG_MSG ("Close command socket.");
  return (status);
}

status_t
run_client (config_t * config)
{
  status_t status = ST_FAILURE;
  context_t context;

  memset (&context, 0, sizeof (context));
  context.config = config;
  context.file_fd = open64 (config->src_file, O_RDONLY);
  if (context.file_fd <= 0)
    {
      ERROR_MSG ("Can't open source file '%s'.", config->src_file);
      return (ST_FAILURE);
    }

  DUMP_VAR (context_t, &context);
  
  context.size = lseek64 (context.file_fd, 0, SEEK_END);
  status = create_server_socket (&context);
  close (context.file_fd);

  DEBUG_MSG ("Exiting client.");
  return (status);
}
