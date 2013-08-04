#include <millstone.h>
#include <block.h>
#include <msg.h>
#include <logging.h>
#include <file_meta.h>
#include <client.h>

#define _GNU_SOURCE /* TEMP_FAILURE_RETRY */
#include <unistd.h> /* TEMP_FAILURE_RETRY, sysconf, close, lseek64, SEEK_END */
#include <errno.h> /* errno, strerror */
#include <fcntl.h> /* open64 */
#include <string.h> /* memset, setlen */
#include <netdb.h> /* gethostbyname */
#include <netinet/in.h> /* htonl, struct sockaddr_in */
#include <sys/uio.h> /* writev, struct iovec */
#include <sys/mman.h> /* mmap64, unmap */
#include <sys/socket.h> /* socket, shutdown, connect */

#include <openssl/sha.h> /* SHA1 */
#include <pthread.h>

TYPEDEF_STRUCT (client_t,
		(connection_t *, connection),
		(msg_queue_t, data_in),
		(msg_queue_t, cmd_in),
		(msg_queue_t, cmd_out),
		)

#ifndef SD_BOTH
#define SD_BOTH (2)
#endif /* SD_BOTH */

static void
close_connection (int fd)
{
  shutdown (fd, SD_BOTH);
  close (fd);
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
	  queue_push (&client->cmd_in.queue, &msg);
	  break;
	case MT_BLOCK_REQUEST:
	  queue_push (&client->data_in.queue, &msg);
	  break;
	default:
	  status = ST_FAILURE;
	  break;
	}
      if (ST_SUCCESS != status)
	break;
    }
  close_connection (client->connection->cmd_fd);
  return (status);
}

static status_t
send_block (client_t * client, block_id_t * block_id)
{
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
    len = iov[i].iov_len;

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
data_writer (void * arg)
{
  client_t * client = arg;
  for (;;)
    {
      msg_t msg;
      queue_pop (&client->data_in.queue, &msg);
      status_t status = send_block (client, &msg.msg_data.block_id);
      if (ST_SUCCESS != status)
	msg.msg_type = MT_BLOCK_SEND_ERROR;
      else
	msg.msg_type = MT_BLOCK_SENT;
      queue_push (&client->cmd_out.queue, &msg);
    }
  return (NULL);
}

static void *
cmd_writer (void * arg)
{
  client_t * client = arg;
  for (;;)
    {
      msg_t msg;
      queue_pop (&client->cmd_out.queue, &msg);
      status_t status = msg_send (client->connection->cmd_fd, &msg);
      if (ST_SUCCESS != status)
	break;
    }
  close_connection (client->connection->cmd_fd);
  return (NULL);
}

static status_t
calc_digest (block_digest_t * block_digest, int fd)
{
  status_t status = ST_FAILURE;
  unsigned char * data = mmap64 (NULL, block_digest->block_id.size, PROT_READ, MAP_PRIVATE,
				 fd, block_digest->block_id.offset);
  if (-1 == (long)data)
    FATAL_MSG ("Failed to map file into memory. Error (%d) %s.\n", errno, strerror (errno));
  else
    {
      SHA1 (data, block_digest->block_id.size, (unsigned char*)block_digest->digest);
      
      if (0 != munmap (data, block_digest->block_id.size))
	ERROR_MSG ("Failed to unmap memory. Error (%d) %s.\n", errno, strerror (errno));
      else
	status = ST_SUCCESS;
    }
  return (status);
}

static void *
digest_calculator (void * arg)
{
  client_t * client = arg;
  block_digest_t block_digest;
  status_t status;
  msg_t msg;

  memset (&block_digest, 0, sizeof (block_digest));
  for (;;)
    {
      queue_pop (&client->cmd_in.queue, &msg);

      block_digest.block_id = msg.msg_data.block_digest.block_id;
      status = calc_digest (&block_digest, client->connection->context->file_fd);

      if (ST_SUCCESS != status)
	msg.msg_type = MT_BLOCK_SEND_ERROR;
      else
	{
	  msg.msg_type = MT_BLOCK_MATCHED;
	  msg.msg_data.block_matched.matched = !memcmp (block_digest.digest, msg.msg_data.block_digest.digest, sizeof (block_digest.digest));
	}
      queue_push (&client->cmd_out.queue, &msg);
    }
  return (NULL);
}

static status_t
start_data_writer (client_t * client)
{
  pthread_t data_writer_id;
  int rv = pthread_create (&data_writer_id, NULL, data_writer, &client);
  if (rv != 0)
    {
      ERROR_MSG ("Failed to start command writer thread");
      return (ST_FAILURE);
    }
  
  status_t status = reader (client);
  
  pthread_cancel (data_writer_id);
  pthread_join (data_writer_id, NULL);

  return (status);
}

static status_t
start_cmd_writer (client_t * client)
{
  pthread_t cmd_writer_id;
  int rv = pthread_create (&cmd_writer_id, NULL, cmd_writer, &client);
  if (rv != 0)
    {
      ERROR_MSG ("Failed to start command writer thread");
      return (ST_FAILURE);
    }

  status_t status = start_data_writer (client);
  
  pthread_cancel (cmd_writer_id);
  pthread_join (cmd_writer_id, NULL);
  
  return (status);
}

static status_t
start_digest_calculators (client_t * client)
{
  int i, ncpu = (long) sysconf (_SC_NPROCESSORS_ONLN);
  pthread_t ids[ncpu];
  status_t status = ST_SUCCESS;

  for (i = 0; i < ncpu; ++i)
    {
      status = pthread_create (&ids[i], NULL, digest_calculator, client);
      if (ST_SUCCESS != status)
	break;
    }

  if (ST_SUCCESS == status)
    status = start_cmd_writer (client);

  for ( ; i>= 0; --i)
    {
      pthread_cancel (ids[i]);
      pthread_join (ids[i], NULL);
    }
  
  return (status);
}

static status_t
run_session (connection_t * connection)
{
  client_t client = { .connection = connection };
  msg_t cmd_out_array_data[MSG_OUT_QUEUE_SIZE];
  msg_t cmd_in_array_data[MSG_IN_QUEUE_SIZE];
  msg_t data_in_array_data[MSG_IN_QUEUE_SIZE];
  
  status_t status = send_file_meta (connection);
  if (ST_SUCCESS != status)
    return (status);
  
  status = MSG_QUEUE_INIT (&client.cmd_out, cmd_out_array_data);
  if (ST_SUCCESS != status)
    return (status);
  status = MSG_QUEUE_INIT (&client.cmd_in, cmd_in_array_data);
  if (ST_SUCCESS != status)
    return (status);
  status = MSG_QUEUE_INIT (&client.data_in, data_in_array_data);
  if (ST_SUCCESS != status)
    return (status);

  status = start_digest_calculators (&client);

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

  int rv = TEMP_FAILURE_RETRY (connect (connection->data_fd, (struct sockaddr *)name, sizeof (*name)));
  if (-1 != rv)
    status = run_session (connection);
  else
    {
      ERROR_MSG ("Connect failed errno(%d) '%s'.", errno, strerror (errno));
      return (ST_FAILURE);
    }

  close_connection (connection->data_fd);

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
  if (connection.cmd_fd < 0)
    {
      ERROR_MSG ("Command socket failed errno(%d) '%s'.", errno, strerror (errno));
      return (ST_FAILURE);
    }

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

  close_connection (connection.cmd_fd);
  
  return (status);
}

status_t
run_client (config_t * config)
{
  status_t status = ST_FAILURE;
  context_t context = { .config = config };
  
  context.file_fd = open64 (config->src_file, O_RDONLY);
  if (context.file_fd <= 0)
    {
      ERROR_MSG ("Can't open source file '%s'.", config->src_file);
      return (ST_FAILURE);
    }

  context.size = lseek64 (context.file_fd, 0, SEEK_END);
  status = connect_to_server (&context);
  close (context.file_fd);

  return (status);
}
