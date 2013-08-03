#define _LARGEFILE64_SOURCE
#define _GNU_SOURCE
#include <unistd.h>
#include <fcntl.h>
#include <netdb.h>
#include <errno.h>
#include <sys/mman.h>

#include <pthread.h>

#include <client.h>
#include <queue.h>
#include <logging.h>

#define MSG_OUT_QUEUE_SIZE (2)
#define MSG_IN_QUEUE_SIZE (16)

TYPEDEF_STRUCT (msg_queue_t,
		(queue_t, queue),
		RARRAY (msg_t, array),
		)

TYPEDEF_STRUCT (client_t,
		(connection_t *, connection),
		(msg_queue_t, data_in),
		(msg_queue_t, cmd_in),
		(msg_queue_t, cmd_out),
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

static void
close_connection (int fd)
{
  shutdown (fd, SD_BOTH);
  close (fd);
}

static void
close_connection_void (void * arg)
{
  close_connection ((long)arg);
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

  int rv = TEMP_FAILURE_RETRY (writev (client->connection->data_fd, iov, sizeof (iov) / sizeof (iov[0])));
  int i, len = 0;
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
msg_queue_init (msg_queue_t * msg_queue, msg_t * array, size_t size)
{
  msg_queue->array.data = array;
  msg_queue->array.size = size;
  msg_queue->array.alloc_size = -1;
  status_t status = queue_init (&msg_queue->queue, (mr_rarray_t*)&msg_queue->array, sizeof (msg_queue->array.data[0]));
  return (status);
}

#define MSG_QUEUE_INIT(MSG_QUEUE, ARRAY) msg_queue_init (MSG_QUEUE, ARRAY, sizeof (ARRAY))

static status_t
start_session (connection_t * connection)
{
  client_t client = { .connection = connection };
  status_t status = send_file_meta (connection);
  msg_t cmd_out_array_data[MSG_OUT_QUEUE_SIZE];
  msg_t cmd_in_array_data[MSG_IN_QUEUE_SIZE];
  msg_t data_in_array_data[MSG_IN_QUEUE_SIZE];
  
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

  pthread_cleanup_push (close_connection_void, (void*)(long)connection->data_fd);

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

  pthread_cleanup_push (close_connection_void, (void*)(long)connection.cmd_fd);

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
