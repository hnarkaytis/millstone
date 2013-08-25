#ifdef HAVE_CONFIG_H
#include <config.h>
#endif /* HAVE_CONFIG_H */
#include <millstone.h>
#include <logging.h>
#include <block.h>
#include <msg.h>
#include <file_meta.h>
#include <client.h>

#include <stddef.h> /* size_t, ssize_t */
#include <unistd.h> /* TEMP_FAILURE_RETRY, sysconf, close, lseek64, SEEK_END */
#include <errno.h> /* errno */
#include <fcntl.h> /* open64 */
#include <string.h> /* memset, setlen, strerror */
#include <netdb.h> /* gethostbyname */
#include <netinet/in.h> /* htonl, struct sockaddr_in */
#include <sys/sysinfo.h> /* _SC_NPROCESSORS_ONLN */
#include <sys/mman.h> /* mmap64, unmap */
#include <sys/uio.h> /* writev, struct iovec */
#include <sys/socket.h> /* socket, shutdown, connect */

#include <openssl/sha.h> /* SHA1 */
#include <pthread.h>
#ifdef HAVE_ZLIB
#include <zlib.h>
#endif /* HAVE_ZLIB */

#include <metaresc.h>
#include <mr_ic.h>

TYPEDEF_STRUCT (client_t,
		(connection_t *, connection),
		(mr_ic_t, sent_blocks),
		(pthread_mutex_t, sent_blocks_lock),
		(msg_queue_t, data_in),
		(msg_queue_t, cmd_in),
		(msg_queue_t, cmd_out),
		)

static mr_hash_value_t
block_digest_hash (const mr_ptr_t key, const void * context)
{
  block_digest_t * block_digest = key.ptr;
  return (*((mr_hash_value_t*)(block_digest->digest)));
}

static int
block_digest_cmp (const mr_ptr_t x, const mr_ptr_t y, const void * context)
{
  block_digest_t * x_block_digest = x.ptr;
  block_digest_t * y_block_digest = y.ptr;
  return (memcmp (x_block_digest->digest, y_block_digest->digest, sizeof (x_block_digest->digest)));
}

static status_t
send_block (client_t * client, block_id_t * block_id)
{
  status_t status = ST_SUCCESS;
  unsigned char * block_data = &client->connection->context->data[block_id->offset];
  
  DUMP_VAR (block_id_t, block_id);

#ifdef HAVE_ZLIB
  Byte buffer[block_id->size << 1];
  uLongf length = sizeof (buffer);
  if (client->connection->context->config->compress_level > 0)
    {
      int z_status = compress2 (buffer, &length, block_data, block_id->size,
				client->connection->context->config->compress_level);
      if (Z_OK != z_status)
	status = ST_FAILURE;
    }
#endif /* HAVE_ZLIB */

  if (ST_SUCCESS == status)
    {
      struct iovec iov[] = {
	{ .iov_len = sizeof (*block_id), .iov_base = block_id, },
	{ .iov_len = sizeof (client->connection->context->config->compress_level),
	  .iov_base = &client->connection->context->config->compress_level, },
	{ .iov_len = block_id->size, .iov_base = block_data, },
      };

#ifdef HAVE_ZLIB
      if (client->connection->context->config->compress_level > 0)
	{
	  iov[2].iov_len = length;
	  iov[2].iov_base = buffer;
	}
#endif /* HAVE_ZLIB */
	  
      ssize_t rv = TEMP_FAILURE_RETRY (writev (client->connection->data_fd, iov, sizeof (iov) / sizeof (iov[0])));
      ssize_t i, len = 0;
      for (i = 0; i < sizeof (iov) / sizeof (iov[0]); ++i)
	len += iov[i].iov_len;

      DEBUG_MSG ("Wrote packet of %zd bytes. Expected to write %zd.", rv, len);

      if (rv != len)
	{
	  ERROR_MSG ("Failed to send data block (sent %d bytes, but expexted %d bytes)", rv, len);
	  status = ST_FAILURE;
	}
    }
  
  return (status);
}

static void *
client_data_writer (void * arg)
{
  client_t * client = arg;

  DEBUG_MSG ("Entered data writer.");
  for (;;)
    {
      msg_t msg;
      status_t status = queue_pop (&client->data_in.queue, &msg);
      if (ST_SUCCESS != status)
	break;
      
      DUMP_VAR (msg_t, &msg);
      status = send_block (client, &msg.msg_data.block_id);
      DEBUG_MSG ("Data block send status %d.", status);

      if (ST_SUCCESS != status)
	msg.msg_type = MT_BLOCK_SEND_ERROR;
      else
	msg.msg_type = MT_BLOCK_SENT;
      
      status = queue_push (&client->cmd_out.queue, &msg);
      if (ST_SUCCESS != status)
	break;
    }

  DEBUG_MSG ("Exiting client data writer.");
  return (NULL);
}

static void *
client_cmd_writer (void * arg)
{
  client_t * client = arg;
  DEBUG_MSG ("Enter client command writer.");
  for (;;)
    {
      msg_t msg;
      status_t status = queue_pop (&client->cmd_out.queue, &msg);
      if (ST_SUCCESS != status)
	break;
      
      DUMP_VAR (msg_t, &msg);
      status = msg_send (client->connection->cmd_fd, &msg);
      if (ST_SUCCESS != status)
	break;
      DEBUG_MSG ("Message sent.");
    }
  
  shutdown (client->connection->cmd_fd, SD_BOTH);
  DEBUG_MSG ("Exiting client command writer.");
  return (NULL);
}

static void *
digest_calculator (void * arg)
{
  client_t * client = arg;
  block_digest_t block_digest;
  block_digest_t * allocated_block_digest = NULL;
  long phys_pages = sysconf (_SC_PHYS_PAGES);
  msg_t msg;

  DEBUG_MSG ("Entered digest calculator.");

  memset (&block_digest, 0, sizeof (block_digest));
  
  for (;;)
    {
      status_t status = queue_pop (&client->cmd_in.queue, &msg);
      if (ST_SUCCESS != status)
	break;
      
      DUMP_VAR (msg_t, &msg);
      
      block_digest.block_id = msg.msg_data.block_digest.block_id;
      SHA1 (&client->connection->context->data[block_digest.block_id.offset],
	    block_digest.block_id.size, (unsigned char*)&block_digest.digest);

      long avphys_pages = sysconf (_SC_AVPHYS_PAGES);
      msg.msg_type = MT_BLOCK_MATCHED;
      msg.msg_data.block_matched.matched = !memcmp (block_digest.digest, msg.msg_data.block_digest.digest, sizeof (block_digest.digest));
      msg.msg_data.block_matched.duplicate = FALSE;

      DEBUG_MSG ("Block on offset %zd matched status %d.",
		 msg.msg_data.block_digest.block_id.offset, msg.msg_data.block_matched.matched);
	  
      if (100 * avphys_pages > phys_pages * client->connection->context->config->mem_threshold)
	{
	  if (allocated_block_digest == NULL)
	    allocated_block_digest = MR_MALLOC (sizeof (*allocated_block_digest));
	  if (allocated_block_digest != NULL)
	    {
	      *allocated_block_digest = block_digest;
	      pthread_mutex_lock (&client->sent_blocks_lock);
	      mr_ptr_t * find = mr_ic_add (&client->sent_blocks, allocated_block_digest, NULL);
	      pthread_mutex_unlock (&client->sent_blocks_lock);
	      if (find != NULL)
		{
		  block_digest_t * matched_block_digest = find->ptr;
		  bool duplicate = (matched_block_digest != allocated_block_digest);

		  if (!duplicate)
		    allocated_block_digest = NULL;
		  
		  msg.msg_data.block_matched.duplicate = duplicate;
		  msg.msg_data.block_matched.duplicate_block_id = matched_block_digest->block_id;
		}
	    }
	}
      
      DEBUG_MSG ("Push message:");
      DUMP_VAR (msg_t, &msg);
      status = queue_push (&client->cmd_out.queue, &msg);
      if (ST_SUCCESS != status)
	break;
      DEBUG_MSG ("Message pushed.");
    }

  if (allocated_block_digest != NULL)
    MR_FREE (allocated_block_digest);

  DEBUG_MSG ("Exiting digest calculator.");
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
      if (ST_SUCCESS != status)
	{
	  DEBUG_MSG ("Failed in msg_recv.");
	  break;
	}
      
      DUMP_VAR (msg_t, &msg);
      if (MT_TERMINATE == msg.msg_type)
	{
	  DEBUG_MSG ("Got termination command.");
	  break;
	}
      
      switch (msg.msg_type)
	{
	case MT_BLOCK_DIGEST:
	  status = queue_push (&client->cmd_in.queue, &msg);
	  break;
	case MT_BLOCK_REQUEST:
	  status = queue_push (&client->data_in.queue, &msg);
	  break;
	default:
	  ERROR_MSG ("Unexpected message type %d.", msg.msg_type);
	  status = ST_FAILURE;
	  break;
	}
      if (ST_SUCCESS != status)
	break;
    }
  DEBUG_MSG ("Exiting client command reader.");
  return (status);
}

static status_t
start_data_writer (client_t * client)
{
  int i, ncpu = (long) sysconf (_SC_NPROCESSORS_ONLN);
  pthread_t ids[ncpu];
  status_t status = ST_FAILURE;

  DEBUG_MSG ("Start server workers %d.", ncpu);
  for (i = 0; i < ncpu; ++i)
    {
      int rv = pthread_create (&ids[i], NULL, client_data_writer, client);
      if (rv != 0)
	break;
    }

  DEBUG_MSG ("Started %d.", i);
  if (i > 0)
    status = client_cmd_reader (client);

  DEBUG_MSG ("Canceling data writer.");
  queue_cancel (&client->data_in.queue);
  queue_cancel (&client->cmd_out.queue);
  for (--i ; i >= 0; --i)
    pthread_join (ids[i], NULL);
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
  queue_cancel (&client->cmd_out.queue);
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
      DEBUG_MSG ("Creatd thread [%d] %d.", i, ids[i]);
      if (rv != 0)
	break;
    }

  DEBUG_MSG ("Started %d.", i);
  if (i > 0)
    status = start_cmd_writer (client);

  DEBUG_MSG ("Canceling digest calculators.");
  queue_cancel (&client->cmd_in.queue);
  queue_cancel (&client->cmd_out.queue);
  for (--i ; i >= 0; --i)
    pthread_join (ids[i], NULL);
  DEBUG_MSG ("Digest calculators canceled.");
  
  return (status);
}

static mr_status_t
free_sent_blocks (mr_ptr_t node, const void * context)
{
  MR_FREE (node.ptr);
  return (MR_SUCCESS);
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

  pthread_mutex_init (&client.sent_blocks_lock, NULL);
  mr_status_t mr_status = mr_ic_new (&client.sent_blocks, block_digest_hash, block_digest_cmp, "block_digest_t", MR_IC_HASH);

  DEBUG_MSG ("Session inited.");
  if (MR_SUCCESS == mr_status)
    {
      status = start_digest_calculators (&client);
      
      pthread_mutex_lock (&client.sent_blocks_lock);
      mr_ic_foreach (&client.sent_blocks, free_sent_blocks, NULL);
      mr_ic_free (&client.sent_blocks, NULL);
      pthread_mutex_unlock (&client.sent_blocks_lock);
    }
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
  DEBUG_MSG ("Shutdown data socket.");

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
  context.data = mmap64 (NULL, context.size, PROT_READ, MAP_PRIVATE, context.file_fd, 0);
  
  if (-1 == (long)context.data)
    FATAL_MSG ("Failed to map file into memory. Error (%d) %s.\n", errno, strerror (errno));
  else
    {
      status = create_server_socket (&context);

      if (0 != munmap (context.data, context.size))
	ERROR_MSG ("Failed to unmap memory. Error (%d) %s.\n", errno, strerror (errno));
    }
  
  close (context.file_fd);

  DEBUG_MSG ("Exiting client.");
  return (status);
}
