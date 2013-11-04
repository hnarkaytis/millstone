#define _GNU_SOURCE /* TEMP_FAILURE_RETRY */

#ifdef HAVE_CONFIG_H
#include <config.h>
#endif /* HAVE_CONFIG_H */
#include <millstone.h> /* config_t */
#include <logging.h>
#include <file.h> /* file_id_t */
#include <block.h> /* block_id_t */
#include <connection.h> /* connection_t */
#include <file_pool.h> /* file_pool_t */
#include <msg.h> /* msg_t */
#include <client.h>

#include <stddef.h> /* size_t, ssize_t */
#include <unistd.h> /* TEMP_FAILURE_RETRY */
#include <errno.h> /* errno */
#include <string.h> /* memset, strerror */
#include <netdb.h> /* gethostbyname */
#include <netinet/in.h> /* htonl, struct sockaddr_in */
#include <netinet/tcp.h> /* TCP_NODELAY */
#include <sys/uio.h> /* writev, struct iovec */
#include <sys/sendfile.h> /* sendfile64 */

#include <openssl/sha.h> /* SHA1 */
#include <pthread.h>

#undef MIN_BLOCK_SIZE
#define MIN_BLOCK_SIZE (PAGE_SIZE << 3)

TYPEDEF_STRUCT (task_t,
		(block_id_t, block_id),
		(size_t, size),
		)

TYPEDEF_STRUCT (client_t,
		(connection_t *, connection),
		(file_pool_t, file_pool),
		(llist_t, tasks),
		(llist_t, blocks),
		)

static void
cancel_client (client_t * client)
{
  file_pool_cancel (&client->file_pool);
  connection_cancel (client->connection);
  llist_cancel (&client->tasks);
  llist_cancel (&client->blocks);
}

static status_t
client_main_loop (void * arg)
{
  client_t * client = arg;
  status_t status = ST_SUCCESS;
  char ** src_file;
  msg_t msg;

  TRACE_MSG ("Enter main loop.");
  
  memset (&msg, 0, sizeof (msg));
  
  for (src_file = client->connection->config->src_files; *src_file != NULL; ++src_file)
    {
      fd_t * fd = client_open_file (&client->file_pool, *src_file);
      if (NULL == fd)
	continue;

      TRACE_MSG ("Opened file '%s'.", *src_file);
      
      file_pool_ref_fd (&client->file_pool, fd, 1);
      
      msg.msg_type = MT_OPEN_FILE;
      msg.file = fd->file;
      status = connection_msg_push (client->connection, &msg);
      if (ST_SUCCESS != status)
	break;
    }
  
  TRACE_MSG ("Wait for end of file transfer with status %d.", status);

  if (ST_SUCCESS == status)
    {
      file_pool_finalize (&client->file_pool);
      msg.msg_type = MT_CMD_CONNECTION_END;
      status = connection_msg_push (client->connection, &msg);
      connection_finalize (client->connection);
    }

  TRACE_MSG ("Exiting main loop.");

  cancel_client (client);
  
  return (status);
}

static void *
client_cmd_writer (void * arg)
{
  client_t * client = arg;
  connection_cmd_writer (client->connection);
  cancel_client (client);
  return (NULL);
}

static status_t
file_unref (client_t * client, file_id_t * file_id)
{
  status_t status = ST_SUCCESS;
  fd_t * fd = file_pool_get_fd (&client->file_pool, file_id);
  if (NULL == fd)
    return (ST_FAILURE);

  int64_t ref_count = file_pool_ref_fd (&client->file_pool, fd, -1);

  TRACE_MSG ("Unref count %d.", ref_count);

  if (0 == ref_count)
    {
      msg_t msg;
      memset (&msg, 0, sizeof (msg));
      msg.msg_type = MT_CLOSE_FILE;
      msg.close_file_info.file_id = *file_id;
      msg.close_file_info.sent_blocks = fd->sent_blocks;
      status = connection_msg_push (client->connection, &msg);
      file_pool_fd_close (&client->file_pool, fd);
    }
  return (status);
}

static status_t
msg_block_matched (client_t * client, block_matched_t * block_matched)
{
  status_t status = ST_SUCCESS;
  
  if (block_matched->matched)
    status = file_unref (client, &block_matched->block_id.file_id);
  else
    {
      if (block_matched->block_id.size <= MIN_BLOCK_SIZE)
	status = llist_push (&client->blocks, &block_matched->block_id);
      else
	{
	  task_t task;
	  memset (&task, 0, sizeof (task));
	  task.block_id = block_matched->block_id;
	  for (task.size = MIN_BLOCK_SIZE;
	       task.size * SPLIT_RATIO < block_matched->block_id.size;
	       task.size *= SPLIT_RATIO);
	  status = llist_push (&client->tasks, &task);
	}
    }
  
  return (status);
}

static status_t
start_file_sync (client_t * client, file_id_t * file_id)
{
  task_t task;
  fd_t * fd = file_pool_get_fd (&client->file_pool, file_id);
  if (NULL == fd)
    return (ST_FAILURE);
  
  memset (&task, 0, sizeof (task));
  task.block_id.offset = 0;
  task.block_id.size = fd->file.size;
  task.block_id.file_id = fd->file.file_id;
  task.size = MAX_BLOCK_SIZE;
  return (llist_push (&client->tasks, &task));
}

static status_t
file_transfer (client_t * client, file_id_t * file_id)
{
  fd_t * fd = file_pool_get_fd (&client->file_pool, file_id);
  if (NULL == fd)
    return (ST_FAILURE);

  TRACE_MSG ("Start file transfer for file '%s'.", fd->file.file_name);
  
  status_t status = ST_SUCCESS;
  block_id_t block_id;
  memset (&block_id, 0, sizeof (block_id));
  block_id.file_id = *file_id;
  block_id.size = MIN_BLOCK_SIZE;
    
  for (block_id.offset = 0; block_id.offset < fd->file.size; block_id.offset += block_id.size)
    {
      if (block_id.size > fd->file.size - block_id.offset)
	block_id.size = fd->file.size - block_id.offset;
      file_pool_ref_fd (&client->file_pool, fd, 1);
      status = llist_push (&client->blocks, &block_id);
      if (ST_SUCCESS != status)
	break;
    }

  TRACE_MSG ("File '%s' transfer ended with status %d. Ref count %d.", fd->file.file_name, status, fd->ref_count);
  
  if (ST_SUCCESS == status)
    status = file_unref (client, file_id);

  return (status);
}

static void *
client_cmd_reader (void * arg)
{
  client_t * client = arg;
  msg_t msg;
  memset (&msg, 0, sizeof (msg));
  
  for (;;)
    {
      status_t status = msg_recv (client->connection->cmd_fd, &msg);
      if (ST_SUCCESS != status)
	break;
      switch (msg.msg_type)
	{
	case MT_BLOCK_MATCHED:
	  status = msg_block_matched (client, &msg.block_matched);
	  break;
	case MT_OPEN_FILE_SUCCESS:
	  status = start_file_sync (client, &msg.file_id);
	  break;
	case MT_OPEN_FILE_NEW:
	  status = file_transfer (client, &msg.file_id);
	  break;
	case MT_OPEN_FILE_FAILURE:
	  status = file_unref (client, &msg.file_id);
	  break;
	default:
	  status = ST_FAILURE;
	  break;
	}
      MR_FREE_RECURSIVELY (msg_t, &msg);
      if (ST_SUCCESS != status)
	break;
    }
  
  cancel_client (client);
  return (NULL);
}

static void *
client_worker (void * arg)
{
  client_t * client = arg;
  task_t task;
  msg_t msg;
  off64_t offset;

  memset (&msg, 0, sizeof (msg));
  memset (&task, 0, sizeof (task));
  msg.msg_type = MT_BLOCK_DIGEST;
  
  for (;;)
    {
      status_t status = llist_pop (&client->tasks, &task);
      if (ST_SUCCESS != status)
	break;
      
      TRACE_MSG ("Server worker got task: offset:size 0x%" SCNx64 ":%" SCNx32 " split on 0x%zx.",
		 task.block_id.offset, task.block_id.size, task.size);
      
      fd_t * fd = file_pool_get_fd (&client->file_pool, &task.block_id.file_id);
      if (NULL == fd)
	break;
	  
      msg.block_digest.block_id.size = task.size;
      msg.block_digest.block_id.file_id = task.block_id.file_id;
      
      for (offset = 0; offset < task.block_id.size; offset += msg.block_digest.block_id.size)
	{
	  msg.block_digest.block_id.offset = task.block_id.offset + offset;
	  if (msg.block_digest.block_id.size > task.block_id.size - offset)
	    msg.block_digest.block_id.size = task.block_id.size - offset;

	  SHA1 (&fd->data[msg.block_digest.block_id.offset], msg.block_digest.block_id.size, (unsigned char*)&msg.block_digest.digest);

	  TRACE_MSG ("Pushing to outgoing queue digest for offset 0x%" SCNx64 ".", msg.block_digest.block_id.offset);
	  
	  file_pool_ref_fd (&client->file_pool, fd, 1);

	  status = connection_msg_push (client->connection, &msg);
	  if (ST_SUCCESS != status)
	    break;

	  TRACE_MSG ("Pushed digest for offset 0x%" SCNx64 ".", msg.block_digest.block_id.offset);
	}
      
      if (ST_SUCCESS != file_unref (client, &task.block_id.file_id))
	break;

      if (ST_SUCCESS != status)
	break;
    }
  
  TRACE_MSG ("Exiting server worker.");

  cancel_client (client);
  return (NULL);
}

static status_t
start_cmd_writer (void * arg)
{
  return (start_threads (client_cmd_writer, 1, client_main_loop, arg));
}

static status_t
start_cmd_reader (void * arg)
{
  return (start_threads (client_cmd_reader, 1, start_cmd_writer, arg));
}

static status_t
start_client_workers (void * arg)
{
  client_t * client = arg;
  return (start_threads (client_worker, client->connection->config->workers_number, start_cmd_reader, client));
}

static status_t
send_block (client_t * client, int data_fd, block_id_t block_id)
{
  fd_t * fd = file_pool_get_fd (&client->file_pool, &block_id.file_id);
  if (NULL == fd)
    return (ST_SUCCESS);
      
  struct iovec iov[] = { { .iov_base = &block_id, .iov_len = sizeof (block_id), } };
  if (ST_SUCCESS != buf_send (data_fd, iov, sizeof (iov) / sizeof (iov[0])))
    return (ST_FAILURE);
  
  while (block_id.size > 0)
    {
      ssize_t rv = TEMP_FAILURE_RETRY (sendfile64 (data_fd, fd->fd, &block_id.offset, block_id.size));
      if (rv <= 0)
	return (ST_FAILURE);
      block_id.offset += rv;
      block_id.size -= rv;
    }

  file_sent_block (fd);
  return (file_unref (client, &block_id.file_id));
}

static void
data_connection_main_loop (client_t * client, int data_fd)
{
  msg_t msg;
  memset (&msg, 0, sizeof (msg));
  msg.msg_type = MT_DATA_CONNECTION_START;
  msg.client_id.id = client->connection->local.sin_port;
  if (ST_SUCCESS != msg_send (data_fd, &msg))
    return;
  
  for (;;)
    {
      block_id_t block_id;
      status_t status = llist_pop (&client->blocks, &block_id);
      if (ST_SUCCESS != status)
	return;

      status = send_block (client, data_fd, block_id);
      if (ST_SUCCESS != status)
	{
	  INFO_MSG ("Lost data connection. Push task 0x%" SCNx64 ":0x%" SCNx32 "to queue.", block_id.offset, block_id.size);
	  
	  llist_push (&client->blocks, &block_id);
	  return;
	}
    }
}

static void *
start_data_connection (void * arg)
{
  client_t * client = arg;
  connection_t * connection = client->connection;
  int data_fd = socket (PF_INET, SOCK_STREAM, IPPROTO_TCP);
  if (data_fd < 0)
    {
      ERROR_MSG ("Data socket failed errno(%d) '%s'.", errno, strerror (errno));
      return (NULL);
    }
  int rv = TEMP_FAILURE_RETRY (connect (data_fd, (struct sockaddr *)&connection->remote, sizeof (connection->remote)));
  if (rv < 0)
    ERROR_MSG ("Connect failed errno(%d) '%s'.", errno, strerror (errno));
  else
    {
      data_connection_main_loop (client, data_fd);
      shutdown (data_fd, SD_BOTH);
    }
  close (data_fd);
  return (NULL);
}

static status_t
init_cmd_session (int fd)
{
  msg_t msg;
  memset (&msg, 0, sizeof (msg));
  msg.msg_type = MT_CMD_CONNECTION_START;

  TRACE_MSG ("Sending command connect handshake.");
  
  status_t status = msg_send (fd, &msg);
  if (ST_SUCCESS != status)
    return (ST_FAILURE);
  
  status = msg_recv (fd, &msg);

  TRACE_MSG ("Handshake status %d.", status);
  
  if (ST_SUCCESS != status)
    return (ST_FAILURE);
  
  if (MT_CMD_CONNECTION_STARTED != msg.msg_type)
    return (ST_FAILURE);
  return (ST_SUCCESS);
}

static status_t
start_client (connection_t * connection)
{
  client_t client;

  memset (&client, 0, sizeof (client));
  client.connection = connection;
  file_pool_init (&client.file_pool);
  LLIST_INIT (&client.tasks, task_t, -1);
  LLIST_INIT (&client.blocks, block_id_t, -1);

  status_t status = init_cmd_session (connection->cmd_fd);

  TRACE_MSG ("Init command session returned %d.", status);
  
  if (ST_SUCCESS == status)
    status = start_threads (start_data_connection, connection->config->data_connections, start_client_workers, &client);

  file_pool_cleanup (&client.file_pool);

  TRACE_MSG ("Client finished.");
  
  return (status);
}

static status_t
connect_to_server (connection_t * connection)
{
  TRACE_MSG ("Connect to %08x:%04x.", connection->remote.sin_addr.s_addr, connection->remote.sin_port);

  int rv = TEMP_FAILURE_RETRY (connect (connection->cmd_fd, (struct sockaddr *)&connection->remote, sizeof (connection->remote)));
  if (rv < 0)
    {
      ERROR_MSG ("Connect failed errno(%d) '%s'.", errno, strerror (errno));
      return (ST_FAILURE);
    }

  int tcp_nodelay = !0;
  rv = setsockopt (connection->cmd_fd, SOL_TCP, TCP_NODELAY, &tcp_nodelay, sizeof (tcp_nodelay));
  if (rv != 0)
    WARN_MSG ("Failed to turn off Nigel algorithm with errno %d - %s.", errno, strerror (errno));
  size_t buf_size = EXPECTED_PACKET_SIZE;
  rv = setsockopt (connection->cmd_fd, SOL_SOCKET, SO_SNDBUF, &buf_size, sizeof (buf_size));
  if (rv != 0)
    WARN_MSG ("Failed to set size of outgoing buffer with errno %d - %s.", errno, strerror (errno));
  socklen_t socklen = sizeof (connection->local); 
  getsockname (connection->cmd_fd, (struct sockaddr *)&connection->local, &socklen); 

  TRACE_MSG ("Connected to server successfully.");
  
  status_t status = start_client (connection);

  shutdown (connection->cmd_fd, SD_BOTH);

  TRACE_MSG ("Shutdown command socket.");

  return (status);
}

static status_t
create_cmd_socket (connection_t * connection)
{
  connection->cmd_fd = socket (PF_INET, SOCK_STREAM, IPPROTO_TCP);
  if (connection->cmd_fd < 0)
    {
      ERROR_MSG ("Command socket failed errno(%d) '%s'.", errno, strerror (errno));
      return (ST_FAILURE);
    }

  TRACE_MSG ("Created command socket.");
  
  status_t status = connect_to_server (connection);
  
  close (connection->cmd_fd);

  TRACE_MSG ("Close command socket.");
  
  return (status);
}

status_t
run_client (config_t * config)
{
  connection_t connection;

  connection_init (&connection, config);

  struct hostent * hostinfo = gethostbyname (config->dst_host);
  if (hostinfo != NULL)
    connection.remote.sin_addr.s_addr = *((in_addr_t*) hostinfo->h_addr);
  else
    connection.remote.sin_addr.s_addr = htonl (INADDR_ANY);

  connection.remote.sin_family = AF_INET;
  connection.remote.sin_port = htons (config->dst_port);

  return (create_cmd_socket (&connection));
}
