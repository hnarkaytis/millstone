#define _GNU_SOURCE /* TEMP_FAILURE_RETRY */

#ifdef HAVE_CONFIG_H
#include <config.h>
#endif /* HAVE_CONFIG_H */
#include <millstone.h> /* config_t, status_t */
#include <logging.h> /* *_MSG */
#include <block.h> /* block_id_t */
#include <connection.h> /* connection_t */
#include <file.h> /* file_id_t */
#include <msg.h> /* msg_t */
#include <sync_storage.h> /* sync_storage_t */
#include <llist.h> /* llist_t */
#include <file_pool.h> /* file_pool_t */
#include <client.h> /* client_id_t */
#include <server.h>

#include <stddef.h> /* size_t, ssize_t */
#include <signal.h> /* signal, SIG_IGN, SIGPIPE */
#include <unistd.h> /* TEMP_FAILURE_RETRY, close */
#include <inttypes.h> /* SCNx64 */
#include <string.h> /* memset, strerror */
#include <errno.h> /* errno */
#include <netinet/in.h> /* htonl, struct sockaddr_in, INADDR_ANY */
#include <sys/socket.h> /* socklen_t, socket, setsockopt, getsockname, bind, accept, listen, shutdown */
#include <netinet/tcp.h> /* TCP_NODELAY */

#include <openssl/sha.h> /* SHA1 */
#include <pthread.h>

#include <metaresc.h>

TYPEDEF_STRUCT (server_ctx_t,
		(config_t *, config),
		int server_sock,
		(sync_storage_t, clients),
		)

TYPEDEF_STRUCT (server_t,
		(connection_t *, connection),
		(file_pool_t, file_pool),
		(llist_t, cmd_in),
		(pthread_mutex_t, mutex),
		(pthread_cond_t, cond),
		(int, ref_count),
		)

TYPEDEF_STRUCT (accepter_ctx_t,
		(server_ctx_t *, server_ctx),
		(struct sockaddr_in, remote),
		(socklen_t, remote_addr_size),
		int fd,
		(pthread_mutex_t, mutex),
		)

int
server_compar (const mr_ptr_t x, const mr_ptr_t y, const void * null)
{
  server_t * server_x = x.ptr;
  server_t * server_y = y.ptr;
  struct sockaddr_in * addr_x = &server_x->connection->remote;
  struct sockaddr_in * addr_y = &server_y->connection->remote;
  int cmp = (addr_x->sin_addr.s_addr > addr_y->sin_addr.s_addr) -
    (addr_x->sin_addr.s_addr < addr_y->sin_addr.s_addr);
  if (cmp)
    return (cmp);
  cmp = (addr_x->sin_port > addr_y->sin_port) -
    (addr_x->sin_port < addr_y->sin_port);
  return (cmp);
}

mr_hash_value_t
server_hash (const mr_ptr_t key, const void * context)
{
  server_t * server = key.ptr;
  struct sockaddr_in * addr = &server->connection->remote;
  return (addr->sin_addr.s_addr + 0xDeadBeef * addr->sin_port);
}

static void
cancel_server (server_t * server)
{
  llist_cancel (&server->cmd_in);
  connection_cancel (server->connection);
}

static status_t
server_cmd_reader (void * arg)
{
  server_t * server = arg;
  status_t status = ST_SUCCESS;
  
  TRACE_MSG ("Start server command reader.");
  
  for (;;)
    {
      msg_t msg;
      status = msg_recv (server->connection->cmd_fd, &msg);
      if (ST_SUCCESS != status)
	break;

      DUMP_VAR (msg_t, &msg);
      
      if (MT_CMD_CONNECTION_END == msg.msg_type)
	break;

      status = llist_push (&server->cmd_in, &msg);
      MR_FREE_RECURSIVELY (msg_t, &msg);
      if (ST_SUCCESS != status)
	break;
    }

  if (ST_SUCCESS == status)
    {
      TRACE_MSG ("Wait for file pool release.");
      
      file_pool_finalize (&server->file_pool);
    }

  cancel_server (server);
  
  TRACE_MSG ("Exiting server command reader with status %d.", status);
  
  return (status);
}

static void *
server_cmd_writer (void * arg)
{
  server_t * server = arg;

  TRACE_MSG ("Start server command writer.");

  connection_cmd_writer (server->connection);
  cancel_server (server);

  TRACE_MSG ("Exiting server command writer.");
  
  return (NULL);
}

static status_t
start_cmd_writer (void * server)
{
  return (start_threads (server_cmd_writer, 1, server_cmd_reader, server));
}

static msg_type_t
server_file_status (server_t * server, file_t * file)
{
  int rv = access (file->file_name, F_OK);
  bool file_exists = (0 == rv);
  if (file_exists)
    {
      rv = access (file->file_name, R_OK | W_OK);
      if (rv != 0)
	{
	  ERROR_MSG ("File (%s) access rights are not 'rw'.", file->file_name);
	  return (MT_OPEN_FILE_FAILURE);
	}
    }

  fd_t * fd = server_open_file (&server->file_pool, file);
  if (NULL == fd)
    return (MT_OPEN_FILE_FAILURE);

  file_pool_ref_fd (&server->file_pool, fd, 1);
  return (file_exists ? MT_OPEN_FILE_SUCCESS : MT_OPEN_FILE_NEW);
}

static status_t
open_file (server_t * server, file_t * file)
{
  msg_t msg;
  memset (&msg, 0, sizeof (msg));
  msg.file_id = file->file_id;
  msg.msg_type = server_file_status (server, file);

  DUMP_VAR (msg_t, &msg);
  
  return (msg_send (server->connection->cmd_fd, &msg));
}

static status_t
close_file (server_t * server, close_file_info_t * close_file_info)
{
  fd_t * fd = file_pool_get_fd (&server->file_pool, &close_file_info->file_id);
  if (NULL != fd)
    if (0 == file_pool_ref_fd (&server->file_pool, fd, -(close_file_info->sent_blocks + 1)))
      file_pool_fd_close (&server->file_pool, fd);
  return (ST_SUCCESS);
}

static status_t
calc_block_digest (server_t * server, block_digest_t * block_digest)
{
  fd_t * fd = file_pool_get_fd (&server->file_pool, &block_digest->block_id.file_id);
  if (NULL == fd)
    return (ST_FAILURE);

  unsigned char digest[SHA_DIGEST_LENGTH];
  SHA1 (&fd->data[block_digest->block_id.offset], block_digest->block_id.size, digest);

  msg_t msg;
  memset (&msg, 0, sizeof (msg));
  msg.msg_type = MT_BLOCK_MATCHED;
  msg.block_matched.block_id = block_digest->block_id;
  msg.block_matched.matched = !memcmp (block_digest->digest, digest, sizeof (digest));
  return (msg_send (server->connection->cmd_fd, &msg));
}

static void *
server_worker (void * arg)
{
  server_t * server = arg;
  status_t status = ST_SUCCESS;
  msg_t msg;

  memset (&msg, 0, sizeof (msg));
  for (;;)
    {
      status = llist_pop (&server->cmd_in, &msg);
      if (ST_SUCCESS != status)
	break;
      switch (msg.msg_type)
	{
	case MT_OPEN_FILE:
	  status = open_file (server, &msg.file);
	  break;
	case MT_CLOSE_FILE:
	  status = close_file (server, &msg.close_file_info);
	  break;
	case MT_BLOCK_DIGEST:
	  status = calc_block_digest (server, &msg.block_digest);
	  break;
	default:
	  status = ST_FAILURE;
	  break;
	}
      MR_FREE_RECURSIVELY (msg_t, &msg);
      if (ST_SUCCESS != status)
	break;
    }
  return (NULL);
}

static void
wait_ref_zero (mr_ptr_t found, mr_ptr_t serched, void * context)
{
  server_t * server = found.ptr;
  pthread_mutex_lock (&server->mutex);
  while (server->ref_count != 0)
    pthread_cond_wait (&server->cond, &server->mutex);
  pthread_mutex_unlock (&server->mutex);
}

static status_t
start_new_server (accepter_ctx_t * accepter_ctx)
{
  status_t status = ST_SUCCESS;

  TRACE_MSG ("Start new command connection.");
  
  connection_t connection;
  connection_init (&connection, accepter_ctx->server_ctx->config);
  connection.cmd_fd = accepter_ctx->fd;
  connection.remote = accepter_ctx->remote;
  
  int tcp_nodelay = !0;
  int rv = setsockopt (connection.cmd_fd, SOL_TCP, TCP_NODELAY, &tcp_nodelay, sizeof (tcp_nodelay));
  if (rv != 0)
    WARN_MSG ("Failed to turn off Nigel algorithm with errno %d - %s.", errno, strerror (errno));
  size_t buf_size = EXPECTED_PACKET_SIZE;
  rv = setsockopt (connection.cmd_fd, SOL_SOCKET, SO_SNDBUF, &buf_size, sizeof (buf_size));
  if (rv != 0)
    WARN_MSG ("Failed to set size of outgoing buffer with errno %d - %s.", errno, strerror (errno));
  
  server_t server;
  memset (&server, 0, sizeof (server));
  server.connection = &connection;
  server.ref_count = 0;
  pthread_mutex_init (&server.mutex, NULL);
  pthread_cond_init (&server.cond, NULL);

  file_pool_init (&server.file_pool);
  LLIST_INIT (&server.cmd_in, msg_t, -1);

  if (NULL != sync_storage_add (&accepter_ctx->server_ctx->clients, &server))
    {
      msg_t msg;
      memset (&msg, 0, sizeof (msg));
      msg.msg_type = MT_CMD_CONNECTION_STARTED;
      status = msg_send (connection.cmd_fd, &msg);
      if (ST_SUCCESS == status)
	status = start_threads (server_worker, server.connection->config->workers_number, start_cmd_writer, &server);
      sync_storage_del (&accepter_ctx->server_ctx->clients, &server, wait_ref_zero);
    }
  
  llist_cancel (&server.cmd_in);
  file_pool_cleanup (&server.file_pool);

  TRACE_MSG ("Close command connection.");
  
  return (status);
}

static status_t
data_connection_worker (server_t * server, int data_fd)
{
  status_t status = ST_SUCCESS;

  TRACE_MSG ("Data worker started.");
  
  for (;;)
    {
      block_id_t block_id;
      status = buf_recv (data_fd, &block_id, sizeof (block_id));
      if (ST_SUCCESS != status)
	break;

      DUMP_VAR (block_id_t, &block_id);
      
      status = ST_FAILURE;
      fd_t * fd = file_pool_get_fd (&server->file_pool, &block_id.file_id);
      if (NULL == fd)
	break;
      status = buf_recv (data_fd, &fd->data[block_id.offset], block_id.size);
      if (ST_SUCCESS != status)
	break;
      if (0 == file_pool_ref_fd (&server->file_pool, fd, 1))
	file_pool_fd_close (&server->file_pool, fd);
    }

  TRACE_MSG ("Data worker finished.");

  return (status);
}

static void
ref_server (server_t * server)
{
  pthread_mutex_lock (&server->mutex);
  ++server->ref_count;
  pthread_mutex_unlock (&server->mutex);
}

static void
unref_server (server_t * server)
{
  pthread_mutex_lock (&server->mutex);
  if (0 == --server->ref_count)
    pthread_cond_broadcast (&server->cond);
  pthread_mutex_unlock (&server->mutex);
}

static void
server_found (mr_ptr_t found, mr_ptr_t serched, void * context)
{
  ref_server (found.ptr);
}

static status_t
start_new_data_connection (accepter_ctx_t * accepter_ctx, client_id_t * client_id)
{
  server_t server;
  connection_t connection;
  memset (&server, 0, sizeof (server));
  memset (&connection, 0, sizeof (connection));
  server.connection = &connection;
  connection.remote.sin_addr = accepter_ctx->remote.sin_addr;
  connection.remote.sin_port = client_id->id;

  TRACE_MSG ("Data connection for client from %08x:%04x.", connection.remote.sin_addr.s_addr, connection.remote.sin_port);
  
  mr_ptr_t * find = sync_storage_find (&accepter_ctx->server_ctx->clients, &server, server_found);
  if (NULL == find)
    return (ST_FAILURE);

  TRACE_MSG ("Client from %08x:%04x found.", connection.remote.sin_addr.s_addr, connection.remote.sin_port);

  status_t status = data_connection_worker (find->ptr, accepter_ctx->fd);
  unref_server (find->ptr);
  
  return (status);
}

static void *
handle_client (void * arg)
{
  accepter_ctx_t * ctx = arg;
  accepter_ctx_t accepter_ctx = *ctx;
  pthread_mutex_unlock (&ctx->mutex);

  msg_t msg;
  memset (&msg, 0, sizeof (msg));
  status_t status = msg_recv (accepter_ctx.fd, &msg);

  TRACE_MSG ("Handshake status %d.", status);
  
  if (ST_SUCCESS == status)
    {
      switch (msg.msg_type)
	{
	case MT_CMD_CONNECTION_START:
	  start_new_server (&accepter_ctx);
	  break;
	case MT_DATA_CONNECTION_START:
	  start_new_data_connection (&accepter_ctx, &msg.client_id);
	  break;
	default:
	  break;
	}

      MR_FREE_RECURSIVELY (msg_t, &msg);
    }
  
  shutdown (accepter_ctx.fd, SHUT_RDWR);
  close (accepter_ctx.fd);
  
  TRACE_MSG ("Closed connection to client: %08x:%04x.", accepter_ctx.remote.sin_addr.s_addr, accepter_ctx.remote.sin_port);

  return (NULL);
}

static status_t
run_accepter (server_ctx_t * server_ctx)
{
  int reuse_addr = !0;
  struct linger linger_opt = { .l_onoff = 0, .l_linger = 0, };

  TRACE_MSG ("Apply options on a server socket.");
  
  int rv = setsockopt (server_ctx->server_sock, SOL_SOCKET, SO_REUSEADDR, &reuse_addr, sizeof (reuse_addr));
  if (rv != 0)
    WARN_MSG ("Failed to set reuse option for socket with errno %d - %s.", errno, strerror (errno));
  rv = setsockopt (server_ctx->server_sock, SOL_SOCKET, SO_LINGER, &linger_opt, sizeof (linger_opt));
  if (rv != 0)
    WARN_MSG ("Failed set liner time for socket with errno %d - %s.", errno, strerror (errno));

  struct sockaddr_in server_name;
  server_name.sin_family = AF_INET;
  server_name.sin_port = htons (server_ctx->config->listen_port);
  server_name.sin_addr.s_addr = htonl (INADDR_ANY);

  rv = bind (server_ctx->server_sock, (struct sockaddr *)&server_name, sizeof (server_name));
  
  TRACE_MSG ("Binding server command socket. Return value %d.", rv);
  
  if (rv < 0)
    {
      ERROR_MSG ("bind failed errno(%d) '%s'.", errno, strerror (errno));
      return (ST_FAILURE);
    }

  rv = listen (server_ctx->server_sock, 1);
  
  TRACE_MSG ("Set incoming queue size for server command socket. Return value %d.", rv);
  
  if (rv < 0)
    {
      ERROR_MSG ("listen failed errno(%d) '%s'.", errno, strerror (errno));
      return (ST_FAILURE);
    }

  for (;;)
    {
      accepter_ctx_t accepter_ctx = { .mutex = PTHREAD_MUTEX_INITIALIZER, .server_ctx = server_ctx, };

      TRACE_MSG ("Waiting for new client.");
      
      accepter_ctx.remote_addr_size = sizeof (accepter_ctx.remote);
      accepter_ctx.fd = TEMP_FAILURE_RETRY (accept (server_ctx->server_sock,
						    (struct sockaddr*)&accepter_ctx.remote,
						    &accepter_ctx.remote_addr_size));
      
      if (accepter_ctx.fd < 0)
	{
	  ERROR_MSG ("accept failed errno(%d) '%s'.", errno, strerror (errno));
	  if ((EBADF == errno) || (EINVAL == errno))
	    break;
	  else
	    continue;
	}

      TRACE_MSG ("New client from: %08x:%04x.", accepter_ctx.remote.sin_addr.s_addr, accepter_ctx.remote.sin_port);
      
      pthread_t id;
      pthread_attr_t attr;
      pthread_attr_init (&attr);
      pthread_attr_setdetachstate (&attr, PTHREAD_CREATE_DETACHED);
      
      pthread_mutex_init (&accepter_ctx.mutex, NULL);
      pthread_mutex_lock (&accepter_ctx.mutex);
      rv = pthread_create (&id, &attr, handle_client, &accepter_ctx);
      
      TRACE_MSG ("New thread for client returned %d.", rv);
      
      if (rv != 0)
	{
	  ERROR_MSG ("Failed to create thread for new client.");
	  shutdown (accepter_ctx.fd, SHUT_RDWR);
	  close (accepter_ctx.fd);
	  continue;
	}
      pthread_mutex_lock (&accepter_ctx.mutex);
      
      TRACE_MSG ("New client has started.");
    }

  return (ST_SUCCESS);
}

static status_t
create_server_socket (server_ctx_t * server_ctx)
{
  status_t status;
  
  server_ctx->server_sock = socket (PF_INET, SOCK_STREAM, IPPROTO_TCP);

  TRACE_MSG ("Created command socket %d.", server_ctx->server_sock);
  
  if (server_ctx->server_sock < 0)
    {
      ERROR_MSG ("socket failed errno(%d) '%s'.", errno, strerror (errno));
      return (ST_FAILURE);
    }

  status = run_accepter (server_ctx);
  
  shutdown (server_ctx->server_sock, SHUT_RDWR);
  close (server_ctx->server_sock);
  
  TRACE_MSG ("Server %d socket closed.", server_ctx->server_sock);
  
  return (status);
}

static mr_status_t
shutdown_server (const mr_ptr_t node, const void * context)
{
  server_t * server = node.ptr;
  shutdown (server->connection->cmd_fd, SHUT_RDWR);
  return (MR_SUCCESS);
}

status_t
run_server (config_t * config)
{
  server_ctx_t server_ctx;

  TRACE_MSG ("Start server.");
  
  memset (&server_ctx, 0, sizeof (server_ctx));
  server_ctx.config = config;
  
  sync_storage_init (&server_ctx.clients, server_compar, server_hash, NULL, "server_t", NULL);

  /* if one of the clients unexpectedly terminates server should ignore SIGPIPE */
  signal (SIGPIPE, SIG_IGN);

  status_t status = create_server_socket (&server_ctx);
  
  sync_storage_yeld (&server_ctx.clients, shutdown_server);
  sync_storage_free (&server_ctx.clients);
  
  TRACE_MSG ("Closed data socket. Exiting server.");
  
  return (status);
}
