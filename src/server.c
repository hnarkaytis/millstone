#ifdef HAVE_CONFIG_H
#include <config.h>
#endif /* HAVE_CONFIG_H */
#include <millstone.h>
#include <logging.h>
#include <block.h>
#include <file_meta.h>
#include <msg.h>
#include <sync_storage.h>
#include <llist.h>
#include <mmap_mng.h>
#include <mtu_tune.h>
#include <server.h>

#include <stddef.h> /* size_t, ssize_t */
#include <signal.h> /* signal, SIG_IGN, SIGPIPE */
#include <unistd.h> /* TEMP_FAILURE_RETRY, close, ftruncate64 */
#include <string.h> /* memcpy, strerror */
#include <errno.h> /* errno */
#include <sys/mman.h> /* mmap64, unmap */
#include <sys/socket.h> /* setsockopt */
#include <netinet/tcp.h> /* TCP_NODELAY */

#include <openssl/sha.h> /* SHA1 */
#include <pthread.h>
#ifdef HAVE_ZLIB
#include <zlib.h>
#endif /* HAVE_ZLIB */

TYPEDEF_STRUCT (task_t,
		(block_id_t, block_id),
		(size_t, size),
		)

TYPEDEF_STRUCT (server_ctx_t,
		(config_t *, config),
		int server_sock,
		int data_sock,
		(sync_storage_t, clients),
		(struct sockaddr_in, server_name),
		)

TYPEDEF_STRUCT (server_t,
		(connection_t *, connection),
		int compress_level,
		(llist_t, cmd_out),
		(llist_t, task_queue),
		(sync_storage_t, data_blocks),
		(mtu_tune_t, mtu_tune),
		(server_ctx_t *, server_ctx),
		int tip,
		(pthread_cond_t, tip_cond),
		(pthread_mutex_t, tip_mutex),
		)

TYPEDEF_STRUCT (accepter_ctx_t,
		(server_ctx_t *, server_ctx),
		(struct sockaddr_in, remote),
		(socklen_t, remote_addr_size),
		int fd,
		(pthread_mutex_t, mutex),
		)

int
block_id_compar (const mr_ptr_t x, const mr_ptr_t y, const void * null)
{
  block_id_t * x_ = x.ptr;
  block_id_t * y_ = y.ptr;
  int cmp = (x_->offset > y_->offset) - (x_->offset < y_->offset);
  if (cmp)
    return (cmp);
  cmp = (x_->size > y_->size) - (x_->size < y_->size);
  return (cmp);
}

mr_hash_value_t
block_id_hash (const mr_ptr_t x, const void * null)
{
  block_id_t * x_ = x.ptr;
  return (x_->offset / MIN_TRANSFER_BLOCK_SIZE);
}

void
block_id_free (const mr_ptr_t x, const void * null)
{
  MR_FREE (x.ptr);
}

status_t
block_id_register (sync_storage_t * sync_storage, block_id_t * block_id)
{
  block_id_t * alloc_block_id = MR_MALLOC (sizeof (*alloc_block_id));
  if (NULL == alloc_block_id)
    {
      FATAL_MSG ("Out of memory.");
      return (ST_FAILURE);
    }
  *alloc_block_id = *block_id;
  status_t status = sync_storage_add (sync_storage, alloc_block_id);
  if (ST_SUCCESS != status)
    MR_FREE (alloc_block_id);
  return (status);
}

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
tip_inc (server_t * server)
{
  pthread_mutex_lock (&server->tip_mutex);
  ++server->tip;
  pthread_mutex_unlock (&server->tip_mutex);
}

static void
tip_dec (server_t * server)
{
  pthread_mutex_lock (&server->tip_mutex);
  if (--server->tip < MAX_TIP)
    pthread_cond_broadcast (&server->tip_cond);
  pthread_mutex_unlock (&server->tip_mutex);
}

static void
tip_cancel (server_t * server)
{
  pthread_mutex_lock (&server->tip_mutex);
  server->tip = 0;
  pthread_cond_broadcast (&server->tip_cond);
  pthread_mutex_unlock (&server->tip_mutex);
}

static void
tip_wait_low (server_t * server, int max_tip)
{
  pthread_mutex_lock (&server->tip_mutex);
  while (server->tip > max_tip)
    pthread_cond_wait (&server->tip_cond, &server->tip_mutex);
  pthread_mutex_unlock (&server->tip_mutex);
}

static status_t
task_push (server_t * server, task_t * task)
{
  tip_inc (server);
  status_t status = llist_push (&server->task_queue, task);
  if (ST_SUCCESS != status)
    tip_dec (server);
  return (status);
}

static status_t
msg_push (server_t * server, msg_t * msg)
{
  tip_inc (server);
  status_t status = llist_push (&server->cmd_out, msg);
  if (ST_SUCCESS != status)
    tip_dec (server);
  return (status);
}

static void
send_terminate (server_t * server)
{
  msg_t msg;
  memset (&msg, 0, sizeof (msg));
  msg.msg_type = MT_TERMINATE;
  msg_push (server, &msg);
}

static status_t
send_block_request (server_t * server, block_id_t * block_id)
{
  status_t status = ST_SUCCESS;
  msg_t msg;
  memset (&msg, 0, sizeof (msg));
  msg.msg_type = MT_BLOCK_REQUEST;
  
  DUMP_VAR (msg_t, &msg);
  
  for (msg.block_id.offset = block_id->offset;
       msg.block_id.offset < block_id->offset + block_id->size;
       msg.block_id.offset += msg.block_id.size)
    {
      msg.block_id.size = block_id->size - (msg.block_id.offset - block_id->offset);

      mtu_tune_set_size (&server->mtu_tune, &msg.block_id);
      
      status = block_id_register (&server->data_blocks, &msg.block_id);
      if (ST_SUCCESS != status)
	break;
      
      status = msg_push (server, &msg);
      if (ST_SUCCESS != status)
	break;
    }

  DEBUG_MSG ("Block requested with status %d.", status);
  
  return (status);
}

static status_t
copy_duplicate (server_t * server, block_matched_t * block_matched)
{
  status_t status = ST_FAILURE;
  unsigned char * src = mmap64 (NULL, block_matched->duplicate_block_id.size,
				PROT_READ, MAP_PRIVATE,
				server->connection->context->file_fd,
				block_matched->duplicate_block_id.offset);
  
  DEBUG_MSG ("Got message that block %zd:%zd is duplicated of block %zd:%zd.",
	     block_matched->block_id.offset, block_matched->block_id.size,
	     block_matched->duplicate_block_id.offset, block_matched->duplicate_block_id.size);
  
  if (-1 == (long)src)
    FATAL_MSG ("Failed to map file into memory. Error (%d) %s.\n", errno, strerror (errno));
  else
    {
      unsigned char * dst = mmap_mng_get_addr (server->connection->context, &block_matched->block_id);
      if (dst != NULL)
	{
	  memcpy (dst, src, block_matched->block_id.size);
	  mmap_mng_unref (&server->connection->context->mmap_mng, &block_matched->block_id);
	  status = ST_SUCCESS;
	}
      if (0 != munmap (src, block_matched->duplicate_block_id.size))
	ERROR_MSG ("Failed to unmap memory. Error (%d) %s.\n", errno, strerror (errno));
    }
  
  return (status);
}

static status_t
block_matched (server_t * server, block_matched_t * block_matched)
{
  status_t status = ST_SUCCESS;
  
  DUMP_VAR (block_matched_t, block_matched);
  
  if (block_matched->matched)
    return (status);
  
  if (block_matched->block_id.size <= MIN_BLOCK_SIZE)
    {
      if (block_matched->duplicate && (block_matched->block_id.size == block_matched->duplicate_block_id.size))
	status = copy_duplicate (server, block_matched);
      else
	status = send_block_request (server, &block_matched->block_id);
    }
  else
    {
      task_t task;
      memset (&task, 0, sizeof (task));
      task.block_id = block_matched->block_id;
      for (task.size = MIN_BLOCK_SIZE;
	   task.size * SPLIT_RATIO < block_matched->block_id.size;
	   task.size *= SPLIT_RATIO);
      
      DUMP_VAR (task_t, &task);
      
      status = task_push (server, &task);
      
      DEBUG_MSG ("Task pushed to queue.");
    }
  return (status);
}

static status_t
block_sent (server_t * server, block_id_t * block_id)
{
  status_t status = ST_SUCCESS;
  bool failure = (NULL != sync_storage_find (&server->data_blocks, block_id));
  
  DEBUG_MSG ("Got confirmation for block %zd:%zd.", block_id->offset, block_id->size);
  
  mtu_tune_log (&server->mtu_tune, block_id->size, failure);
  if (failure)
    {
      DEBUG_MSG ("Packet lost for offset 0x%zx.", block_id->offset);

      sync_storage_del (&server->data_blocks, block_id);
      
#ifdef HAVE_ZLIB
      if (server->compress_level > 0)
	{
	  /* we need to split lost packets approximetly half-and-half */
	  size_t size = block_id->size;
	  int size_width = mtu_tune_get_width (size);
	  /* standard packets will have size of 2^N and
	     will be splitted in two equal parts.
	     Last block of file might have arbitrary size. */
	  if (size - (1 << (size_width - 1)) <= (1 << size_width))
	    --size_width;
	  /* do not split on blocks less then minimal size */
	  if (size_width < MIN_TRANSFER_BLOCK_SIZE_BITS)
	    size_width = MIN_TRANSFER_BLOCK_SIZE_BITS;
	  block_id->size = 1 << size_width;
	  /* send request for the first part */
	  status = send_block_request (server, block_id);
	  /* send request for the rest */
	  if ((ST_SUCCESS == status) && (size > (1 << size_width)))
	    {
	      block_id->offset += 1 << size_width;
	      block_id->size = size - (1 << size_width);
	      status = send_block_request (server, block_id);
	    }
	}
      else
#endif /* HAVE_ZLIB */
	status = send_block_request (server, block_id);
    }
  return (status);
}

static void *
server_cmd_reader (void * arg)
{
  server_t * server = arg;

  DEBUG_MSG ("Started server command reader.");
  
  for (;;)
    {
      msg_t msg;
      status_t status = msg_recv (server->connection->cmd_fd, &msg);

      if (ST_SUCCESS != status)
	break;

      DUMP_VAR (msg_t, &msg);
      
      switch (msg.msg_type)
	{
	case MT_BLOCK_MATCHED:
	  status = block_matched (server, &msg.block_matched);
	  break;
	case MT_BLOCK_SEND_ERROR:
	  ERROR_MSG ("Client failed to send data block (offset %zd size %zd).",
		     msg.block_id.offset, msg.block_id.size);
	case MT_BLOCK_SENT:
	  status = block_sent (server, &msg.block_id);
	  break;
	default:
	  status = ST_FAILURE;
	  break;
	}
      tip_dec (server);
      
      if (ST_SUCCESS != status)
	break;
    }

  llist_cancel (&server->cmd_out);
  DEBUG_MSG ("Exiting server command reader thread.");
  
  return (NULL);
}

static void *
server_worker (void * arg)
{
  server_t * server = arg;
  task_t task;
  msg_t msg;
  off64_t offset;

  DEBUG_MSG ("Started server worker.");
  
  memset (&msg, 0, sizeof (msg));
  for (;;)
    {
      status_t status = llist_pop (&server->task_queue, &task);
      if (ST_SUCCESS != status)
	break;
      
      DEBUG_MSG ("Server worker got task: offset:size %zd:%zd split on %zd.",
		 task.block_id.offset, task.block_id.size, task.size);
      
      msg.msg_type = MT_BLOCK_DIGEST;
      msg.block_id.size = task.size;
      for (offset = 0; offset < task.block_id.size; offset += msg.block_id.size)
	{
	  msg.block_id.offset = task.block_id.offset + offset;
	  if (msg.block_id.size > task.block_id.size - offset)
	    msg.block_id.size = task.block_id.size - offset;

	  DEBUG_MSG ("Calc digest for offset %zd status %d.", msg.block_id.offset, status);
	  
	  unsigned char * data = mmap_mng_get_addr (server->connection->context, &msg.block_digest.block_id);
	  if (NULL == data)
	    break;
	  
	  SHA1 (data, msg.block_digest.block_id.size, (unsigned char*)&msg.block_digest.digest);
	  mmap_mng_unref (&server->connection->context->mmap_mng, &msg.block_digest.block_id);

	  DEBUG_MSG ("Pushing to outgoing queue digest for offset %zd.", msg.block_id.offset);
	  
	  status = msg_push (server, &msg);
	  if (ST_SUCCESS != status)
	    break;
	  
	  DEBUG_MSG ("Pushed digest for offset %zd.", msg.block_id.offset);
	}
      
      tip_dec (server);
    }
  
  DEBUG_MSG ("Exiting server worker.");
  
  return (NULL);
}

static status_t
server_cmd_writer (server_t * server)
{
  status_t status = ST_SUCCESS;
  char buf[EXPECTED_PACKET_SIZE];
  
  DEBUG_MSG ("Started server command writer.");
  
  memset (buf, 0, sizeof (buf));
  for (;;)
    {
      size_t buf_size = sizeof (buf);
      status = llist_pop_bulk (&server->cmd_out, buf, &buf_size);
      if (ST_SUCCESS != status)
	break;
      
      DEBUG_MSG ("Write %d bytes to client %08x:%04x.", buf_size,
		 server->connection->remote.sin_addr.s_addr, server->connection->remote.sin_port);

      status = msg_send (server->connection->cmd_fd, buf, buf_size);
      if (status != ST_SUCCESS)
	break;
    }
  
  DEBUG_MSG ("Exiting server command writer.");
  
  return (status);
}

static status_t
start_workers (server_t * server)
{
  int i;
  pthread_t ids[server->connection->context->config->workers_number];
  status_t status = ST_FAILURE;

  DEBUG_MSG ("Start server workers %d.", server->connection->context->config->workers_number);
  
  for (i = 0; i < server->connection->context->config->workers_number; ++i)
    {
      int rv = pthread_create (&ids[i], NULL, server_worker, server);
      if (rv != 0)
	break;
    }

  DEBUG_MSG ("Started %d.", i);
  
  if (i > 0)
    status = server_cmd_writer (server);

  DEBUG_MSG ("Canceling server workers.");
  
  llist_cancel (&server->task_queue);
  llist_cancel (&server->cmd_out);
  for (--i ; i >= 0; --i)
    pthread_join (ids[i], NULL);
  
  DEBUG_MSG ("Server workers canceled.");
  
  return (status);
}

static void *
data_retrieval (void * arg)
{
  server_t * server = arg;
  status_t status = ST_SUCCESS;
  block_id_t block_id;
  
  DEBUG_MSG ("Data retrieval thread has started.");
  
  memset (&block_id, 0, sizeof (block_id));
  block_id.size = MAX_BLOCK_SIZE;

  for (block_id.offset = 0;
       block_id.offset < server->connection->context->size;
       block_id.offset += block_id.size)
    {
      if (block_id.size > server->connection->context->size - block_id.offset)
	block_id.size = server->connection->context->size - block_id.offset;
      
      status = send_block_request (server, &block_id);
      if (ST_SUCCESS != status)
	break;

      tip_wait_low (server, MAX_TIP);
    }

  tip_wait_low (server, 0);
  if (ST_SUCCESS == status)
    send_terminate (server);
  
  DEBUG_MSG ("Exiting data retrieval thread.");
  
  return (NULL);
}

static status_t
start_data_retrieval (server_t * server)
{
  pthread_t id;
  status_t status;
  int rv = pthread_create (&id, NULL, data_retrieval, server);
  if (rv != 0)
    {
      ERROR_MSG ("Failed to start data retrieval thread.");
      return (ST_FAILURE);
    }
  
  DEBUG_MSG ("Start data retrieval. Thread created %d.", rv);

  status = server_cmd_writer (server);

  DEBUG_MSG ("Canceling data retrieval thread.");

  llist_cancel (&server->cmd_out);
  tip_cancel (server);
  pthread_join (id, NULL);
  
  DEBUG_MSG ("Data retrieval thread canceled.");
  
  return (status);
}

static void *
task_producer (void * arg)
{
  server_t * server = arg;
  status_t status = ST_SUCCESS;
  task_t task;
  
  DEBUG_MSG ("Tasks producer thread has started.");

  memset (&task, 0, sizeof (task));
  task.block_id.size = MAX_BLOCK_SIZE;
  task.size = MAX_BLOCK_SIZE;
  
  for (task.block_id.offset = 0;
       task.block_id.offset < server->connection->context->size;
       task.block_id.offset += task.block_id.size)
    {
      if (task.block_id.size > server->connection->context->size - task.block_id.offset)
	task.block_id.size = server->connection->context->size - task.block_id.offset;

      status = task_push (server, &task);
      if (ST_SUCCESS != status)
	break;

      tip_wait_low (server, MAX_TIP);
    }

  tip_wait_low (server, 0);
  if (ST_SUCCESS == status)
    send_terminate (server);
  
  DEBUG_MSG ("Exiting data retrieval thread.");
  
  return (NULL);
}

static status_t
start_task_producer (server_t * server)
{
  pthread_t id;
  int rv = pthread_create (&id, NULL, task_producer, server);
  if (rv != 0)
    {
      ERROR_MSG ("Failed to start tasks producer thread.");
      return (ST_FAILURE);
    }

  status_t status = start_workers (server);

  DEBUG_MSG ("Canceling tasks producer thread.");
  
  llist_cancel (&server->task_queue);
  tip_cancel (server);
  pthread_join (id, NULL);
  
  DEBUG_MSG ("Tasks producer thread canceled.");
  
  return (status);
}

static status_t
start_cmd_reader (server_t * server)
{
  pthread_t id;
  status_t status;
  int rv = pthread_create (&id, NULL, server_cmd_reader, server);
  
  if (rv != 0)
    {
      ERROR_MSG ("Failed to start command reader thread.");
      return (ST_FAILURE);
    }
  
  DEBUG_MSG ("Satart command reader. Thread create returned %d.", rv);
  DEBUG_MSG ("File on server status %d.", server->connection->context->file_exists);
  
  if (!server->connection->context->file_exists)
    status = start_data_retrieval (server);
  else
    status = start_task_producer (server);

  DEBUG_MSG ("Canceling command reader thread.");

  llist_cancel (&server->cmd_out);
  llist_cancel (&server->task_queue);
  pthread_join (id, NULL);
  
  DEBUG_MSG ("Command reader thread canceled.");
  
  return (status);
}

static void *
handle_client (void * arg)
{
  accepter_ctx_t * ctx = arg;
  accepter_ctx_t accepter_ctx = *ctx;
  pthread_mutex_unlock (&ctx->mutex);

  context_t context;
  memset (&context, 0, sizeof (context));
  context.config = accepter_ctx.server_ctx->config;
  mmap_mng_init (&context.mmap_mng, PROT_WRITE, MAP_SHARED);

  connection_t connection;
  memset (&connection, 0, sizeof (connection));
  connection.context = &context;
  connection.cmd_fd = accepter_ctx.fd;
  connection.remote.sin_addr = accepter_ctx.remote.sin_addr;
  bool tcp_nodelay = TRUE;
  setsockopt (connection.cmd_fd, SOL_TCP, TCP_NODELAY, &tcp_nodelay, sizeof (tcp_nodelay));
  
  server_t server;
  memset (&server, 0, sizeof (server));
  server.connection = &connection;
  server.tip = 0;
  pthread_mutex_init (&server.tip_mutex, NULL);
  pthread_cond_init (&server.tip_cond, NULL);
  
  server.server_ctx = accepter_ctx.server_ctx;
  
  sync_storage_init (&server.data_blocks, block_id_compar, block_id_hash, block_id_free, "block_id_t", NULL);
  mtu_tune_init (&server.mtu_tune);
  
  LLIST_INIT (&server.cmd_out, msg_t);
  LLIST_INIT (&server.task_queue, task_t);

  DEBUG_MSG ("Context for new client inited. Read file meta from client.");
  
  status_t status = read_file_meta (&connection); /* reads UDP port of remote into connection_t and opens file for write */

  if (ST_SUCCESS == status)
    {
      status = sync_storage_add (&server.server_ctx->clients, &server);
      
      DEBUG_MSG ("Added client context to registry. Return value %d.", status);
      
      if (ST_SUCCESS == status)
	status = start_cmd_reader (&server);

      sync_storage_del (&server.server_ctx->clients, &server);
      mmap_mng_free (&context.mmap_mng);
      close (context.file_fd);
    }
  
  shutdown (accepter_ctx.fd, SD_BOTH);
  close (accepter_ctx.fd);

  /* free allocated slots */
  llist_cancel (&server.task_queue);
  llist_cancel (&server.cmd_out);
  
  sync_storage_free (&server.data_blocks);

  DEBUG_MSG ("Closed connection to client: %08x:%04x.", accepter_ctx.remote.sin_addr.s_addr, accepter_ctx.remote.sin_port);

  return (NULL);
}

static status_t
run_accepter (server_ctx_t * server_ctx)
{
  bool reuse_addr = TRUE;
  struct linger linger_opt = { .l_onoff = 1, .l_linger = 1, };

  DEBUG_MSG ("Apply options on server command socket.");
  setsockopt (server_ctx->server_sock, SOL_SOCKET, SO_REUSEADDR, &reuse_addr, sizeof (reuse_addr));
  setsockopt (server_ctx->server_sock, SOL_SOCKET, SO_LINGER, &linger_opt, sizeof (linger_opt));

  int rv = bind (server_ctx->server_sock, (struct sockaddr *)&server_ctx->server_name, sizeof (server_ctx->server_name));
  
  DEBUG_MSG ("Binding server command socket. Return value %d.", rv);
  
  if (rv < 0)
    {
      ERROR_MSG ("bind failed errno(%d) '%s'.", errno, strerror (errno));
      return (ST_FAILURE);
    }

  rv = listen (server_ctx->server_sock, 1);
  
  DEBUG_MSG ("Set incoming queue size for server command socket. Return value %d.", rv);
  
  if (rv < 0)
    {
      ERROR_MSG ("listen failed errno(%d) '%s'.", errno, strerror (errno));
      return (ST_FAILURE);
    }

  for (;;)
    {
      accepter_ctx_t accepter_ctx = { .mutex = PTHREAD_MUTEX_INITIALIZER, .server_ctx = server_ctx, };

      DEBUG_MSG ("Waiting for new client.");
      
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

      DEBUG_MSG ("New client from: %08x:%04x.", accepter_ctx.remote.sin_addr.s_addr, accepter_ctx.remote.sin_port);
      
      pthread_t id;
      pthread_attr_t attr;
      pthread_attr_init (&attr);
      pthread_attr_setdetachstate (&attr, PTHREAD_CREATE_DETACHED);
      
      pthread_mutex_init (&accepter_ctx.mutex, NULL);
      pthread_mutex_lock (&accepter_ctx.mutex);
      rv = pthread_create (&id, &attr, handle_client, &accepter_ctx);
      
      DEBUG_MSG ("New thread for client returned %d.", rv);
      
      if (rv != 0)
	{
	  ERROR_MSG ("Failed to create thread for new client.");
	  shutdown (accepter_ctx.fd, SD_BOTH);
	  close (accepter_ctx.fd);
	  continue;
	}
      pthread_mutex_lock (&accepter_ctx.mutex);
      
      DEBUG_MSG ("New client has started.");
    }

  return (ST_SUCCESS);
}

static status_t
create_server_socket (server_ctx_t * server_ctx)
{
  status_t status;
  server_ctx->server_sock = socket (PF_INET, SOCK_STREAM, IPPROTO_TCP);
  
  DEBUG_MSG ("Created command socket %d.", server_ctx->server_sock);
  
  if (server_ctx->server_sock < 0)
    {
      ERROR_MSG ("socket failed errno(%d) '%s'.", errno, strerror (errno));
      return (ST_FAILURE);
    }

  status = run_accepter (server_ctx);
  
  shutdown (server_ctx->server_sock, SD_BOTH);
  close (server_ctx->server_sock);
  
  DEBUG_MSG ("Server socket closed.");
  
  return (status);
}

static status_t
put_data_block (server_t * server, unsigned char * buf, int buf_size)
{
  status_t status = ST_SUCCESS;
  block_id_t * block_id = (block_id_t *)buf;
  typeof (server->connection->context->config->compress_level) * compress_level = (void*)&buf[sizeof (*block_id)];
  int size = buf_size - sizeof (*block_id) - sizeof (*compress_level);
  unsigned char * src = &buf[sizeof (*block_id) + sizeof (*compress_level)];
  mr_ptr_t * find = sync_storage_find (&server->data_blocks, block_id);
  if (find == NULL) /* got second duplicate */
    return (ST_SUCCESS);
  
  DEBUG_MSG ("Write block at offset 0x%zx size %zd.", block_id->offset, block_id->size);

  if (size < 0)
    {
      ERROR_MSG ("Recieved block is too small.");
      return (ST_FAILURE);
    }

  /* set compress level into clients properties */
  server->compress_level = *compress_level;
  /* unregister block in registry */
  sync_storage_del (&server->data_blocks, block_id);

  /* get address for block and release temporary lock */
  unsigned char * dst = mmap_mng_get_addr (server->connection->context, block_id);

  if (NULL == dst)
    return (ST_FAILURE);
  
#ifdef HAVE_ZLIB
  if (*compress_level > 0)
    {
      uLong length = block_id->size;
      int z_status = uncompress (dst, &length, src, size);
      if (Z_OK != z_status)
	{
	  ERROR_MSG ("Failed to uncompressed recieved block.");
	  status = ST_FAILURE;
	}
      if (length != block_id->size)
	{
	  ERROR_MSG ("Uncompressed block size mismatched target size.");
	  status = ST_FAILURE;
	}
    }
  else
#endif /* HAVE_ZLIB */
    
    {
      if (*compress_level > 0)
	{
	  ERROR_MSG ("Got compressed data, but zlib is not available.");
	  status = ST_FAILURE;
	}
      if (size != block_id->size)
	{
	  ERROR_MSG ("Recieved block size mismatched target size (%d != %d).", size, block_id->size);
	  status = ST_FAILURE;
	}
      memcpy (dst, src, block_id->size);
    }

  mmap_mng_unref (&server->connection->context->mmap_mng, block_id);
  
  DEBUG_MSG ("Write block done.");
  
  return (status);
}

static void *
server_data_reader (void * arg)
{
  server_ctx_t * server_ctx = arg;
  unsigned char buf[1 << (MAX_TRANSFER_BLOCK_SIZE_BITS + 1)];
  connection_t connection;
  server_t server;

  memset (&connection, 0, sizeof (connection));
  memset (&server, 0, sizeof (server));
  server.connection = &connection;

  DEBUG_MSG ("Start main loop in data reader.");
  
  for (;;)
    {
      union {
	struct sockaddr _addr;
	struct sockaddr_in addr_in;
      } addr;
      socklen_t addr_len = sizeof (addr);
      int rv = recvfrom (server_ctx->data_sock, buf, sizeof (buf), 0, &addr._addr, &addr_len);
      
      DEBUG_MSG ("Recieved packet in data reader. Sender: %08x:%04x. Packet size %d.",
		addr.addr_in.sin_addr.s_addr, addr.addr_in.sin_port, rv);

      if (rv <= 0)
	{
	  ERROR_MSG ("Failed to recieve UDP packet.");
	  continue;
	}
      if (addr_len != sizeof (addr.addr_in))
	{
	  ERROR_MSG ("Got UDP packet from unknown type of address.");
	  continue;
	}
      connection.remote = addr.addr_in;
      mr_ptr_t * find = sync_storage_find (&server_ctx->clients, &server);
      if (NULL == find)
	{
	  DEBUG_MSG ("Unknown or disconnected client.");
	  continue;
	}
      
      DEBUG_MSG ("Data reader identified client and initiated write.");
      
      put_data_block (find->ptr, buf, rv);
    }
  return (NULL);
}

static status_t
start_data_readers (server_ctx_t * server_ctx)
{
  int i;
  pthread_t ids[server_ctx->config->workers_number];
  status_t status = ST_FAILURE;

  DEBUG_MSG ("Starting %d data readers.", server_ctx->config->workers_number);
  
  for (i = 0; i < server_ctx->config->workers_number; ++i)
    {
      int rv = pthread_create (&ids[i], NULL, server_data_reader, server_ctx);
      if (rv != 0)
	break;
    }

  DEBUG_MSG ("Started %d data readers.", i);
  
  if (i > 0)
    status = create_server_socket (server_ctx);

  DEBUG_MSG ("Canceling and joining data readers.");
  
  for (--i ; i >= 0; --i)
    {
      pthread_cancel (ids[i]);
      pthread_join (ids[i], NULL);
    }
  
  DEBUG_MSG ("Exiting data readers starter.");
  
  return (status);
}

status_t
run_server (config_t * config)
{
  server_ctx_t server_ctx;

  DEBUG_MSG ("Start server.");
  
  memset (&server_ctx, 0, sizeof (server_ctx));
  server_ctx.config = config;
  
  server_ctx.server_name.sin_family = AF_INET;
  server_ctx.server_name.sin_port = htons (config->listen_port);
  server_ctx.server_name.sin_addr.s_addr = htonl (INADDR_ANY);

  sync_storage_init (&server_ctx.clients, server_compar, server_hash, NULL, "server_t", NULL);

  /* if one of the clients unexpectedly terminates
     server should ignore SIGPIPE */
  signal (SIGPIPE, SIG_IGN);

  server_ctx.data_sock = socket (PF_INET, SOCK_DGRAM, IPPROTO_UDP);
  
  DEBUG_MSG ("Created data socket %d.", server_ctx.data_sock);
  
  if (server_ctx.data_sock < 0)
    {
      ERROR_MSG ("socket failed errno(%d) '%s'.", errno, strerror (errno));
      return (ST_FAILURE);
    }

  status_t status = ST_FAILURE;
  int rv = bind (server_ctx.data_sock, (struct sockaddr *)&server_ctx.server_name, sizeof (server_ctx.server_name));
  
  DEBUG_MSG ("Binded data socket. Return value: %d.", rv);
  
  if (0 == rv)
    status = start_data_readers (&server_ctx);
  else
    ERROR_MSG ("bind failed errno(%d) '%s'.", errno, strerror (errno));
  
  close (server_ctx.data_sock);
  sync_storage_free (&server_ctx.clients);
  
  DEBUG_MSG ("Closed data socket. Exiting server.");
  
  return (status);
}
