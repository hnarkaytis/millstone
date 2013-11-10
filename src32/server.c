#define _GNU_SOURCE /* TEMP_FAILURE_RETRY */

#ifdef HAVE_CONFIG_H
#include <config.h>
#endif /* HAVE_CONFIG_H */
#include <millstone.h>
#include <logging.h>
#include <block.h>
#include <connection.h>
#include <file.h>
#include <file_meta.h>
#include <msg.h>
#include <sync_storage.h>
#include <llist.h>
#include <mtu_tune.h>
#include <server.h>

#include <stddef.h> /* size_t, ssize_t */
#include <signal.h> /* signal, SIG_IGN, SIGPIPE */
#include <unistd.h> /* TEMP_FAILURE_RETRY, close */
#include <inttypes.h> /* SCNx64 */
#include <string.h> /* memcpy, strerror */
#include <errno.h> /* errno */
#include <limits.h> /* CHAR_BIT */
#include <time.h> /* struct timespec */
#include <sys/time.h> /* struct timeval */
#include <sys/mman.h> /* mmap64, unmap */
#include <sys/socket.h> /* setsockopt */
#include <netinet/tcp.h> /* TCP_NODELAY */

#include <openssl/sha.h> /* SHA1 */
#include <pthread.h>
#ifdef HAVE_ZLIB
#include <zlib.h>
#endif /* HAVE_ZLIB */

TYPEDEF_STRUCT (timestamped_block_t,
		(block_id_t, block_id),
		(struct timeval, time),
		)

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

TYPEDEF_STRUCT (blocks_trasfer_t,
		(mtu_tune_t, mtu_tune),
		(sync_storage_t, req_blocks),
		(struct timeval, round_trip_time),
		(llist_t, delayed_blocks),
		(pthread_cond_t, cond),
		(pthread_mutex_t, mutex),
		(bool, cancel),
		)

TYPEDEF_STRUCT (ref_count_t,
		int count,
		(pthread_cond_t, cond),
		(pthread_mutex_t, mutex),
		)

TYPEDEF_STRUCT (server_t,
		(server_ctx_t *, server_ctx),
		(connection_t *, connection),
		(typeof (((config_t *)NULL)->compress_level), compress_level),
		(llist_t, cmd_out),
		(llist_t, task_queue),
		(blocks_trasfer_t, blocks_trasfer),
		(ref_count_t, ref_count),
		)

TYPEDEF_STRUCT (accepter_ctx_t,
		(server_ctx_t *, server_ctx),
		(struct sockaddr_in, remote),
		(socklen_t, remote_addr_size),
		int fd,
		(pthread_mutex_t, mutex),
		)

static status_t
timestamped_block_add (sync_storage_t * sync_storage, block_id_t * block_id)
{
  timestamped_block_t * element = MR_MALLOC (sizeof (*element));
  status_t status = ST_FAILURE;
  if (NULL == element)
    {
      FATAL_MSG ("Out of memory.");
      return (status);
    }
  element->block_id = *block_id;
  gettimeofday (&element->time, NULL);
  mr_ptr_t * find = sync_storage_add (sync_storage, element);
  if (NULL == find)
    MR_FREE (element);
  else
    status = ST_SUCCESS;
  
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
server_cancel (server_t * server)
{
  llist_cancel (&server->task_queue);
  llist_cancel (&server->cmd_out);

  file_chunks_cancel (server->connection->file);

  server->blocks_trasfer.cancel = TRUE;
  llist_cancel (&server->blocks_trasfer.delayed_blocks);
  pthread_cond_broadcast (&server->blocks_trasfer.cond);
}

static status_t
send_block_request (server_t * server, block_id_t * block_id)
{
  status_t status = ST_SUCCESS;
  msg_t msg;
  memset (&msg, 0, sizeof (msg));
  msg.msg_type = MT_BLOCK_REQUEST;
  
  for (msg.block_id.offset = block_id->offset;
       msg.block_id.offset < block_id->offset + block_id->size;
       msg.block_id.offset += msg.block_id.size)
    {
      msg.block_id.size = block_id->size - (msg.block_id.offset - block_id->offset);

      mtu_tune_set_size (&server->blocks_trasfer.mtu_tune, &msg.block_id);
      
      if (NULL == chunk_ref (server->connection->file, msg.block_id.offset))
	status = ST_FAILURE;
      if (ST_SUCCESS != status)
	break;
    
      status = timestamped_block_add (&server->blocks_trasfer.req_blocks, &msg.block_id);
      if (ST_SUCCESS != status)
	break;

      status = llist_push (&server->cmd_out, &msg);
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
				server->connection->file->fd,
				block_matched->duplicate_block_id.offset);
  
  DEBUG_MSG ("Got message that block 0x%" SCNx64 ":%" SCNx32 " is duplicated of block 0x%" SCNx64 ":%" SCNx32 ".",
	     block_matched->block_id.offset, block_matched->block_id.size,
	     block_matched->duplicate_block_id.offset, block_matched->duplicate_block_id.size);
  
  if (-1 == (long)src)
    FATAL_MSG ("Failed to map file into memory. Error (%d) %s.\n", errno, strerror (errno));
  else
    {
      void * dst = file_chunks_get_addr (server->connection->file, block_matched->block_id.offset);
      if (dst != NULL)
	{
	  memcpy (dst, src, block_matched->block_id.size);
	  status = chunk_unref (server->connection->file, block_matched->block_id.offset);
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
  
  if (!block_matched->matched)
    {
      if (block_matched->duplicate)
	status = copy_duplicate (server, block_matched);
      else
	{
	  if (block_matched->block_id.size <= MIN_BLOCK_SIZE)
	    status = send_block_request (server, &block_matched->block_id);
	  else
	    {
	      task_t task;
	      memset (&task, 0, sizeof (task));
	      task.block_id = block_matched->block_id;
	      for (task.size = MIN_BLOCK_SIZE;
		   task.size * SPLIT_RATIO < block_matched->block_id.size;
		   task.size *= SPLIT_RATIO);
      
	      DUMP_VAR (task_t, &task);

	      status = ST_FAILURE;
	      if (NULL != chunk_ref (server->connection->file, task.block_id.offset))
		status = llist_push (&server->task_queue, &task);
	    }
	}
    }
  
  if (ST_SUCCESS != chunk_unref (server->connection->file, block_matched->block_id.offset))
    status = ST_FAILURE;
  
  return (status);
}

static status_t
block_sent (server_t * server, block_id_t * block_id)
{
  status_t status = ST_SUCCESS;

  DEBUG_MSG ("Got confirmation for block 0x%" SCNx64 ":%" SCNx32 ".", block_id->offset, block_id->size);

  if (NULL != sync_storage_find (&server->blocks_trasfer.req_blocks, block_id, NULL))
    {
      timestamped_block_t timestamped_block;
      timestamped_block.block_id = *block_id;
      gettimeofday (&timestamped_block.time, NULL);
      status = llist_push (&server->blocks_trasfer.delayed_blocks, &timestamped_block);
    }

  return (status);
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
  msg.msg_type = MT_BLOCK_DIGEST;
  
  for (;;)
    {
      status_t status = llist_pop (&server->task_queue, &task);
      if (ST_SUCCESS != status)
	break;
      
      DEBUG_MSG ("Server worker got task: offset:size 0x%" SCNx64 ":%" SCNx32 " split on 0x%zx.",
		 task.block_id.offset, task.block_id.size, task.size);
      
      msg.block_id.size = task.size;
      for (offset = 0; offset < task.block_id.size; offset += msg.block_id.size)
	{
	  msg.block_id.offset = task.block_id.offset + offset;
	  if (msg.block_id.size > task.block_id.size - offset)
	    msg.block_id.size = task.block_id.size - offset;

	  DEBUG_MSG ("Calc digest for offset 0x%" SCNx64 " status %d.", msg.block_id.offset, status);

	  status = ST_FAILURE;
	  void * data = file_chunks_get_addr (server->connection->file, msg.block_digest.block_id.offset);
	  if (NULL == data)
	    break;
	  
	  SHA1 (data, msg.block_digest.block_id.size, (unsigned char*)&msg.block_digest.digest);

	  DEBUG_MSG ("Pushing to outgoing queue digest for offset 0x%" SCNx64 ".", msg.block_id.offset);
	  
	  status = llist_push (&server->cmd_out, &msg);
	  if (ST_SUCCESS != status)
	    break;

	  DEBUG_MSG ("Pushed digest for offset 0x%" SCNx64 ".", msg.block_id.offset);
	}
      
      if (ST_SUCCESS != status)
	break;
      
      status = chunk_unref (server->connection->file, task.block_id.offset);
      if (ST_SUCCESS != status)
	break;
    }
  
  DEBUG_MSG ("Exiting server worker.");
  
  return (NULL);
}

static void *
server_cmd_writer (void * arg)
{
  server_t * server = arg;
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
  
  server_cancel (server);

  DEBUG_MSG ("Exiting server command writer.");
  
  return (NULL);
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
	  DEBUG_MSG ("Client failed to send data block (offset 0x%" SCNx64 ":%" SCNx32 ").",
		     msg.block_id.offset, msg.block_id.size);
	case MT_BLOCK_SENT:
	  status = block_sent (server, &msg.block_id);
	  break;
	  
	default:
	  status = ST_FAILURE;
	  break;
	}
      
      if (ST_SUCCESS != status)
	break;
    }

  server_cancel (server);
  
  DEBUG_MSG ("Exiting server command reader thread.");
  
  return (NULL);
}

static status_t
chunk_file (server_t * server, status_t (*block_handler) (server_t *, block_id_t *))
{
  status_t status = ST_SUCCESS;
  off64_t offset = 0;
  msg_t msg;
  
  DEBUG_MSG ("Start chunking file.");

  memset (&msg, 0, sizeof (msg));
  msg.msg_type = MT_BLOCK_REF;
  
  for (;;)
    {
      chunk_t * chunk = chunk_ref (server->connection->file, offset);
      if (NULL == chunk)
	{
	  status = ST_FAILURE;	  
	  break;
	}

      DUMP_VAR (chunk_t, chunk);
      
      msg.block_id = chunk->block_id;
      status = llist_push (&server->cmd_out, &msg);
      if (ST_SUCCESS != status)
	break;
      
      status = block_handler (server, &chunk->block_id);
      if (ST_SUCCESS != status)
	break;
      
      status = chunk_unref (server->connection->file, offset);
      if (ST_SUCCESS != status)
	break;
      
      offset += chunk->block_id.size;
      
      if (offset >= server->connection->file->size)
	break;
    }

  if (ST_SUCCESS == status)
    file_chunks_finilize (server->connection->file);
  
  server_cancel (server);
  
  DEBUG_MSG ("Exiting file chunker.");
  
  return (status);
}

static status_t
push_task (server_t * server, block_id_t * block_id)
{
  task_t task;
  task.block_id = *block_id;
  task.size = block_id->size;

  if (NULL == chunk_ref (server->connection->file, task.block_id.offset))
    return (ST_FAILURE);
  return (llist_push (&server->task_queue, &task));
}

static status_t
task_producer (void * arg)
{
  return (chunk_file (arg, push_task));
}

static status_t
start_file_sync (void * arg)
{
  server_t * server = arg;
  status_t status = ST_SUCCESS;
  if (!server->connection->file->file_exists)
    status = chunk_file (server, send_block_request);
  else
    status = start_threads (server_worker, server->connection->file->config->workers_number, task_producer, server);
  shutdown (server->connection->cmd_fd, SD_BOTH); /* force shutdown of reader and writer */
  return (status);
}

static status_t
retry_splitted_block (server_t * server, block_id_t * block_id)
{
  status_t status = ST_SUCCESS;
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
  return (status);
}

static void *
delayed_blocks_handler (void * arg)
{
  server_t * server = arg;
  blocks_trasfer_t * blocks_trasfer = &server->blocks_trasfer;
  timestamped_block_t timestamped_block;

  memset (&timestamped_block, 0, sizeof (timestamped_block));
  
  for (;;)
    {
      status_t status = llist_pop (&blocks_trasfer->delayed_blocks, &timestamped_block);
      if (ST_SUCCESS != status)
	break;

      struct timeval tv_delay;
      timeradd (&timestamped_block.time, &blocks_trasfer->round_trip_time, &tv_delay);
      struct timespec ts_delay;
      TIMEVAL_TO_TIMESPEC (&tv_delay, &ts_delay);
      
      pthread_mutex_lock (&blocks_trasfer->mutex);
      pthread_cond_timedwait (&blocks_trasfer->cond, &blocks_trasfer->mutex, &ts_delay);
      pthread_mutex_unlock (&blocks_trasfer->mutex);

      if (blocks_trasfer->cancel)
	break;
      
      if (ST_SUCCESS != sync_storage_del (&blocks_trasfer->req_blocks, &timestamped_block.block_id, NULL))
	continue;

      mtu_tune_log (&blocks_trasfer->mtu_tune, timestamped_block.block_id.size, TRUE);
	  
      status = retry_splitted_block (server, &timestamped_block.block_id);
      if (ST_SUCCESS != status)
	break;

      status = chunk_unref (server->connection->file, timestamped_block.block_id.offset);
      if (ST_SUCCESS != status)
	break;
    }
  return (NULL);
}

static status_t
start_delayed_blocks_handler (void * server)
{
  return (start_threads (delayed_blocks_handler, 1, start_file_sync, server));
}

static status_t
start_cmd_writer (void * server)
{
  return (start_threads (server_cmd_writer, 1, start_delayed_blocks_handler, server));
}

void
server_chunk_release (chunk_t * chunk, void * context)
{
  server_t * server = context;
  msg_t msg;
  memset (&msg, 0, sizeof (msg));
  msg.msg_type = MT_BLOCK_UNREF;
  msg.block_id = chunk->block_id;
  llist_push (&server->cmd_out, &msg);
}

static void *
handle_client (void * arg)
{
  accepter_ctx_t * ctx = arg;
  accepter_ctx_t accepter_ctx = *ctx;
  pthread_mutex_unlock (&ctx->mutex);

  file_t file;
  memset (&file, 0, sizeof (file));
  file.config = accepter_ctx.server_ctx->config;
  file_chunks_init (&file, PROT_WRITE, MAP_SHARED);

  connection_t connection;
  memset (&connection, 0, sizeof (connection));
  connection.file = &file;
  connection.cmd_fd = accepter_ctx.fd;
  connection.remote.sin_addr = accepter_ctx.remote.sin_addr;
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
  
  file_chunks_set_release_handler (&file, server_chunk_release, &server);
  
  server.server_ctx = accepter_ctx.server_ctx;

  server.ref_count.count = 0;
  pthread_mutex_init (&server.ref_count.mutex, NULL);
  pthread_cond_init (&server.ref_count.cond, NULL);
  
  server.blocks_trasfer.round_trip_time.tv_sec = 0;
  server.blocks_trasfer.round_trip_time.tv_usec = 1000000L / 100;
  pthread_mutex_init (&server.blocks_trasfer.mutex, NULL);
  pthread_cond_init (&server.blocks_trasfer.cond, NULL);
  server.blocks_trasfer.cancel = FALSE;
  
  sync_storage_init (&server.blocks_trasfer.req_blocks,
		     block_id_compar, block_id_hash, block_id_free,
		     "timestamped_block_t", &server);
  mtu_tune_init (&server.blocks_trasfer.mtu_tune);
  
  LLIST_INIT (&server.blocks_trasfer.delayed_blocks, timestamped_block_t, -1);
  LLIST_INIT (&server.task_queue, task_t, -1);
  LLIST_INIT (&server.cmd_out, msg_t, 2 * EXPECTED_PACKET_SIZE / sizeof (msg_t));

  DEBUG_MSG ("Context for new client inited. Read file meta from client.");

  /* reads UDP port of remote into connection_t and opens file for write */
  status_t status = read_file_meta (&connection, &server.compress_level);

  DUMP_VAR (server_t, &server);
  
  if (ST_SUCCESS == status)
    {
      mr_ptr_t * find = sync_storage_add (&server.server_ctx->clients, &server);
      if (NULL == find)
	status = ST_FAILURE;
      
      DEBUG_MSG ("Added client context to registry. Return value %d.", status);
      
      if (ST_SUCCESS == status)
	status = start_threads (server_cmd_reader, 1, start_cmd_writer, &server);

      sync_storage_del (&server.server_ctx->clients, &server, NULL);
      pthread_mutex_lock (&server.ref_count.mutex);
      while (server.ref_count.count != 0)
	pthread_cond_wait (&server.ref_count.cond, &server.ref_count.mutex);
      pthread_mutex_unlock (&server.ref_count.mutex);
      close (file.fd);
    }
  
  shutdown (accepter_ctx.fd, SD_BOTH);
  close (accepter_ctx.fd);

  /* free allocated slots */
  llist_cancel (&server.cmd_out);
  llist_cancel (&server.task_queue);
  llist_cancel (&server.blocks_trasfer.delayed_blocks);
  
  sync_storage_free (&server.blocks_trasfer.req_blocks);

  file_chunks_free (&file);
  
  DEBUG_MSG ("Closed connection to client: %08x:%04x.", accepter_ctx.remote.sin_addr.s_addr, accepter_ctx.remote.sin_port);

  return (NULL);
}

static status_t
run_accepter (server_ctx_t * server_ctx)
{
  int reuse_addr = !0;
  struct linger linger_opt = { .l_onoff = 0, .l_linger = 0, };

  DEBUG_MSG ("Apply options on server command socket.");
  
  int rv = setsockopt (server_ctx->server_sock, SOL_SOCKET, SO_REUSEADDR, &reuse_addr, sizeof (reuse_addr));
  if (rv != 0)
    WARN_MSG ("Failed to set reuse option for socket with errno %d - %s.", errno, strerror (errno));
  rv = setsockopt (server_ctx->server_sock, SOL_SOCKET, SO_LINGER, &linger_opt, sizeof (linger_opt));
  if (rv != 0)
    WARN_MSG ("Failed set liner time for socket with errno %d - %s.", errno, strerror (errno));

  rv = bind (server_ctx->server_sock, (struct sockaddr *)&server_ctx->server_name, sizeof (server_ctx->server_name));
  
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
data_reader_wd (void * arg)
{
  server_ctx_t * server_ctx = arg;
  status_t status = create_server_socket (server_ctx);
  shutdown (server_ctx->data_sock, SD_BOTH);
  return (status);
}

static int
struct_timeval_compar (struct timeval * x, struct timeval * y)
{
  int cmp = (x->tv_sec > y->tv_sec) - (x->tv_sec < y->tv_sec);
  if (cmp)
    return (cmp);
  cmp = (x->tv_usec > y->tv_usec) - (x->tv_usec < y->tv_usec);
  return (cmp);
}

static void
calc_round_trip_time (mr_ptr_t found, mr_ptr_t orig, void * context)
{
  server_t * server = context;
  timestamped_block_t * timestamped_block = found.ptr;

  struct timeval now, round_trip_time;
  gettimeofday (&now, NULL);
  timersub (&now, &timestamped_block->time, &round_trip_time);

  if (struct_timeval_compar (&round_trip_time, &server->blocks_trasfer.round_trip_time) > 0)
    server->blocks_trasfer.round_trip_time = round_trip_time;
}

static status_t
put_data_block (server_t * server, unsigned char * buf, int buf_size)
{
  status_t status = ST_SUCCESS;
  block_id_t * block_id = (block_id_t *)buf;
  int size = buf_size - sizeof (*block_id);
  unsigned char * src = &buf[sizeof (*block_id)];

  if (size < 0)
    {
      ERROR_MSG ("Recieved block has invalid size %d.", size);
      return (ST_FAILURE);
    }

  /* unregister block in registry */
  if (ST_SUCCESS != sync_storage_del (&server->blocks_trasfer.req_blocks, block_id, calc_round_trip_time))
    return (ST_SUCCESS); /* got second duplicate */
  
  mtu_tune_log (&server->blocks_trasfer.mtu_tune, block_id->size, FALSE);
  
  /* get address for a block */
  void * dst = file_chunks_get_addr (server->connection->file, block_id->offset);

  /* unref initial ref */
  if (ST_SUCCESS != chunk_unref (server->connection->file, block_id->offset))
    status = ST_FAILURE;

  if (NULL == dst)
    return (ST_FAILURE);
  
  DEBUG_MSG ("Write block at offset 0x%" SCNx64 ":%" SCNx32 ".", block_id->offset, block_id->size);

#ifdef HAVE_ZLIB
  if (server->compress_level > 0)
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
      if (server->compress_level > 0)
	{
	  ERROR_MSG ("Got compressed data, but zlib is not available.");
	  status = ST_FAILURE;
	}
      if (size != block_id->size)
	{
	  ERROR_MSG ("Recieved block size mismatched target size (%d != %d).", size, block_id->size);
	  status = ST_FAILURE;
	}
      
      if (ST_SUCCESS == status)
	memcpy (dst, src, block_id->size);
    }
  
  /* unref block get_addr */
  if (ST_SUCCESS != chunk_unref (server->connection->file, block_id->offset))
    status = ST_FAILURE;
  
  DEBUG_MSG ("Write block done.");
  
  return (status);
}

static void
server_ref (mr_ptr_t found, mr_ptr_t orig, void * context)
{
  server_t * server = found.ptr;
  pthread_mutex_lock (&server->ref_count.mutex);
  ++server->ref_count.count;
  pthread_mutex_unlock (&server->ref_count.mutex);
}

static void
server_unref (server_t * server)
{
  pthread_mutex_lock (&server->ref_count.mutex);
  if (0 == --server->ref_count.count)
    pthread_cond_broadcast (&server->ref_count.cond);
  pthread_mutex_unlock (&server->ref_count.mutex);
}

static void *
server_data_reader (void * arg)
{
  server_ctx_t * server_ctx = arg;
  unsigned char buf[1 << (MAX_TRANSFER_BLOCK_SIZE_BITS + 1)];
  connection_t connection;
  server_t server;

  memset (buf, 0, sizeof (buf));
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
      if (0 == rv)
	break;
      
      DEBUG_MSG ("Recieved packet in data reader. Sender: %08x:%04x. Packet size %d.",
		addr.addr_in.sin_addr.s_addr, addr.addr_in.sin_port, rv);

      if (rv < 0)
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
      mr_ptr_t * find = sync_storage_find (&server_ctx->clients, &server, server_ref);
      if (NULL == find)
	{
	  DEBUG_MSG ("Unknown or disconnected client.");
	  continue;
	}
      
      DEBUG_MSG ("Data reader identified client and initiated write.");
      
      put_data_block (find->ptr, buf, rv);

      server_unref (find->ptr);
    }
  return (NULL);
}

static mr_status_t
shutdown_server (const mr_ptr_t node, const void * context)
{
  server_t * server = node.ptr;
  shutdown (server->connection->cmd_fd, SD_BOTH);
  return (MR_SUCCESS);
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

  /* if one of the clients unexpectedly terminates server should ignore SIGPIPE */
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
    status = start_threads (server_data_reader, config->workers_number, data_reader_wd, &server_ctx);
  else
    ERROR_MSG ("bind failed errno(%d) '%s'.", errno, strerror (errno));
  
  close (server_ctx.data_sock);
  sync_storage_yeld (&server_ctx.clients, shutdown_server);
  sync_storage_free (&server_ctx.clients);
  
  DEBUG_MSG ("Closed data socket. Exiting server.");
  
  return (status);
}
