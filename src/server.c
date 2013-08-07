#include <millstone.h>
#include <logging.h>
#include <block.h>
#include <file_meta.h>
#include <queue.h>
#include <msg.h>
#include <calc_digest.h>
#include <sync_storage.h>
#include <server.h>

#include <unistd.h> /* TEMP_FAILURE_RETRY, sysconf, close, ftruncate64 */
#include <string.h> /* memcpy, strerror */
#include <errno.h> /* errno */
#include <sys/user.h> /* PAGE_SIZE */
#include <sys/mman.h> /* mmap64, unmap */

#include <pthread.h>

#define DATA_READERS (4)

#define SPLIT_RATIO (128)
#define MIN_BLOCK_SIZE (PAGE_SIZE)
#define MAX_BLOCK_SIZE (MIN_BLOCK_SIZE * SPLIT_RATIO * SPLIT_RATIO)

TYPEDEF_STRUCT (task_t,
		(block_id_t, block_id),
		(size_t, size),
		)

TYPEDEF_STRUCT (task_queue_t,
		(queue_t, queue),
		RARRAY (task_t, array),
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
		(msg_queue_t, cmd_out),
		(task_queue_t, task_queue),
		(sync_storage_t, data_blocks),
		(server_ctx_t *, server_ctx)
		)

TYPEDEF_STRUCT (accepter_ctx_t,
		(server_ctx_t *, server_ctx),
		(struct sockaddr_in, remote),
		(socklen_t, remote_addr_size),
		int fd,
		(pthread_mutex_t, mutex),
		)

static int
offset_key_compar (const long x, const long y, const void * null)
{
  return ((x > y) - (y > x));
}

static mr_hash_value_t
offset_key_hash (const long x, const void * null)
{
  return (x);
}

static long
offset_key (off64_t offset)
{
  return (offset / MIN_BLOCK_SIZE);
}

static int
addr_compar (const mr_ptr_t x, const mr_ptr_t y, const void * null)
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

static mr_hash_value_t
addr_hash (const mr_ptr_t key, const void * context)
{
  server_t * server = key.ptr;
  struct sockaddr_in * addr = &server->connection->remote;
  return (addr->sin_addr.s_addr + 0xDeadBeef * addr->sin_port);
}

static status_t
block_matched (server_t * server, block_matched_t * block_matched)
{
  status_t status = ST_SUCCESS;
  if (block_matched->matched)
    return (status);
  
  if (block_matched->block_id.size > MIN_BLOCK_SIZE)
    {
      task_t task;
      task.block_id = block_matched->block_id;
      task.size = block_matched->block_id.size / SPLIT_RATIO;
      queue_push (&server->task_queue.queue, &task);
    }
  else
    {
      msg_t msg;
      msg.msg_type = MT_BLOCK_REQUEST;
      msg.msg_data.block_id = block_matched->block_id;
      status = sync_storage_add (&server->data_blocks, offset_key (msg.msg_data.block_id.offset));
      if (ST_SUCCESS == status)
	queue_push (&server->cmd_out.queue, &msg);
    }
  return (status);
}

static status_t
block_sent (server_t * server, block_id_t * block_id)
{
  if (NULL != sync_storage_find (&server->data_blocks, offset_key (block_id->offset)))
    {
      msg_t msg;
      msg.msg_type = MT_BLOCK_REQUEST;
      msg.msg_data.block_id = *block_id;
      queue_push (&server->cmd_out.queue, &msg);
    }
  return (ST_SUCCESS);
}

static void *
server_cmd_reader (void * arg)
{
  server_t * server = arg;

  for (;;)
    {
      msg_t msg;
      status_t status = msg_recv (server->connection->cmd_fd, &msg);
      if (ST_SUCCESS != status)
	break;
      switch (msg.msg_type)
	{
	case MT_BLOCK_MATCHED:
	  status = block_matched (server, &msg.msg_data.block_matched);
	  break;
	case MT_BLOCK_SENT:
	  status = block_sent (server, &msg.msg_data.block_id);
	  break;
	case MT_BLOCK_SEND_ERROR:
	  ERROR_MSG ("Client failed to send data block (offset %lld size %lld).",
		     msg.msg_data.block_id.offset, msg.msg_data.block_id.size);
	  sync_storage_del (&server->data_blocks, offset_key (msg.msg_data.block_id.offset));
	  break;
	default:
	  status = ST_FAILURE;
	  break;
	}
      if (ST_SUCCESS != status)
	break;
    }
  shutdown (server->connection->cmd_fd, SD_BOTH);
  return (NULL);
}

static void *
server_worker (void * arg)
{
  server_t * server = arg;
  task_t task;
  msg_t msg;
  off64_t offset;

  memset (&msg, 0, sizeof (msg));
  for (;;)
    {
      queue_pop (&server->task_queue.queue, &task);
  
      msg.msg_type = MT_BLOCK_DIGEST;
      msg.msg_data.block_id.size = task.size;
      for (offset = 0; offset < task.block_id.size; offset += msg.msg_data.block_id.size)
	{
	  msg.msg_data.block_id.offset = task.block_id.offset + offset;
	  if (msg.msg_data.block_id.size > task.block_id.size - offset)
	    msg.msg_data.block_id.size = task.block_id.size - offset;
	  status_t status = calc_digest (&msg.msg_data.block_digest, server->connection->context->file_fd);
	  if (ST_SUCCESS != status)
	    break;
	  queue_push (&server->cmd_out.queue, &msg);
	}
    }
  return (NULL);
}

static status_t
server_cmd_writer (server_t * server)
{
  status_t status;
  msg_t msg;

  memset (&msg, 0, sizeof (msg));
  for (;;)
    {
      queue_pop (&server->cmd_out.queue, &msg);
      status = msg_send (server->connection->cmd_fd, &msg);
      if (status != ST_SUCCESS)
	break;
      if (MT_TERMINATE == msg.msg_type)
	{
	  INFO_MSG ("Terminating connection with client.");
	  break;
	}
    }  
  return (status);
}

static status_t
start_workers (server_t * server)
{
  int i, ncpu = (long) sysconf (_SC_NPROCESSORS_ONLN);
  pthread_t ids[ncpu];
  status_t status = ST_FAILURE;

  for (i = 0; i < ncpu; ++i)
    {
      int rv = pthread_create (&ids[i], NULL, server_worker, server);
      if (rv != 0)
	break;
    }

  if (i > 0)
    status = server_cmd_writer (server);

  for ( ; i >= 0; --i)
    {
      pthread_cancel (ids[i]);
      pthread_join (ids[i], NULL);
    }
  
  return (status);
}

static void *
data_retrieval (void * arg)
{
  server_t * server = arg;
  msg_t msg;
  off64_t offset;
  
  memset (&msg, 0, sizeof (msg));
  msg.msg_type = MT_BLOCK_REQUEST;
  msg.msg_data.block_id.size = MIN_BLOCK_SIZE;
  for (offset = 0; offset < server->connection->context->size; offset += msg.msg_data.block_id.size)
    {
      if (msg.msg_data.block_id.size > server->connection->context->size - offset)
	msg.msg_data.block_id.size = server->connection->context->size - offset;
      queue_push (&server->cmd_out.queue, &msg);
    }
  return (NULL);
}

static status_t
start_data_retrieval (server_t * server)
{
  pthread_t id;
  status_t status;
  int rv = pthread_create (&id, NULL, data_retrieval, &server);
  if (rv != 0)
    {
      ERROR_MSG ("Failed to start data retrieval thread.");
      return (ST_FAILURE);
    }

  status = server_cmd_writer (server);

  pthread_cancel (id);
  pthread_join (id, NULL);
  return (status);
}

static status_t
start_cmd_reader (server_t * server)
{
  pthread_t id;
  int rv = pthread_create (&id, NULL, server_cmd_reader, &server);
  if (rv != 0)
    {
      ERROR_MSG ("Failed to start command reader thread.");
      return (ST_FAILURE);
    }

  status_t status;
  if (!server->connection->context->file_exists)
    status = start_data_retrieval (server);
  else
    {
      task_t task;
      task.block_id.offset = 0;
      task.block_id.size = server->connection->context->size;
      task.size = MAX_BLOCK_SIZE,
      queue_push (&server->task_queue.queue, &task);
      status = start_workers (server);
    }

  pthread_cancel (id);
  pthread_join (id, NULL);
  
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

  connection_t connection;
  memset (&connection, 0, sizeof (connection));
  connection.context = &context;
  connection.cmd_fd = accepter_ctx.fd;
  connection.remote.sin_addr = accepter_ctx.remote.sin_addr;

  server_t server;
  memset (&server, 0, sizeof (server));
  server.connection = &connection;
  server.server_ctx = accepter_ctx.server_ctx;
  
  sync_storage_init (&server.data_blocks, offset_key_compar, offset_key_hash, "long_int_t", NULL);
  
  msg_t cmd_out_array_data[MSG_OUT_QUEUE_SIZE];
  MSG_QUEUE_INIT (&server.cmd_out, cmd_out_array_data);

  task_t task_array_data[MSG_OUT_QUEUE_SIZE + MSG_IN_QUEUE_SIZE];
  server.task_queue.array.data = task_array_data;
  server.task_queue.array.size = sizeof (task_array_data);
  server.task_queue.array.alloc_size = -1;
  queue_init (&server.task_queue.queue, (mr_rarray_t*)&server.task_queue.array, sizeof (server.task_queue.array.data[0]));

  status_t status = read_file_meta (&connection); /* reads UDP port of remote */

  if (ST_SUCCESS == status)
    {
      status = sync_storage_add (&server.server_ctx->clients, &server);
      if (ST_SUCCESS == status)
	status = start_cmd_reader (&server);
      close (context.file_fd);
    }
  
  shutdown (accepter_ctx.fd, SD_BOTH);
  close (accepter_ctx.fd);
  
  return (NULL);
}

static status_t
run_accepter (server_ctx_t * server_ctx)
{
  int reuse_addr = !0;
  struct linger linger_opt = { .l_onoff = 1, .l_linger = 1, };

  setsockopt (server_ctx->server_sock, SOL_SOCKET, SO_REUSEADDR, &reuse_addr, sizeof (reuse_addr));
  setsockopt (server_ctx->server_sock, SOL_SOCKET, SO_LINGER, &linger_opt, sizeof (linger_opt));

  int rv = bind (server_ctx->server_sock, (struct sockaddr *)&server_ctx->server_name, sizeof (server_ctx->server_name));
  if (rv < 0)
    {
      ERROR_MSG ("bind failed errno(%d) '%s'.", errno, strerror (errno));
      return (ST_FAILURE);
    }

  rv = listen (server_ctx->server_sock, 1);
  if (rv < 0)
    {
      ERROR_MSG ("listen failed errno(%d) '%s'.", errno, strerror (errno));
      return (ST_FAILURE);
    }

  for (;;)
    {
      accepter_ctx_t accepter_ctx = { .mutex = PTHREAD_MUTEX_INITIALIZER, .server_ctx = server_ctx, };

      accepter_ctx.remote_addr_size = sizeof (accepter_ctx.remote);
      accepter_ctx.fd = TEMP_FAILURE_RETRY (accept (server_ctx->server_sock,
						    (struct sockaddr*)&accepter_ctx.remote,
						    &accepter_ctx.remote_addr_size));
      
      if (accepter_ctx.fd < 0)
	{
	  ERROR_MSG ("accept failed errno(%d) '%s'.", errno, strerror (errno));
	  continue;
	}

      pthread_t id;
      pthread_attr_t attr;
      pthread_attr_init (&attr);
      pthread_attr_setdetachstate (&attr, PTHREAD_CREATE_DETACHED);
      
      pthread_mutex_lock (&accepter_ctx.mutex);
      rv = pthread_create (&id, &attr, handle_client, &accepter_ctx);
      if (rv != 0)
	{
	  ERROR_MSG ("Failed to create thread for new client.");
	  shutdown (accepter_ctx.fd, SD_BOTH);
	  close (accepter_ctx.fd);
	  continue;
	}
      pthread_mutex_lock (&accepter_ctx.mutex);
    }

  return (ST_SUCCESS);
}

static status_t
create_server_socket (server_ctx_t * server_ctx)
{
  status_t status;
  server_ctx->server_sock = socket (PF_INET, SOCK_STREAM, IPPROTO_TCP);
  if (server_ctx->server_sock < 0)
    {
      ERROR_MSG ("socket failed errno(%d) '%s'.", errno, strerror (errno));
      return (ST_FAILURE);
    }

  status = run_accepter (server_ctx);
  
  close (server_ctx->server_sock);
  
  return (status);
}

static status_t
put_data_block (server_t * server, unsigned char * buf, int size)
{
  status_t status = ST_FAILURE;
  block_id_t * block_id = (block_id_t *)buf;
  if (size < sizeof (*block_id))
    {
      ERROR_MSG ("Recieved block is too small.");
      return (ST_FAILURE);
    }
  if (block_id->size + sizeof (*block_id) != size)
    {
      ERROR_MSG ("Packet size mismatched header info.");
      return (ST_FAILURE);
    }
  
  unsigned char * data = mmap64 (NULL, block_id->size, PROT_READ, MAP_PRIVATE,
				 server->connection->context->file_fd, block_id->offset);
  if (-1 == (long)data)
    FATAL_MSG ("Failed to map file into memory. Error (%d) %s.\n", errno, strerror (errno));
  else
    {
      memcpy (data, &buf[sizeof (*block_id)], block_id->size);
      if (0 != munmap (data, block_id->size))
	ERROR_MSG ("Failed to unmap memory. Error (%d) %s.\n", errno, strerror (errno));
      else
	status = ST_SUCCESS;
    }
  
  return (status);
}

static void *
server_data_reader (void * arg)
{
  server_ctx_t * server_ctx = arg;
  unsigned char buf[1 << 16];
  connection_t connection;
  server_t server = { .connection = &connection, };
      
  for (;;)
    {
      union {
	struct sockaddr _addr;
	struct sockaddr_in addr_in;
      } addr;
      socklen_t addr_len = sizeof (addr);
      int rv = recvfrom (server_ctx->data_sock, buf, sizeof (buf), 0, &addr._addr, &addr_len);
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
	  ERROR_MSG ("Unknown client.");
	  continue;
	}
      put_data_block (find->ptr, buf, rv);
    }
  return (NULL);
}

static status_t
start_data_readers (server_ctx_t * server_ctx)
{
  int i;
  pthread_t ids[DATA_READERS];
  status_t status = ST_FAILURE;

  for (i = 0; i < DATA_READERS; ++i)
    {
      int rv = pthread_create (&ids[i], NULL, server_data_reader, server_ctx);
      if (rv != 0)
	break;
    }

  if (i > 0)
    status = create_server_socket (server_ctx);

  for ( ; i >= 0; --i)
    {
      pthread_cancel (ids[i]);
      pthread_join (ids[i], NULL);
    }
  
  return (status);
}

status_t
run_server (config_t * config)
{
  server_ctx_t server_ctx;

  memset (&server_ctx, 0, sizeof (server_ctx));
  server_ctx.config = config;
  
  server_ctx.server_name.sin_family = AF_INET;
  server_ctx.server_name.sin_port = htons (config->listen_port);
  server_ctx.server_name.sin_addr.s_addr = htonl (INADDR_ANY);

  sync_storage_init (&server_ctx.clients, addr_compar, addr_hash, "server_t", NULL);
  
  server_ctx.data_sock = socket (PF_INET, SOCK_DGRAM, IPPROTO_UDP);
  if (server_ctx.data_sock < 0)
    {
      ERROR_MSG ("socket failed errno(%d) '%s'.", errno, strerror (errno));
      return (ST_FAILURE);
    }

  status_t status;
  int rv = bind (server_ctx.data_sock, (struct sockaddr *)&server_ctx.server_name, sizeof (server_ctx.server_name));
  if (0 == rv)
    status = start_data_readers (&server_ctx);
  else
    {
      ERROR_MSG ("bind failed errno(%d) '%s'.", errno, strerror (errno));
      status = ST_FAILURE;
    }
  
  close (server_ctx.data_sock);
  
  return (status);
}
