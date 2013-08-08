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

int
offset_key_compar (const long x, const long y, const void * null)
{
  return ((x > y) - (y > x));
}

mr_hash_value_t
offset_key_hash (const long x, const void * null)
{
  return (x);
}

static long
offset_key (off64_t offset)
{
  return (offset / MIN_BLOCK_SIZE);
}

int
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

mr_hash_value_t
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
  DUMP_VAR (block_matched_t, block_matched);
  if (block_matched->matched)
    return (status);
  
  if (block_matched->block_id.size > MIN_BLOCK_SIZE)
    {
      task_t task;
      memset (&task, 0, sizeof (task));
      task.block_id = block_matched->block_id;
      task.size = block_matched->block_id.size / SPLIT_RATIO;
      DUMP_VAR (task_t, &task);
      queue_push (&server->task_queue.queue, &task);
      DEBUG_MSG ("Task pushed to queue.");
    }
  else
    {
      msg_t msg;
      memset (&msg, 0, sizeof (msg));
      msg.msg_type = MT_BLOCK_REQUEST;
      msg.msg_data.block_id = block_matched->block_id;
      DUMP_VAR (msg_t, &msg);
      status = sync_storage_add (&server->data_blocks, offset_key (msg.msg_data.block_id.offset));
      if (ST_SUCCESS == status)
	queue_push (&server->cmd_out.queue, &msg);
      DEBUG_MSG ("Mesasge pushed to queue with status %d.", status);
    }
  return (status);
}

static status_t
block_sent (server_t * server, block_id_t * block_id)
{
  DEBUG_MSG ("Got confirmation for block %zd:%zd.", block_id->offset, block_id->size);
  if (NULL != sync_storage_find (&server->data_blocks, offset_key (block_id->offset)))
    {
      msg_t msg;
      memset (&msg, 0, sizeof (msg));
      msg.msg_type = MT_BLOCK_REQUEST;
      msg.msg_data.block_id = *block_id;
      DEBUG_MSG ("Request block %zd:%zd once again.", block_id->offset, block_id->size);
      queue_push (&server->cmd_out.queue, &msg);
      DEBUG_MSG ("Request for block %zd:%zd pushed to queue.", block_id->offset, block_id->size);
    }
  return (ST_SUCCESS);
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
      DEBUG_MSG ("Got command message. Status %d.", status);
      if (ST_SUCCESS != status)
	break;

      DUMP_VAR (msg_t, & msg);
      switch (msg.msg_type)
	{
	case MT_BLOCK_MATCHED:
	  status = block_matched (server, &msg.msg_data.block_matched);
	  break;
	case MT_BLOCK_SENT:
	  status = block_sent (server, &msg.msg_data.block_id);
	  break;
	case MT_BLOCK_SEND_ERROR:
	  ERROR_MSG ("Client failed to send data block (offset %zd size %zd).",
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
      queue_pop (&server->task_queue.queue, &task);
      DEBUG_MSG ("Server worker got task: offset:size %zd:%zd split on %zd.",
		 task.block_id.offset, task.block_id.size, task.size);
      
      msg.msg_type = MT_BLOCK_DIGEST;
      msg.msg_data.block_id.size = task.size;
      for (offset = 0; offset < task.block_id.size; offset += msg.msg_data.block_id.size)
	{
	  msg.msg_data.block_id.offset = task.block_id.offset + offset;
	  if (msg.msg_data.block_id.size > task.block_id.size - offset)
	    msg.msg_data.block_id.size = task.block_id.size - offset;
	  status_t status = calc_digest (&msg.msg_data.block_digest, server->connection->context->file_fd);
	  DEBUG_MSG ("Calc digest for offset %zd status %d.", msg.msg_data.block_id.offset, status);
	  if (ST_SUCCESS != status)
	    break;
	  DEBUG_MSG ("Pushing to outgoing queue digest for offset %zd.", msg.msg_data.block_id.offset);
	  queue_push (&server->cmd_out.queue, &msg);
	  DEBUG_MSG ("Pushed digest for offset %zd.", msg.msg_data.block_id.offset);
	}
    }
  DEBUG_MSG ("Exiting server worker.");
  return (NULL);
}

static status_t
server_cmd_writer (server_t * server)
{
  status_t status;
  msg_t msg;

  DUMP_VAR (server_t, server);
  DEBUG_MSG ("Started server command writer.");
  memset (&msg, 0, sizeof (msg));
  for (;;)
    {
      queue_pop (&server->cmd_out.queue, &msg);
      DEBUG_MSG ("Write message type %d to client %08x:%04x.", msg.msg_type,
		 server->connection->remote.sin_addr.s_addr, server->connection->remote.sin_port);
      DUMP_VAR (msg_t, &msg);
      status = msg_send (server->connection->cmd_fd, &msg);
      if (status != ST_SUCCESS)
	break;
      if (MT_TERMINATE == msg.msg_type)
	{
	  DEBUG_MSG ("Terminating connection with client.");
	  break;
	}
    }
  DEBUG_MSG ("Exiting server command writer.");
  return (status);
}

static status_t
start_workers (server_t * server)
{
  int i, ncpu = (long) sysconf (_SC_NPROCESSORS_ONLN);
  pthread_t ids[ncpu];
  status_t status = ST_FAILURE;

  DEBUG_MSG ("Start server workers %d.", ncpu);
  for (i = 0; i < ncpu; ++i)
    {
      int rv = pthread_create (&ids[i], NULL, server_worker, server);
      if (rv != 0)
	break;
    }

  DEBUG_MSG ("Started %d.", i);
  if (i > 0)
    status = server_cmd_writer (server);

  DEBUG_MSG ("Canceling server workers.");
  for (--i ; i >= 0; --i)
    {
      pthread_cancel (ids[i]);
      pthread_join (ids[i], NULL);
    }
  DEBUG_MSG ("Server workers canceled.");
  
  return (status);
}

static void *
data_retrieval (void * arg)
{
  server_t * server = arg;
  msg_t msg;
  off64_t offset;
  
  DEBUG_MSG ("Data retrieval thread has started.");
  memset (&msg, 0, sizeof (msg));
  msg.msg_type = MT_BLOCK_REQUEST;
  msg.msg_data.block_id.size = MIN_BLOCK_SIZE;
  for (offset = 0; offset < server->connection->context->size; offset += msg.msg_data.block_id.size)
    {
      if (msg.msg_data.block_id.size > server->connection->context->size - offset)
	msg.msg_data.block_id.size = server->connection->context->size - offset;
      DEBUG_MSG ("Push task to get block %zd:%zd.", msg.msg_data.block_id.offset, msg.msg_data.block_id.size);
      queue_push (&server->cmd_out.queue, &msg);
    }
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
  pthread_cancel (id);
  pthread_join (id, NULL);
  DEBUG_MSG ("Data retrieval thread canceled.");
  return (status);
}

static status_t
start_cmd_reader (server_t * server)
{
  pthread_t id;
  int rv = pthread_create (&id, NULL, server_cmd_reader, server);
  if (rv != 0)
    {
      ERROR_MSG ("Failed to start command reader thread.");
      return (ST_FAILURE);
    }
  DEBUG_MSG ("Satart command reader. Thread create returned %d.", rv);

  DEBUG_MSG ("File on server status %d.", server->connection->context->file_exists);
  status_t status;
  if (!server->connection->context->file_exists)
    status = start_data_retrieval (server);
  else
    {
      task_t task;
      memset (&task, 0, sizeof (task));
      task.block_id.offset = 0;
      task.block_id.size = server->connection->context->size;
      task.size = MAX_BLOCK_SIZE;
      DEBUG_MSG ("File on server exists. Push initial task.");
      queue_push (&server->task_queue.queue, &task);
      DEBUG_MSG ("Start workers.");
      status = start_workers (server);
    }

  DEBUG_MSG ("Canceling command reader thread.");
  pthread_cancel (id);
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

  DEBUG_MSG ("Context for new client inited. Read file meta from client.");
  status_t status = read_file_meta (&connection); /* reads UDP port of remote into connection_t and opens file for write */

  if (ST_SUCCESS == status)
    {
      status = sync_storage_add (&server.server_ctx->clients, &server);
      DEBUG_MSG ("Adder client context to registry. Return value %d.", status);
      if (ST_SUCCESS == status)
	status = start_cmd_reader (&server);
      close (context.file_fd);
    }
  
  shutdown (accepter_ctx.fd, SD_BOTH);
  close (accepter_ctx.fd);
  DEBUG_MSG ("Closed connection to client: %08x:%04x.", accepter_ctx.remote.sin_addr.s_addr, accepter_ctx.remote.sin_port);
  return (NULL);
}

static status_t
run_accepter (server_ctx_t * server_ctx)
{
  int reuse_addr = !0;
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
	  continue;
	}

      DEBUG_MSG ("New client from: %08x:%04x.", accepter_ctx.remote.sin_addr.s_addr, accepter_ctx.remote.sin_port);
      
      pthread_t id;
      pthread_attr_t attr;
      pthread_attr_init (&attr);
      pthread_attr_setdetachstate (&attr, PTHREAD_CREATE_DETACHED);
      
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
  
  close (server_ctx->server_sock);
  DEBUG_MSG ("Server socket closed.");
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

  DEBUG_MSG ("Write block at offset %zd size %zd.", block_id->offset, block_id->size);
  /* unregister block in registry */
  sync_storage_del (&server->data_blocks, offset_key (block_id->offset));

  unsigned char * data = mmap64 (NULL, block_id->size, PROT_WRITE, MAP_PRIVATE,
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
  DEBUG_MSG ("Write block done.");
  
  return (status);
}

static void *
server_data_reader (void * arg)
{
  server_ctx_t * server_ctx = arg;
  unsigned char buf[1 << 16];
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
	  ERROR_MSG ("Unknown client.");
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
  pthread_t ids[DATA_READERS];
  status_t status = ST_FAILURE;

  DEBUG_MSG ("Starting %d data readers.", sizeof (ids) / sizeof (ids[0]));
  for (i = 0; i < sizeof (ids) / sizeof (ids[0]); ++i)
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

  memset (&server_ctx, 0, sizeof (server_ctx));
  server_ctx.config = config;
  
  server_ctx.server_name.sin_family = AF_INET;
  server_ctx.server_name.sin_port = htons (config->listen_port);
  server_ctx.server_name.sin_addr.s_addr = htonl (INADDR_ANY);

  sync_storage_init (&server_ctx.clients, addr_compar, addr_hash, "server_t", NULL);

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
  DEBUG_MSG ("Closed data socket. Exiting server.");
  
  return (status);
}
