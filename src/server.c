#include <millstone.h>
#include <logging.h>
#include <block.h>
#include <file_meta.h>
#include <queue.h>
#include <msg.h>
#include <calc_digest.h>
#include <server.h>

#include <unistd.h> /* TEMP_FAILURE_RETRY, sysconf, close, ftruncate64 */
#include <errno.h> /* errno, strerror */
#include <sys/user.h> /* PAGE_SIZE */

#include <pthread.h>

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

TYPEDEF_STRUCT (server_t,
		(connection_t *, connection),
		(msg_queue_t, cmd_out),
		(task_queue_t, task_queue),
		)

TYPEDEF_STRUCT (accepter_ctx_t,
		(config_t *, config),
		(struct sockaddr_in, clientname),
		int fd,
		(pthread_mutex_t, mutex),
		)

static void
block_matched (server_t * server, block_matched_t * block_matched)
{
  if (block_matched->matched)
    return;
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
      queue_push (&server->cmd_out.queue, &msg);
    }
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
	  block_matched (server, &msg.msg_data.block_matched);
	  break;
	case MT_BLOCK_SENT:
	  break;
	case MT_BLOCK_SEND_ERROR:
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
server_data_reader (void * arg)
{
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
start_data_reader (server_t * server)
{
  pthread_t id;
  status_t status;
  int rv = pthread_create (&id, NULL, server_data_reader, &server);
  if (rv != 0)
    {
      ERROR_MSG ("Failed to start data reader thread.");
      return (ST_FAILURE);
    }

  if (server->connection->context->file_exists)
    {
      task_t task = {
	.block_id = {
	  .offset = 0,
	  .size = server->connection->context->size,
	},
	.size = MAX_BLOCK_SIZE,
      };
      queue_push (&server->task_queue.queue, &task);
      status = start_workers (server);
    }
  else
    status = start_data_retrieval (server);

  pthread_cancel (id);
  pthread_join (id, NULL);
  return (status);
}

static status_t
init_server (connection_t * connection)
{
  server_t server = { .connection = connection, };
  msg_t cmd_out_array_data[MSG_OUT_QUEUE_SIZE];
  task_t task_array_data[MSG_OUT_QUEUE_SIZE + MSG_IN_QUEUE_SIZE];
  
  status_t status = MSG_QUEUE_INIT (&server.cmd_out, cmd_out_array_data);
  if (ST_SUCCESS != status)
    return (status);

  server.task_queue.array.data = task_array_data;
  server.task_queue.array.size = sizeof (task_array_data);
  server.task_queue.array.alloc_size = -1;
  status = queue_init (&server.task_queue.queue, (mr_rarray_t*)&server.task_queue.array, sizeof (server.task_queue.array.data[0]));
  if (ST_SUCCESS != status)
    return (status);

  pthread_t id;
  int rv = pthread_create (&id, NULL, server_cmd_reader, &server);
  if (rv != 0)
    {
      ERROR_MSG ("Failed to start command reader thread.");
      return (ST_FAILURE);
    }
  
  status = start_data_reader (&server);

  pthread_cancel (id);
  pthread_join (id, NULL);
  
  return (status);
}

static status_t
start_data_socket (connection_t * connection)
{
  status_t status;
  connection->data_fd = socket (PF_INET, SOCK_DGRAM, 0);
  if (connection->data_fd < 0)
    {
      ERROR_MSG ("socket failed errno(%d) '%s'.", errno, strerror (errno));
      return (ST_FAILURE);
    }

  status = init_server (connection);
  close (connection->data_fd);
  
  return (status);
}

static void *
handle_client (void * arg)
{
  accepter_ctx_t * ctx = arg;
  accepter_ctx_t accepter_ctx = *ctx;
  pthread_mutex_unlock (&ctx->mutex);

  context_t context = { .config = accepter_ctx.config, };
  connection_t connection = {
    .context = &context,
    .cmd_fd = accepter_ctx.fd,
    .name = accepter_ctx.clientname,
  };
  status_t status = read_file_meta (&connection);
  
  if (ST_SUCCESS == status)
    {
      start_data_socket (&connection);
      close (context.file_fd);
    }
  
  shutdown (accepter_ctx.fd, SD_BOTH);
  close (accepter_ctx.fd);
  
  return (NULL);
}

static status_t
run_accepter (config_t * config, int sock)
{
  struct sockaddr_in name;
  int reuse_addr = !0;
  struct linger linger_opt = { .l_onoff = 1, .l_linger = 1, };

  setsockopt (sock, SOL_SOCKET, SO_REUSEADDR, &reuse_addr, sizeof (reuse_addr));
  setsockopt (sock, SOL_SOCKET, SO_LINGER, &linger_opt, sizeof (linger_opt));

  name.sin_family = AF_INET;
  name.sin_port = htons (config->listen_port);
  name.sin_addr.s_addr = htonl (INADDR_ANY);

  int status = bind (sock, (struct sockaddr *) &name, sizeof (name));
  if (status < 0)
    {
      ERROR_MSG ("bind failed errno(%d) '%s'.", errno, strerror (errno));
      return (ST_FAILURE);
    }

  status = listen (sock, 1);
  if (status < 0)
    {
      ERROR_MSG ("listen failed errno(%d) '%s'.", errno, strerror (errno));
      return (ST_FAILURE);
    }

  for (;;)
    {
      accepter_ctx_t accepter_ctx = { .mutex = PTHREAD_MUTEX_INITIALIZER, };
      socklen_t size = sizeof (accepter_ctx.clientname);

      accepter_ctx.fd = TEMP_FAILURE_RETRY (accept (sock, (struct sockaddr*)&accepter_ctx.clientname, &size));
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
      int rv = pthread_create (&id, &attr, handle_client, &accepter_ctx);
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

status_t
run_server (config_t * config)
{
  status_t status;
  int sock = socket (PF_INET, SOCK_STREAM, 0);
  if (sock < 0)
    {
      ERROR_MSG ("socket failed errno(%d) '%s'.", errno, strerror (errno));
      return (ST_FAILURE);
    }

  status = run_accepter (config, sock);
  close (sock);
  
  return (status);
}
