#include <millstone.h>
#include <logging.h>
#include <block.h>
#include <close_connection.h>
#include <file_meta.h>
#include <server.h>

#include <unistd.h> /* TEMP_FAILURE_RETRY, sysconf, close, ftruncate64 */
#include <errno.h> /* errno, strerror */

#include <pthread.h>

TYPEDEF_STRUCT (split_task_t,
		(block_id_t, block_id),
		(size_t, size),
		)

TYPEDEF_STRUCT (accepter_ctx_t,
		(config_t *, config),
		(struct sockaddr_in, clientname),
		int fd,
		(pthread_mutex_t, mutex),
		)

static status_t
client_main_loop (connection_t * connection)
{
#if 0
  msg_t cmd_in_array_data[MSG_IN_QUEUE_SIZE];

  status = MSG_QUEUE_INIT (&client.cmd_in, cmd_in_array_data);
  if (ST_SUCCESS != status)
    return (status);
#endif
  return (ST_FAILURE);
}

static status_t
start_data_socket (context_t * context, struct sockaddr_in * name)
{
  connection_t connection = { .context = context, };
  status_t status = client_main_loop (&connection);
  return (status);
}

static void *
handle_client (void * arg)
{
  accepter_ctx_t * ctx = arg;
  accepter_ctx_t accepter_ctx = *ctx;
  pthread_mutex_unlock (&ctx->mutex);

  context_t context = { .config = accepter_ctx.config, };
  connection_t connection = { .context = &context, .cmd_fd = accepter_ctx.fd, };
  bool file_exists = false;
  status_t status = read_file_meta (&connection, &file_exists);
  
  if (ST_SUCCESS == status)
    start_data_socket (&context, &accepter_ctx.clientname);

  close (context.file_fd);
  
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
	  close_connection (accepter_ctx.fd);
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
