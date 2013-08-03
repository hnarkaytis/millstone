#define _LARGEFILE64_SOURCE
#define _GNU_SOURCE
#include <unistd.h>
#include <fcntl.h>
#include <netdb.h>
#include <errno.h>

#include <pthread.h>

#include <client.h>
#include <logging.h>

status_t
session (connection_t * connection)
{
  return (EXIT_SUCCESS);
}

status_t
connect_to_server (context_t * context)
{
  struct sockaddr_in name;
  connection_t connection = {
    .context = context,
  };

  connection.conn_fd = socket (PF_INET, SOCK_STREAM, 0);
  if (connection.conn_fd <= 0)
    {
      ERROR_MSG ("Socket failed errno(%d) '%s'.", errno, strerror (errno));
      return (ST_FAILURE);
    }

  pthread_cleanup_push ((void (*) (void*))close, (void*)(long)connection.conn_fd);

  struct hostent * hostinfo = gethostbyname (context->config->dst_host);
  if (hostinfo != NULL)
    name.sin_addr.s_addr = *((in_addr_t*) hostinfo->h_addr);
  else
    name.sin_addr.s_addr = htonl (INADDR_ANY);

  name.sin_family = AF_INET;
  name.sin_port = htons (context->config->dst_port);

  int rv = TEMP_FAILURE_RETRY (connect (connection.conn_fd, (struct sockaddr *)&name,
					sizeof (struct sockaddr_in)));
  if (-1 == rv)
    {
      ERROR_MSG ("Connect failed errno(%d) '%s'.", errno, strerror (errno));
      return (ST_FAILURE);
    }

  session (&connection);

  pthread_cleanup_pop (!0);

  
  return (ST_SUCCESS);
}

status_t
client (config_t * config)
{
  status_t status = ST_FAILURE;
  context_t context = { .config = config };
  
  context.file_fd = open64 (config->src_file, O_RDONLY);
  if (context.file_fd <= 0)
    {
      ERROR_MSG ("Can't open source file '%s'", config->src_file);
      return (ST_FAILURE);
    }

  pthread_cleanup_push ((void (*) (void*))close, (void*)(long)context.file_fd);
  status = connect_to_server (&context);
  pthread_cleanup_pop (!0);

  return (status);
}

