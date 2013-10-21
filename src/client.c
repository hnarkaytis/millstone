#define _GNU_SOURCE /* TEMP_FAILURE_RETRY */

#ifdef HAVE_CONFIG_H
#include <config.h>
#endif /* HAVE_CONFIG_H */
#include <millstone.h>
#include <logging.h>
#include <block.h>
#include <msg.h>
#include <llist.h>
#include <file_meta.h>
#include <client.h>

#include <stddef.h> /* size_t, ssize_t */
#include <unistd.h> /* TEMP_FAILURE_RETRY, sysconf, close, lseek64, SEEK_END */
#include <inttypes.h> /* SCNx64 */
#include <errno.h> /* errno */
#include <fcntl.h> /* open64 */
#include <string.h> /* memset, setlen, strerror */
#include <netdb.h> /* gethostbyname */
#include <netinet/in.h> /* htonl, struct sockaddr_in */
#include <sys/sysinfo.h> /* _SC_NPROCESSORS_ONLN */
#include <sys/uio.h> /* writev, struct iovec */
#include <sys/mman.h> /* mmap64, unmap */
#include <sys/socket.h> /* socket, shutdown, connect, setsockopt */
#include <netinet/tcp.h> /* TCP_NODELAY */

#include <openssl/sha.h> /* SHA1 */
#include <pthread.h>
#ifdef HAVE_ZLIB
#include <zlib.h>
#endif /* HAVE_ZLIB */

#include <metaresc.h>
#include <mr_ic.h>

TYPEDEF_STRUCT (chunk_dedup_t,
		(off64_t, chunk_id),
		(sync_storage_t, dedup),
		)

TYPEDEF_STRUCT (client_t,
		(connection_t *, connection),
		(llist_t, data_in),
		(llist_t, cmd_in),
		(llist_t, cmd_out),
		(sync_storage_t, dedup),
		(sync_storage_t, chunk_dedup),
		)

mr_hash_value_t
block_digest_hash (const mr_ptr_t key, const void * context)
{
  block_digest_t * block_digest = key.ptr;
  return (*((mr_hash_value_t*)(block_digest->digest)));
}

int
block_digest_compar (const mr_ptr_t x, const mr_ptr_t y, const void * context)
{
  block_digest_t * x_block_digest = x.ptr;
  block_digest_t * y_block_digest = y.ptr;
  return (memcmp (x_block_digest->digest, y_block_digest->digest, sizeof (x_block_digest->digest)));
}

void
block_digest_free (const mr_ptr_t x, const void * context)
{
  MR_FREE (x.ptr);
}

static status_t
send_block (client_t * client, block_id_t * block_id)
{
  status_t status = ST_SUCCESS;
  unsigned char * block_data = file_chunks_get_addr (client->connection->file, block_id->offset);

  DUMP_VAR (block_id_t, block_id);

  if (NULL == block_data)
    return (ST_FAILURE);
  
#ifdef HAVE_ZLIB
  Byte buffer[block_id->size << 1];
  uLongf length = sizeof (buffer);
  if (client->connection->file->config->compress_level > 0)
    {
      int z_status = compress2 (buffer, &length, block_data, block_id->size,
				client->connection->file->config->compress_level);
      
      if (Z_OK != z_status)
	status = ST_FAILURE;
    }
#endif /* HAVE_ZLIB */

  if (ST_SUCCESS == status)
    {
      struct iovec iov[] = {
	{ .iov_len = sizeof (*block_id), .iov_base = block_id, },
	{ .iov_len = block_id->size, .iov_base = block_data, },
      };

#ifdef HAVE_ZLIB
      if (client->connection->file->config->compress_level > 0)
	{
	  iov[1].iov_len = length;
	  iov[1].iov_base = buffer;
	}
#endif /* HAVE_ZLIB */

      ssize_t rv;
      ssize_t i, len = 0;
      
      do {
	rv = writev (client->connection->data_fd, iov, sizeof (iov) / sizeof (iov[0]));
      } while ((-1 == rv) && ((EPERM == errno) || (EINTR == errno) || (ENOBUFS == errno)));

      for (i = 0; i < sizeof (iov) / sizeof (iov[0]); ++i)
	len += iov[i].iov_len;

      DEBUG_MSG ("Wrote packet of %zd bytes. Expected to write %zd.", rv, len);

      if (rv != len)
	{
	  ERROR_MSG ("Failed to send data block (sent %d bytes, but expexted %d bytes). Error (%d) '%s'", rv, len, errno, strerror (errno));
	  status = ST_FAILURE;
	}
      
      if (ST_SUCCESS != chunk_unref (client->connection->file, block_id->offset))
	status = ST_FAILURE;
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
      status_t status = llist_pop (&client->data_in, &msg);
      if (ST_SUCCESS != status)
	break;
      
      DUMP_VAR (msg_t, &msg);

      status = send_block (client, &msg.block_id);
      
      DEBUG_MSG ("Data block send status %d.", status);

      if (ST_SUCCESS != status)
	msg.msg_type = MT_BLOCK_SEND_ERROR;
      else
	msg.msg_type = MT_BLOCK_SENT;
      
      status = llist_push (&client->cmd_out, &msg);
      if (ST_SUCCESS != status)
	break;
    }

  llist_cancel (&client->data_in);
  llist_cancel (&client->cmd_in);
  llist_cancel (&client->cmd_out);

  DEBUG_MSG ("Exiting client data writer.");
  
  return (NULL);
}

static void *
client_cmd_writer (void * arg)
{
  client_t * client = arg;
  char buf[EXPECTED_PACKET_SIZE];

  DEBUG_MSG ("Enter client command writer.");
  
  memset (buf, 0, sizeof (buf));
  for (;;)
    {
      size_t buf_size = sizeof (buf);
      status_t status = llist_pop_bulk (&client->cmd_out, buf, &buf_size);
      if (ST_SUCCESS != status)
	break;

      DEBUG_MSG ("Send %d bytes to server.", buf_size);

      status = msg_send (client->connection->cmd_fd, buf, buf_size);
      if (ST_SUCCESS != status)
	break;

      DEBUG_MSG ("Message sent.");
    }
  
  shutdown (client->connection->cmd_fd, SD_BOTH);
  llist_cancel (&client->data_in);
  llist_cancel (&client->cmd_in);
  llist_cancel (&client->cmd_out);
  
  DEBUG_MSG ("Exiting client command writer.");
  
  return (NULL);
}

static void
dedup_add (sync_storage_t * sync_storage, block_digest_t * block_digest, int mem_threshold)
{
  long avphys_pages = sysconf (_SC_AVPHYS_PAGES);
  static long phys_pages = 0;

  if (0 == phys_pages)
    phys_pages = sysconf (_SC_PHYS_PAGES);

  if (100 * avphys_pages > phys_pages * mem_threshold)
    {
      block_digest_t * allocated_block_digest = MR_MALLOC (sizeof (*allocated_block_digest));
      if (allocated_block_digest != NULL)
	{
	  *allocated_block_digest = *block_digest;
	  mr_ptr_t * find = sync_storage_add (sync_storage, allocated_block_digest);
	  if ((NULL == find) || (find->ptr != allocated_block_digest))
	    MR_FREE (allocated_block_digest);
	}
    }
}

mr_hash_value_t
chunk_dedup_hash (const mr_ptr_t key, const void * context)
{
  chunk_dedup_t * chunk_dedup = key.ptr;
  return (chunk_dedup->chunk_id);
}

int
chunk_dedup_compar (const mr_ptr_t x, const mr_ptr_t y, const void * context)
{
  chunk_dedup_t * x_ = x.ptr;
  chunk_dedup_t * y_ = y.ptr;
  return ((x_->chunk_id > y_->chunk_id) - (x_->chunk_id < y_->chunk_id));
}

void
chunk_dedup_free (const mr_ptr_t x, const void * context)
{
  chunk_dedup_t * chunk_dedup = x.ptr;
  sync_storage_free (&chunk_dedup->dedup);
  MR_FREE (chunk_dedup);
}

static chunk_dedup_t *
chunk_dedup_find (sync_storage_t * sync_storage, off64_t chunk_id)
{
  chunk_dedup_t chunk_dedup;
  memset (&chunk_dedup, 0, sizeof (chunk_dedup));
  chunk_dedup.chunk_id = chunk_id;
  mr_ptr_t * find = sync_storage_find (sync_storage, &chunk_dedup, NULL);
  return (find ? find->ptr : NULL);
}

static void *
digest_calculator (void * arg)
{
  client_t * client = arg;
  block_digest_t block_digest;
  msg_t msg;

  DEBUG_MSG ("Entered digest calculator.");

  memset (&block_digest, 0, sizeof (block_digest));
  memset (&msg, 0, sizeof (msg));
  
  for (;;)
    {
      status_t status = llist_pop (&client->cmd_in, &msg);
      if (ST_SUCCESS != status)
	break;
      
      DUMP_VAR (msg_t, &msg);
      
      block_digest.block_id = msg.block_digest.block_id;
      unsigned char * block_data = file_chunks_get_addr (client->connection->file, block_digest.block_id.offset);
      if (NULL == block_data)
	break;
      
      SHA1 (block_data, block_digest.block_id.size, (unsigned char*)&block_digest.digest);

      if (ST_SUCCESS != chunk_unref (client->connection->file, block_digest.block_id.offset))
	break;

      msg.msg_type = MT_BLOCK_MATCHED;
      msg.block_matched.matched = !memcmp (block_digest.digest, msg.block_digest.digest, sizeof (block_digest.digest));
      msg.block_matched.duplicate = FALSE;
      memset (&msg.block_matched.duplicate_block_id, 0, sizeof (msg.block_matched.duplicate_block_id));

      DEBUG_MSG ("Block on offset %" SCNx64 " matched status %d.",
		 msg.block_digest.block_id.offset, msg.block_matched.matched);

      if (msg.block_matched.matched)
	dedup_add (&client->dedup, &block_digest, client->connection->file->config->mem_threshold);
      else
	{
	  mr_ptr_t * find = sync_storage_find (&client->dedup, &block_digest, NULL);
	  block_id_t * matched_block_id = find ? find->ptr : NULL;
	  if ((matched_block_id != NULL) && (matched_block_id->size == block_digest.block_id.size))
	    {
	      msg.block_matched.duplicate = TRUE;
	      msg.block_matched.duplicate_block_id = *matched_block_id;
	    }
	  else
	    {
	      off64_t chunk_id = chunk_get_id (client->connection->file, block_digest.block_id.offset);
	      chunk_dedup_t * chunk_dedup = chunk_dedup_find (&client->chunk_dedup, chunk_id);
	      if (chunk_dedup != NULL)
		dedup_add (&chunk_dedup->dedup, &block_digest, client->connection->file->config->mem_threshold);
	    }
	}
      
      DUMP_VAR (msg_t, &msg);
      
      status = llist_push (&client->cmd_out, &msg);
      if (ST_SUCCESS != status)
	break;
      
      DEBUG_MSG ("Message pushed.");
    }

  llist_cancel (&client->data_in);
  llist_cancel (&client->cmd_in);
  llist_cancel (&client->cmd_out);

  DEBUG_MSG ("Exiting digest calculator.");
  
  return (NULL);
}

static status_t
block_ref (client_t * client, block_id_t * block_id)
{
  chunk_t * chunk = chunk_ref (client->connection->file, block_id->offset);
  status_t status = ST_SUCCESS;

  if (NULL == chunk)
    status = ST_FAILURE;
  else
    {
      off64_t chunk_id = chunk_get_id (client->connection->file, block_id->offset);
      chunk_dedup_t * chunk_dedup = chunk_dedup_find (&client->chunk_dedup, chunk_id);
      if (chunk_dedup == NULL)
	{
	  chunk_dedup = MR_MALLOC (sizeof (*chunk_dedup));
	  if (NULL == chunk_dedup)
	    status = ST_FAILURE;
	  else
	    {
	      memset (chunk_dedup, 0, sizeof (*chunk_dedup));
	      chunk_dedup->chunk_id = chunk_id;
	      sync_storage_init (&chunk_dedup->dedup, block_digest_compar, block_digest_hash, block_digest_free, "block_digest_t", client);
	      mr_ptr_t * find = sync_storage_add (&client->chunk_dedup, chunk_dedup);
	      if ((NULL == find) || (find->ptr != chunk_dedup))
		MR_FREE (chunk_dedup);
	      if (NULL == find)
		status = ST_FAILURE;
	    }
	}
    }
  
  return (status);
}

static status_t
client_cmd_reader (void * arg)
{
  client_t * client = arg;
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
      
      switch (msg.msg_type)
	{
	case MT_BLOCK_REF:
	  status = block_ref (client, &msg.block_id);
	  break;

	case MT_BLOCK_UNREF:
	  status = chunk_unref (client->connection->file, msg.block_id.offset);
	  break;
	  
	case MT_BLOCK_DIGEST:
	  status = llist_push (&client->cmd_in, &msg);
	  break;
	  
	case MT_BLOCK_REQUEST:
	  status = llist_push (&client->data_in, &msg);
	  break;
	  
	default:
	  ERROR_MSG ("Unexpected message type %d.", msg.msg_type);
	  status = ST_FAILURE;
	  break;
	}
      if (ST_SUCCESS != status)
	break;
    }
  
  llist_cancel (&client->data_in);
  llist_cancel (&client->cmd_in);
  llist_cancel (&client->cmd_out);

  DEBUG_MSG ("Exiting client command reader.");
  
  return (status);
}

static status_t
start_cmd_writer (void * arg)
{
  return (start_threads (client_cmd_writer, 1, client_cmd_reader, arg));
}

static status_t
start_data_writers (void * arg)
{
  client_t * client = arg;
  return (start_threads (client_data_writer, client->connection->file->config->workers_number, start_cmd_writer, client));
}

static mr_status_t
block_digest_register (const mr_ptr_t node, const void * context)
{
  block_digest_t * block_digest = node.ptr;
  client_t * client = (client_t*)context;
  dedup_add (&client->dedup, block_digest, client->connection->file->config->mem_threshold);
  return (MR_SUCCESS);
}

void
client_chunk_release (chunk_t * chunk, void * context)
{
  client_t * client = context;
  off64_t chunk_id = chunk_get_id (client->connection->file, chunk->block_id.offset);
  chunk_dedup_t * chunk_dedup = chunk_dedup_find (&client->chunk_dedup, chunk_id);
  if (chunk_dedup != NULL)
    sync_storage_yeld (&chunk_dedup->dedup, block_digest_register);
}

static status_t
run_session (connection_t * connection)
{
  client_t client;

  memset (&client, 0, sizeof (client));
  client.connection = connection;

  DEBUG_MSG ("Sending file meata data.");
  
  status_t status = send_file_meta (connection);
  if (ST_SUCCESS != status)
    return (status);
  
  LLIST_INIT (&client.cmd_out, msg_t, -1);
  LLIST_INIT (&client.cmd_in, msg_t, -1);
  LLIST_INIT (&client.data_in, msg_t, -1);

  file_chunks_set_release_handler (connection->file, client_chunk_release, &client);
  sync_storage_init (&client.dedup, block_digest_compar, block_digest_hash, block_digest_free, "block_digest_t", &client);
  sync_storage_init (&client.chunk_dedup, chunk_dedup_compar, chunk_dedup_hash, chunk_dedup_free, "chunk_dedup_t", &client);
  
  DEBUG_MSG ("Session inited with.");
  
  status = start_threads (digest_calculator, connection->file->config->workers_number, start_data_writers, &client);

  file_chunks_free (connection->file);
  sync_storage_free (&client.chunk_dedup);
  sync_storage_free (&client.dedup);

  llist_cancel (&client.data_in);
  llist_cancel (&client.cmd_in);
  llist_cancel (&client.cmd_out);
  
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
  struct hostent * hostinfo = gethostbyname (connection->file->config->dst_host);
  if (hostinfo != NULL)
    connection->remote.sin_addr.s_addr = *((in_addr_t*) hostinfo->h_addr);
  else
    connection->remote.sin_addr.s_addr = htonl (INADDR_ANY);

  connection->remote.sin_family = AF_INET;
  connection->remote.sin_port = htons (connection->file->config->dst_port);

  DEBUG_MSG ("Connect to %08x:%04x.", connection->remote.sin_addr.s_addr, connection->remote.sin_port);

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

  DEBUG_MSG ("Connected to server successfully.");
  
  status_t status = create_data_socket (connection);

  shutdown (connection->cmd_fd, SD_BOTH);

  DEBUG_MSG ("Shutdown command socket.");

  return (status);
}

static status_t
create_client_socket (file_t * file)
{
  connection_t connection;

  memset (&connection, 0, sizeof (connection));
  connection.file = file;

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
  file_t file;

  memset (&file, 0, sizeof (file));
  file.config = config;
  file.fd = open64 (config->src_file, O_RDONLY);
  if (file.fd <= 0)
    {
      FATAL_MSG ("Can't open source file '%s'.", config->src_file);
      return (ST_FAILURE);
    }

  DUMP_VAR (file_t, &file);
  
  file.size = lseek64 (file.fd, 0, SEEK_END);

  file_chunks_init (&file, PROT_READ, MAP_PRIVATE, MAX_BLOCK_SIZE);
  
  status = create_client_socket (&file);

  file_chunks_cancel (&file);
  close (file.fd);

  DEBUG_MSG ("Exiting client.");
  
  return (status);
}
