#ifdef HAVE_CONFIG_H
#include <config.h>
#endif /* HAVE_CONFIG_H */
#include <logging.h>
#include <client.h>
#include <server.h>
#include <connection.h>
#include <millstone.h>

#include <unistd.h> /* sysconf */
#include <stdlib.h> /* EXIT_*, strtol */
#include <string.h> /* memset, strchr */
#include <getopt.h> /* getopt_long */

#include <pthread.h>

#ifdef HAVE_ZLIB
#include <zlib.h>
#define DEFAULT_COMPRESS_LEVEL (Z_NO_COMPRESSION)
#else /* HAVE_ZLIB */
#define DEFAULT_COMPRESS_LEVEL (0)
#endif /* HAVE_ZLIB */

#define DEFAULT_MEM_THRESHOLD (100)

status_t
start_threads (void * (* handler) (void *), int count, status_t (*nested_handler) (void *), void * arg)
{
  int i;
  pthread_t ids[count];
  status_t status = ST_FAILURE;

  DEBUG_MSG ("Start server workers %d.", count);
  
  for (i = 0; i < count; ++i)
    {
      int rv = pthread_create (&ids[i], NULL, handler, arg);
      if (rv != 0)
	break;
    }

  DEBUG_MSG ("Started %d.", i);
  
  if (i > 0)
    status = nested_handler (arg);

  DEBUG_MSG ("Canceling workers.");

  for (--i ; i >= 0; --i)
    pthread_join (ids[i], NULL);
  
  DEBUG_MSG ("Workers canceled.");
  
  return (status);
}  

void
cpu_id (unsigned i, unsigned regs[4])
{
#ifdef _WIN32
  __cpuid ((int *)regs, (int)i);
#else
  asm volatile
    ("cpuid" : "=a" (regs[0]), "=b" (regs[1]), "=c" (regs[2]), "=d" (regs[3])
     : "a" (i), "c" (0));
  /* ECX is set to zero for CPUID function 4 */
#endif
}

int
get_cores ()
{
  int cores = (long) sysconf (_SC_NPROCESSORS_ONLN);
  unsigned int regs[4];
  char vendor[sizeof (regs)];
  unsigned int * vendor_ui = (void*)vendor;
  int physical, logical;
  
  memset (vendor, 0, sizeof (vendor));

  cpu_id (1, regs);
  logical = (regs[1] >> 16) & 0xff; /* EBX[23:16] */

  cpu_id (0, regs); /* Get vendor */
  vendor_ui[0] = regs[1]; /* EBX */
  vendor_ui[1] = regs[3]; /* EDX */
  vendor_ui[2] = regs[2]; /* ECX */
  
  if (strcmp (vendor, "GenuineIntel") == 0)
    {
      /* Get DCP cache info */
      cpu_id (4, regs);
      physical = ((regs[0] >> 26) & 0x3f) + 1; /* EAX[31:26] + 1 */
    }
  else if (strcmp (vendor, "AuthenticAMD") == 0)
    {
      /* Get NC: Number of CPU cores - 1 */
      cpu_id (0x80000008, regs);
      physical = ((unsigned)(regs[2] & 0xff)) + 1; /* ECX[7:0] + 1 */
    }
  else
    physical = logical;

  return ((cores * physical) / logical);
}

static status_t
parse_args (int argc, char * argv[], config_t * config)
{
  int c;
  static struct option long_options[] =
    {
      /* These options set a flag. */
      {"workers", required_argument, NULL, 'w'},
      {"compress", required_argument, NULL, 'c'},
      {"log-level", required_argument, NULL, 'l'},
      {"memory-threshold", required_argument, NULL, 'm'},
      {0, 0, 0, 0}
    };
  /* `getopt_long' stores the option index here. */
  int option_index = 0;

  memset (config, 0, sizeof (*config));
  config->listen_port = DEFAULT_LISTEN_PORT;
  config->dst_port = DEFAULT_LISTEN_PORT;
  config->mem_threshold = DEFAULT_MEM_THRESHOLD;
  config->compress_level = DEFAULT_COMPRESS_LEVEL;
  config->workers_number = get_cores ();
  
  while ((c = getopt_long (argc, argv, "c:l:w:m:", long_options, &option_index)) != -1)
    switch (c)
      {
      case 0:
	/* If this option set a flag, do nothing else now. */
	if (long_options[option_index].flag != 0)
	  break;
	printf ("option %s", long_options[option_index].name);
	if (optarg)
	  printf (" with arg %s", optarg);
	printf ("\n");
	break;

      case 'c':
	if (optarg)
	  config->compress_level = atoi (optarg);
	break;
	
      case 'w':
	if (optarg)
	  config->workers_number = atoi (optarg);
	break;
	
      case 'm':
	if (optarg)
	  config->mem_threshold = atoi (optarg);
	break;
	
      case 'l':
	{
	  log_level_t log_level = get_log_level (optarg);
	  set_log_level (log_level);
	  DUMP_VAR (log_level_t, &log_level);
	}
	break;
      }

#ifndef HAVE_ZLIB
  config->compress_level = DEFAULT_COMPRESS_LEVEL;
#endif /* HAVE_ZLIB */
  if (config->workers_number <= 0)
    config->workers_number = 1;
  
  if (argc - optind == 0)
    config->run_mode = RM_SERVER;
  else if (argc - optind == 2)
    {
      char * dst = argv[optind + 1];
      char * semicollon = strchr (dst, ':');
      char * slash = strchr (dst, '/');

      config->run_mode = RM_CLIENT;
      config->src_file = argv[optind];
      config->dst_host = dst;

      if (slash)
	{
	  config->dst_file = slash + 1;
	  *slash = 0;
	}
      else
	config->dst_file = config->src_file;
      
      if (semicollon)
	{
	  char * end;
	  config->dst_port = strtol (semicollon + 1, &end, 10);
	  if (0 != *end)
	    {
	      ERROR_MSG ("Can't parse port number from '%s'", dst);
	      return (ST_FAILURE);
	    }
	  *semicollon = 0;
	}
    }
  else
    {
      ERROR_MSG ("Unexpected number of arguments.\n%s [OPTION...] - server mode\n%s SRC DST - client mode",
		 argv[0], argv[0]);
      return (ST_FAILURE);
    }
  return (ST_SUCCESS);
}

int main (int argc, char * argv[])
{
  config_t config;

  DEBUG_MSG ("Start Millstone. Parse params.");
  
  status_t status = parse_args (argc, argv, &config);
  if (ST_SUCCESS != status)
    return (EXIT_FAILURE);

  DUMP_VAR (config_t, &config);
  
  switch (config.run_mode)
    {
    case RM_SERVER:
      status = run_server (&config);
      break;
    case RM_CLIENT:
      status = run_client (&config);
      break;
    default:
      status = ST_FAILURE;
      break;
    }
  
  DEBUG_MSG ("Stop Millstone.");

  return ((ST_SUCCESS != status) ? EXIT_FAILURE : EXIT_SUCCESS);
}
