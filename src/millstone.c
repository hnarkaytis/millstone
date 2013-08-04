#define _LARGEFILE64_SOURCE
#include <stdlib.h>
#include <getopt.h>

#include <millstone.h>
#include <logging.h>
#include <client.h>
#include <server.h>

#define run_server(...) ST_SUCCESS

static status_t
parse_args (int argc, char * argv[], config_t * config)
{
  int c;
  static struct option long_options[] =
    {
      /* These options set a flag. */
      {"compress", no_argument, NULL, 'c'},
      {0, 0, 0, 0}
    };
  /* `getopt_long' stores the option index here. */
  int option_index = 0;

  memset (config, 0, sizeof (*config));
  while ((c = getopt_long (argc, argv, "c", long_options, &option_index)) != -1)
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
	break;
      }
  
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
  status_t status = parse_args (argc, argv, &config);
  if (ST_SUCCESS != status)
    return (EXIT_FAILURE);

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

  return ((ST_SUCCESS != status) ? EXIT_FAILURE : EXIT_SUCCESS);
}
