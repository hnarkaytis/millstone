#define _LARGEFILE64_SOURCE
#include <stdlib.h>
#include <stdbool.h>
#include <getopt.h>

#include <millstone.h>
#include <logging.h>

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
      config->run_mode = RM_CLIENT;
      config->src = argv[optind];
      config->dst = argv[optind + 1];
    }
  else
    {
      ERROR_MSG ("Unexpected number of arguments.\n./%s [OPTION...] - server mode\n%s SRC DST - client mode",
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
      break;
    case RM_CLIENT:
      break;
    default:
      status = ST_FAILURE;
      break;
    }

  return ((ST_SUCCESS != status) ? EXIT_FAILURE : EXIT_SUCCESS);
}


