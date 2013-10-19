#define _GNU_SOURCE /* TEMP_FAILURE_RETRY */

#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <signal.h>
#include <execinfo.h>
#include <pthread.h>
#include <string.h>

#include "logging.h"

#define LL_INIT(LEVEL) [LL_##LEVEL] = #LEVEL
static const char * log_level_str[] =
  {
    LL_INIT (ALL),
    LL_INIT (TRACE),
    LL_INIT (DEBUG),
    LL_INIT (INFO),
    LL_INIT (WARN),
    LL_INIT (ERROR),
    LL_INIT (FATAL),
    LL_INIT (OFF),
  };

static log_level_t current_log_level = DEFAULT_LOG_LEVEL;

void
set_log_level (log_level_t log_level)
{
  current_log_level = log_level;
}

log_level_t
get_log_level (char * log_level)
{
  log_level_t i;
  for (i = 0; i < sizeof (log_level_str) / sizeof (log_level_str[0]); ++i)
    if (log_level_str[i] && (0 == strcasecmp (log_level_str[i], log_level)))
      return (i);

  return (DEFAULT_LOG_LEVEL);
}

void
log_message (const char * filename, const char * function, int line, log_level_t log_level, const char * template, ...)
{
  static sig_atomic_t cnt = 1;
  static pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
  va_list args;

  if (current_log_level <= log_level)
    {
      const char * log_level_str_ = "Unknown";
      if ((log_level >= 0) && (log_level <= sizeof (log_level_str) / sizeof (log_level_str[0])) && log_level_str[log_level])
	log_level_str_ = log_level_str[log_level];

      pthread_mutex_lock (&mutex);
      fprintf (stderr, "[%d] <%s> thread %u file '%s' function '%s' line %d: ", cnt++, log_level_str_, (int)pthread_self (), filename, function, line);
      va_start (args, template);
      vfprintf (stderr, template, args);
      va_end (args);
      fprintf (stderr, "\n");
      
      if (log_level < LL_DEBUG)
	{
	  void * array[8];
	  size_t size;
	  char ** strings;
	  size_t i;

	  size = backtrace (array, sizeof (array) / sizeof (array[0]));
	  strings = backtrace_symbols (array, size);
	  fprintf (stderr, "Obtained %zd stack frames.\n", size);
	  if (strings)
	    {
	      for (i = 0; i < size; ++i)
		fprintf (stderr, "%s\n", strings[i]);
	      free (strings);
	    }
	}
      pthread_mutex_unlock (&mutex);
    }
}
