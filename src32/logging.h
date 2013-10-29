#ifndef LOGGING_H
#define LOGGING_H

#ifdef HAVE_CONFIG_H
#include <config.h>
#endif /* HAVE_CONFIG */

#define DEFAULT_LOG_LEVEL (LL_INFO)

#if !defined (COMPILE_LOG_LEVEL_LL_ALL) && !defined (COMPILE_LOG_LEVEL_LL_TRACE) && !defined (COMPILE_LOG_LEVEL_LL_DEBUG) && !defined (COMPILE_LOG_LEVEL_LL_INFO) && !defined (COMPILE_LOG_LEVEL_LL_WARN) && !defined (COMPILE_LOG_LEVEL_LL_ERROR) && !defined (COMPILE_LOG_LEVEL_LL_FATAL) && !defined (COMPILE_LOG_LEVEL_LL_OFF)
#define COMPILE_LOG_LEVEL_LL_OFF
#endif /* COMPILE_LOG_LEVEL_LL_XXX */

#include <metaresc.h>

TYPEDEF_ENUM (log_level_t,
	      LL_ALL,
	      LL_TRACE,
	      LL_DEBUG,
	      LL_INFO,
	      LL_WARN,
	      LL_ERROR,
	      LL_FATAL,
	      LL_OFF,
	      )

extern void set_log_level (log_level_t log_level);
extern log_level_t get_log_level (char * log_level);
extern void log_message (const char * filename, const char * function, int line, log_level_t log_level, const char * template, ...);

#define LOG_MSG(LOG_LEVEL, ...) log_message (__FILE__, __PRETTY_FUNCTION__, __LINE__, LOG_LEVEL, __VA_ARGS__)
#define _TRACE_MSG(...) LOG_MSG (LL_TRACE, __VA_ARGS__)
#define _DEBUG_MSG(...) LOG_MSG (LL_DEBUG, __VA_ARGS__)
#define _INFO_MSG(...) LOG_MSG (LL_INFO, __VA_ARGS__)
#define _WARN_MSG(...) LOG_MSG (LL_WARN, __VA_ARGS__)
#define _ERROR_MSG(...) LOG_MSG (LL_ERROR, __VA_ARGS__)
#define _FATAL_MSG(...) LOG_MSG (LL_FATAL, __VA_ARGS__)
#define _OFF_MSG(...)

#ifdef COMPILE_LOG_LEVEL_LL_ALL
#define TRACE_MSG _TRACE_MSG
#define DEBUG_MSG _DEBUG_MSG
#define INFO_MSG _INFO_MSG
#define WARN_MSG _WARN_MSG
#define ERROR_MSG _ERROR_MSG
#define FATAL_MSG _FATAL_MSG
#endif /* COMPILE_LOG_LEVEL_LL_ALL */

#ifdef COMPILE_LOG_LEVEL_LL_TRACE
#define TRACE_MSG _TRACE_MSG
#define DEBUG_MSG _DEBUG_MSG
#define INFO_MSG _INFO_MSG
#define WARN_MSG _WARN_MSG
#define ERROR_MSG _ERROR_MSG
#define FATAL_MSG _FATAL_MSG
#endif /* COMPILE_LOG_LEVEL_LL_TRACE */

#ifdef COMPILE_LOG_LEVEL_LL_DEBUG
#define TRACE_MSG _OFF_MSG
#define DEBUG_MSG _DEBUG_MSG
#define INFO_MSG _INFO_MSG
#define WARN_MSG _WARN_MSG
#define ERROR_MSG _ERROR_MSG
#define FATAL_MSG _FATAL_MSG
#endif /* COMPILE_LOG_LEVEL_LL_DEBUG */

#ifdef COMPILE_LOG_LEVEL_LL_INFO
#define TRACE_MSG _OFF_MSG
#define DEBUG_MSG _OFF_MSG
#define INFO_MSG _INFO_MSG
#define WARN_MSG _WARN_MSG
#define ERROR_MSG _ERROR_MSG
#define FATAL_MSG _FATAL_MSG
#endif /* COMPILE_LOG_LEVEL_LL_INFO */

#ifdef COMPILE_LOG_LEVEL_LL_WARN
#define TRACE_MSG _OFF_MSG
#define DEBUG_MSG _OFF_MSG
#define INFO_MSG _OFF_MSG
#define WARN_MSG _WARN_MSG
#define ERROR_MSG _ERROR_MSG
#define FATAL_MSG _FATAL_MSG
#endif /* COMPILE_LOG_LEVEL_LL_WARN */

#ifdef COMPILE_LOG_LEVEL_LL_ERROR
#define TRACE_MSG _OFF_MSG
#define DEBUG_MSG _OFF_MSG
#define INFO_MSG _OFF_MSG
#define WARN_MSG _OFF_MSG
#define ERROR_MSG _ERROR_MSG
#define FATAL_MSG _FATAL_MSG
#endif /* COMPILE_LOG_LEVEL_LL_ERROR */

#ifdef COMPILE_LOG_LEVEL_LL_FATAL
#define TRACE_MSG _OFF_MSG
#define DEBUG_MSG _OFF_MSG
#define INFO_MSG _OFF_MSG
#define WARN_MSG _OFF_MSG
#define ERROR_MSG _OFF_MSG
#define FATAL_MSG _FATAL_MSG
#endif /* COMPILE_LOG_LEVEL_LL_FATAL */

#ifdef COMPILE_LOG_LEVEL_LL_OFF
#define TRACE_MSG _OFF_MSG
#define DEBUG_MSG _OFF_MSG
#define INFO_MSG _OFF_MSG
#define WARN_MSG _OFF_MSG
#define ERROR_MSG _OFF_MSG
#define FATAL_MSG _OFF_MSG
#endif /* COMPILE_LOG_LEVEL_LL_OFF */

#endif /* LOGGING_H */
