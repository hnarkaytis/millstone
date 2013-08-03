#ifndef LOGGING_H
#define LOGGING_H

typedef enum log_level_t {
  LL_ALL,
  LL_TRACE,
  LL_DEBUG,
  LL_INFO,
  LL_WARN,
  LL_ERROR,
  LL_FATAL,
  LL_OFF,
} log_level_t;

extern void set_log_level (log_level_t log_level);
extern log_level_t get_log_level (char * log_level);
extern void log_message (const char * filename, const char * function, int line, log_level_t log_level, const char * template, ...);

#define LOG_MSG(LOG_LEVEL, ...) log_message (__FILE__, __PRETTY_FUNCTION__, __LINE__, LOG_LEVEL, __VA_ARGS__)
#define TRACE_MSG(...) LOG_MSG (LL_TRACE, __VA_ARGS__)
#define DEBUG_MSG(...) LOG_MSG (LL_DEBUG, __VA_ARGS__)
#define INFO_MSG(...) LOG_MSG (LL_INFO, __VA_ARGS__)
#define WARN_MSG(...) LOG_MSG (LL_WARN, __VA_ARGS__)
#define ERROR_MSG(...) LOG_MSG (LL_ERROR, __VA_ARGS__)
#define FATAL_MSG(...) LOG_MSG (LL_FATAL, __VA_ARGS__)

#endif /* LOGGING_H */
