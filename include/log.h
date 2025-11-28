#ifndef _H_RR_LOG_
#define _H_RR_LOG_

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <stdlib.h>
#include <stdio.h>
#include <inttypes.h>
#include "time.h"

enum DebugLevel
{
  LOG_LEVEL_NONE,
  LOG_LEVEL_INFO,
  LOG_LEVEL_WARN,
  LOG_LEVEL_ERROR,
  LOG_LEVEL_TRACE,
  LOG_LEVEL_FIXME,
  LOG_LEVEL_FATAL
};

void log_init(void);
void log_enableTracing(void);

// platform specific debug initialization
void platform_debugInit(void);

#ifdef ENABLE_BACKTRACE
void printBacktrace(void);
#define LOG_PRINT_BACKTRACE() printBacktrace()
#else
#define LOG_PRINT_BACKTRACE()
#endif

#if defined(_WIN32) && !defined(__GNUC__)
  #define DIRECTORY_SEPARATOR '\\'
#else
  #define DIRECTORY_SEPARATOR '/'
#endif

#ifdef __GNUC__
  #define LOG_UNREACHABLE_MARKER() __builtin_unreachable()
#elif defined(_MSC_VER)
  #define LOG_UNREACHABLE_MARKER() __assume(0)
#else
  #define LOG_UNREACHABLE_MARKER()
#endif

void log_level(enum DebugLevel level, const char * file, unsigned int line,
    const char * function, const char * format, ...)
  __attribute__((format (printf, 5, 6)));

void log_info(const char * file, unsigned int line, const char * function,
    const char * format, ...) __attribute__((format (printf, 4, 5)));

void log_warn(const char * file, unsigned int line, const char * function,
    const char * format, ...) __attribute__((format (printf, 4, 5)));

void log_error(const char * file, unsigned int line, const char * function,
    const char * format, ...) __attribute__((format (printf, 4, 5)));

void log_trace(const char * file, unsigned int line, const char * function,
    const char * format, ...) __attribute__((format (printf, 4, 5)));

#define STRIPPATH(s) ( \
  sizeof(s) >  2 && (s)[sizeof(s)- 3] == DIRECTORY_SEPARATOR ? (s) + sizeof(s) -  2 : \
  sizeof(s) >  3 && (s)[sizeof(s)- 4] == DIRECTORY_SEPARATOR ? (s) + sizeof(s) -  3 : \
  sizeof(s) >  4 && (s)[sizeof(s)- 5] == DIRECTORY_SEPARATOR ? (s) + sizeof(s) -  4 : \
  sizeof(s) >  5 && (s)[sizeof(s)- 6] == DIRECTORY_SEPARATOR ? (s) + sizeof(s) -  5 : \
  sizeof(s) >  6 && (s)[sizeof(s)- 7] == DIRECTORY_SEPARATOR ? (s) + sizeof(s) -  6 : \
  sizeof(s) >  7 && (s)[sizeof(s)- 8] == DIRECTORY_SEPARATOR ? (s) + sizeof(s) -  7 : \
  sizeof(s) >  8 && (s)[sizeof(s)- 9] == DIRECTORY_SEPARATOR ? (s) + sizeof(s) -  8 : \
  sizeof(s) >  9 && (s)[sizeof(s)-10] == DIRECTORY_SEPARATOR ? (s) + sizeof(s) -  9 : \
  sizeof(s) > 10 && (s)[sizeof(s)-11] == DIRECTORY_SEPARATOR ? (s) + sizeof(s) - 10 : \
  sizeof(s) > 11 && (s)[sizeof(s)-12] == DIRECTORY_SEPARATOR ? (s) + sizeof(s) - 11 : \
  sizeof(s) > 12 && (s)[sizeof(s)-13] == DIRECTORY_SEPARATOR ? (s) + sizeof(s) - 12 : \
  sizeof(s) > 13 && (s)[sizeof(s)-14] == DIRECTORY_SEPARATOR ? (s) + sizeof(s) - 13 : \
  sizeof(s) > 14 && (s)[sizeof(s)-15] == DIRECTORY_SEPARATOR ? (s) + sizeof(s) - 14 : \
  sizeof(s) > 15 && (s)[sizeof(s)-16] == DIRECTORY_SEPARATOR ? (s) + sizeof(s) - 15 : \
  sizeof(s) > 16 && (s)[sizeof(s)-17] == DIRECTORY_SEPARATOR ? (s) + sizeof(s) - 16 : \
  sizeof(s) > 17 && (s)[sizeof(s)-18] == DIRECTORY_SEPARATOR ? (s) + sizeof(s) - 17 : \
  sizeof(s) > 18 && (s)[sizeof(s)-19] == DIRECTORY_SEPARATOR ? (s) + sizeof(s) - 18 : \
  sizeof(s) > 19 && (s)[sizeof(s)-20] == DIRECTORY_SEPARATOR ? (s) + sizeof(s) - 19 : \
  sizeof(s) > 20 && (s)[sizeof(s)-21] == DIRECTORY_SEPARATOR ? (s) + sizeof(s) - 20 : \
  sizeof(s) > 21 && (s)[sizeof(s)-22] == DIRECTORY_SEPARATOR ? (s) + sizeof(s) - 21 : (s))

#define LOG_PRINT(level, fmt, ...) do { \
  log_level(level, STRIPPATH(__FILE__), __LINE__, __FUNCTION__, \
      fmt, ##__VA_ARGS__); \
} while (0)

#define LOG_BREAK() LOG_PRINT(LOG_LEVEL_INFO, "================================================================================")
#define LOG_INFO(fmt, ...) LOG_PRINT(LOG_LEVEL_INFO, fmt, ##__VA_ARGS__)
#define LOG_WARN(fmt, ...) LOG_PRINT(LOG_LEVEL_WARN, fmt, ##__VA_ARGS__)
#define LOG_ERROR(fmt, ...) LOG_PRINT(LOG_LEVEL_ERROR, fmt, ##__VA_ARGS__)
#define LOG_TRACE(fmt, ...) LOG_PRINT(LOG_LEVEL_TRACE, fmt, ##__VA_ARGS__)
#define LOG_FIXME(fmt, ...) LOG_PRINT(LOG_LEVEL_FIXME, fmt, ##__VA_ARGS__)
#define LOG_FATAL(fmt, ...) do { \
  LOG_BREAK(); \
  LOG_PRINT(LOG_LEVEL_FATAL, fmt, ##__VA_ARGS__); \
  LOG_PRINT_BACKTRACE(); \
  abort(); \
  LOG_UNREACHABLE_MARKER(); \
} while(0)

#define LOG_ASSERT_PRINT(...) LOG_ERROR("Assertion failed: %s", #__VA_ARGS__)

#define LOG_ASSERT(...) do { \
  if (!(__VA_ARGS__)) \
  { \
    LOG_ASSERT_PRINT(__VA_ARGS__); \
    abort(); \
  } \
} while (0)

#define LOG_UNREACHABLE() LOG_FATAL("Unreachable code reached")

#if defined(LOG_SPICE) | defined(LOG_IVSHMEM)
  #define LOG_PROTO(fmt, args...) LOG_PRINT("[P]", fmt, ##args)
#else
  #define LOG_PROTO(fmt, ...) do {} while(0)
#endif

#endif
