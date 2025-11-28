#include "log.h"
#include "util.h"

#include <stdarg.h>
#include <stdio.h>
#include <string.h>
#include <stdbool.h>
#include <unistd.h>

#define COLOR_RESET  "\033[0m"
#define COLOR_YELLOW "\033[0;33m"
#define COLOR_RED    "\033[0;31m"
#define COLOR_CYAN   "\033[0;36m"
#define COLOR_WHITE  "\033[0;37m"
#define COLOR_GREY   "\033[0;90m"



static const char ** log_lookup = NULL;
static uint64_t startTime;
static bool     traceEnabled = false;

void log_init(void)
{
  startTime = microtime();
  static const char * colorLookup[] =
  {
    COLOR_RESET        , // DEBUG_LEVEL_NONE
    COLOR_RESET  "[I] ", // DEBUG_LEVEL_INFO
    COLOR_YELLOW "[W] ", // DEBUG_LEVEL_WARN
    COLOR_RED    "[E] ", // DEBUG_LEVEL_ERROR
    COLOR_GREY   "[T] ", // DEBUG_LEVEL_TRACE
    COLOR_CYAN   "[F] ", // DEBUG_LEVEL_FIXME
    COLOR_WHITE  "[!] "  // DEBUG_LEVEL_FATAL
  };

  static const char * plainLookup[] =
  {
    ""    , // DEBUG_LEVEL_NONE
    "[I] ", // DEBUG_LEVEL_INFO
    "[W] ", // DEBUG_LEVEL_WARN
    "[E] ", // DEBUG_LEVEL_ERROR
    "[T] ", // DEBUG_LEVEL_TRACE
    "[F] ", // DEBUG_LEVEL_FIXME
    "[!] "  // DEBUG_LEVEL_FATAL
  };

  log_lookup = (isatty(STDERR_FILENO) == 1) ? colorLookup : plainLookup;
}

void log_enableTracing(void)
{
  traceEnabled = true;
}

inline static void log_levelVA(enum DebugLevel level, const char * file,
    unsigned int line, const char * function, const char * format, va_list va)
{
  if (level == LOG_LEVEL_TRACE && !traceEnabled)
    return;

  const char * f = strrchr(file, DIRECTORY_SEPARATOR);
  if (!f)
    f = file;
  else
    ++f;

  uint64_t elapsed = microtime() - startTime;
  uint64_t sec     = elapsed / 1000000UL;
  uint64_t us      = elapsed % 1000000UL;

  fprintf(stderr, "%02u:%02u:%02u.%03u %s %18s:%-4u | %-30s | ",
      (unsigned)(sec / 60 / 60),
      (unsigned)(sec / 60 % 60),
      (unsigned)(sec % 60),
      (unsigned)(us / 1000),
      log_lookup[level],
      f,
      line, function);

  vfprintf(stderr, format, va);
  fprintf(stderr, "%s\n", log_lookup[LOG_LEVEL_NONE]);
}

void log_level(enum DebugLevel level, const char * file, unsigned int line,
    const char * function, const char * format, ...)
{
  va_list va;
  va_start(va, format);
  log_levelVA(level, file, line, function, format, va);
  va_end(va);
}

void log_info(const char * file, unsigned int line, const char * function,
    const char * format, ...)
{
  va_list va;
  va_start(va, format);
  log_levelVA(LOG_LEVEL_INFO, file, line, function, format, va);
  va_end(va);
}

void log_warn(const char * file, unsigned int line, const char * function,
    const char * format, ...)
{
  va_list va;
  va_start(va, format);
  log_levelVA(LOG_LEVEL_WARN, file, line, function, format, va);
  va_end(va);
}

void log_error(const char * file, unsigned int line, const char * function,
    const char * format, ...)
{
  va_list va;
  va_start(va, format);
  log_levelVA(LOG_LEVEL_ERROR, file, line, function, format, va);
  va_end(va);
}

void log_trace(const char * file, unsigned int line, const char * function,
    const char * format, ...)
{
  va_list va;
  va_start(va, format);
  log_levelVA(LOG_LEVEL_INFO, file, line, function, format, va);
  va_end(va);
}
