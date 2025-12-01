#ifndef _H_RR_UTIL_
#define _H_RR_UTIL_

#include <time.h>
#include <stdint.h>

#define ARRAY_SIZE(x) (sizeof((x)) / sizeof(*(x)))

static inline uint64_t rr_microtime(void)
{
  struct timespec time;
  clock_gettime(CLOCK_MONOTONIC, &time);
  return (uint64_t)time.tv_sec * 1000000LL + time.tv_nsec / 1000LL;
}

#endif
