#ifndef _H_RR_UTIL_
#define _H_RR_UTIL_

#include <time.h>
#include <stdint.h>
#include <string.h>
#include <arpa/inet.h>

#define ARRAY_SIZE(x) (sizeof((x)) / sizeof(*(x)))

static inline uint64_t rr_microtime(void)
{
  struct timespec time;
  clock_gettime(CLOCK_MONOTONIC, &time);
  return (uint64_t)time.tv_sec * 1000000LL + time.tv_nsec / 1000LL;
}

static inline char * rr_trim(char *v)
{
  if (!v)
    return NULL;

  while(*v == ' ' || *v == '\t')
    ++v;

  char *e = v;
  while(*e != '\0')
    ++e;

  if (e > v)
  {
    --e;
    while(*e == ' ' || *e == '\t')
      --e;
    e[1] = '\0';
  }

  return v;
}

static inline void rr_calc_ipv6_cidr_end(
  const unsigned __int128 *start,
  unsigned prefix_len,
  unsigned __int128 *end_out)
{
  const uint8_t *s = (const uint8_t *)start;
  uint8_t       *e = (uint8_t *)end_out;

  memcpy(e, s, 16);

  if (prefix_len >= 128)
    return;

  unsigned full = prefix_len / 8;
  unsigned rem  = prefix_len % 8;

  if (rem)
  {
    uint8_t keep = (uint8_t)(0xFFu << (8u - rem));
    uint8_t host = (uint8_t)~keep;

    e[full] |= host;
    for (unsigned i = full + 1; i < 16; ++i)
      e[i] = 0xFF;
  }
  else
  {
    for (unsigned i = full; i < 16; ++i)
      e[i] = 0xFF;
  }
}

static inline uint8_t rr_ipv4_to_cidr(const uint32_t start, const uint32_t end)
{
  uint32_t diff = start ^ end;
  return (diff == 0) ? 32 : (uint8_t)__builtin_clz((unsigned)diff);
}

#endif
