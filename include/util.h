#ifndef _H_RR_UTIL_
#define _H_RR_UTIL_

#include <time.h>
#include <stdint.h>
#include <string.h>
#include <stdbool.h>
#include <arpa/inet.h>

#define ARRAY_SIZE(x) (sizeof((x)) / sizeof(*(x)))
#define MAX(a, b) (((a) > (b)) ? (a) : (b))
#define MIN(a, b) (((a) > (b)) ? (b) : (a))

bool rr_sanatize(char *text, size_t maxLen);
int rr_parse_ipv4_decimal(const char *str, uint32_t *host);

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

static inline uint8_t rr_ipv6_to_cidr(const unsigned __int128 start, const unsigned __int128 end)
{
  unsigned __int128 diff = start ^ end;
  if (diff == 0)
    return 128;

#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
  // Common prefix of the original IPv6 byte stream == common *low* bits of this integer.
  uint64_t lo = (uint64_t)diff;
  if (lo != 0)
    return (uint8_t)__builtin_ctzll((unsigned long long)lo);

  uint64_t hi = (uint64_t)(diff >> 64); // must be non-zero here
  return (uint8_t)(64 + __builtin_ctzll((unsigned long long)hi));
#else
  // On big-endian, memcpy-order matches numeric MSB-first significance, so CLZ is correct.
  uint64_t hi = (uint64_t)(diff >> 64);
  if (hi != 0)
    return (uint8_t)__builtin_clzll((unsigned long long)hi);

  uint64_t lo2 = (uint64_t)diff; // must be non-zero here
  return (uint8_t)(64 + __builtin_clzll((unsigned long long)lo2));
#endif
}

#endif
