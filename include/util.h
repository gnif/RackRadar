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
int rr_parse_ipv6_decimal(const char *str, unsigned __int128 *host);
uint8_t rr_ipv4_to_cidr(const uint32_t start, const uint32_t end);
uint8_t rr_ipv6_to_cidr(const unsigned __int128 start, const unsigned __int128 end);
void rr_calc_ipv6_cidr_end(const unsigned __int128 *start, unsigned prefix_len, unsigned __int128 *end_out);

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

#endif
