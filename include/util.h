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

static inline size_t rr_align_up(size_t off, size_t a)
{
  return (off + (a - 1)) & ~(a - 1);
}

#define RR_ARENA_CALC_TOTAL(T, FIELDS, TOTAL_LVAL) do {                       \
  (TOTAL_LVAL) = sizeof(T);                                                   \
  FIELDS(T, RR_ARENA_TOTAL_STEP, (TOTAL_LVAL));                               \
} while (0)

#define RR_ARENA_TOTAL_STEP(T, member, count, TOTAL_LVAL) do {                \
  size_t _a = alignof(*(((T*)0)->member));                                    \
  (TOTAL_LVAL) = rr_align_up((TOTAL_LVAL), _a);                               \
  (TOTAL_LVAL) += sizeof(*(((T*)0)->member)) * (size_t)(count);               \
} while (0)

#define RR_ARENA_INIT(T, PTR, FIELDS, OFF_LVAL) do {                          \
  (OFF_LVAL) = sizeof(T);                                                     \
  FIELDS(T, RR_ARENA_INIT_STEP, (PTR), (OFF_LVAL));                           \
} while (0)

#define RR_ARENA_INIT_STEP(T, member, count, PTR, OFF_LVAL) do {              \
  size_t _a = alignof(*(((T*)0)->member));                                    \
  (OFF_LVAL) = rr_align_up((OFF_LVAL), _a);                                   \
  (PTR)->member = (__typeof__((PTR)->member))((uint8_t *)(PTR) + (OFF_LVAL)); \
  (OFF_LVAL) += sizeof(*(((T*)0)->member)) * (size_t)(count);                 \
} while (0)

#define RR_ARENA_ALLOC_INIT(T, FIELDS, OUT_PTR_LVAL) do {                     \
  size_t _total__, _off__;                                                    \
  RR_ARENA_CALC_TOTAL(T, FIELDS, _total__);                                   \
  (OUT_PTR_LVAL) = (T *)calloc(1, _total__);                                  \
  if ((OUT_PTR_LVAL)) {                                                       \
    RR_ARENA_INIT(T, (OUT_PTR_LVAL), FIELDS, _off__);                         \
  }                                                                           \
} while (0)

#endif
