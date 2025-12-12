#ifndef _H_RR_UTIL_
#define _H_RR_UTIL_

#include <time.h>
#include <stdint.h>
#include <string.h>
#include <stdbool.h>
#include <stdalign.h>
#include <stdarg.h>
#include <sys/types.h>
#include <arpa/inet.h>

#define ARRAY_SIZE(x) (sizeof((x)) / sizeof(*(x)))
#define MAX(a, b) (((a) > (b)) ? (a) : (b))
#define MIN(a, b) (((a) > (b)) ? (b) : (a))

typedef struct RRBuffer
{
  char  *buffer;
  size_t bufferSz;
  size_t pos;
}
RRBuffer;

ssize_t rr_alloc_vsprintf   (RRBuffer *buf, const char *fmt, va_list ap);
ssize_t rr_alloc_sprintf    (RRBuffer *buf, const char *fmt, ...);
ssize_t rr_buffer_append_str(RRBuffer *buf, const char *str);
bool    rr_buffer_appendf   (RRBuffer *buf, const char *fmt, ...);
void    rr_buffer_reset     (RRBuffer *buf);
void    rr_buffer_free      (RRBuffer *buf);

bool    rr_sanatize          (char *text, size_t maxLen);
int     rr_parse_ipv4_decimal(const char *str, uint32_t *host);
int     rr_parse_ipv6_decimal(const char *str, unsigned __int128 *host);
uint8_t rr_ipv4_to_cidr      (const uint32_t start, const uint32_t end);
uint8_t rr_ipv6_to_cidr      (const unsigned __int128 start, const unsigned __int128 end);
bool    rr_calc_ipv4_cidr_end(uint32_t start, unsigned prefix_len, uint32_t *end_out);
bool    rr_calc_ipv6_cidr_end(const unsigned __int128 *start, unsigned prefix_len, unsigned __int128 *end_out);

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

static inline unsigned __int128 rr_bswap128(unsigned __int128 x)
{
  uint64_t lo = (uint64_t)x;
  uint64_t hi = (uint64_t)(x >> 64);
  return ((unsigned __int128)__builtin_bswap64(lo) << 64) |
         (unsigned __int128)__builtin_bswap64(hi);
}

static inline unsigned __int128 rr_raw_to_be(unsigned __int128 raw)
{
#if defined(__BYTE_ORDER__) && (__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__)
  return rr_bswap128(raw);
#else
  return raw;
#endif
}

static inline unsigned __int128 rr_be_to_raw(unsigned __int128 be)
{
#if defined(__BYTE_ORDER__) && (__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__)
  return rr_bswap128(be);
#else
  return be;
#endif
}

static inline void rr_ntop_u128_raw(char *dst, size_t dstlen, unsigned __int128 raw)
{
  struct in6_addr a;
  memcpy(&a, &raw, 16);
  inet_ntop(AF_INET6, &a, dst, dstlen);
}

static inline uint8_t rr_u128_ctz_be(unsigned __int128 x)
{
  if (x == 0) return 128;

  uint64_t lo = (uint64_t)x;
  if (lo) return (uint8_t)__builtin_ctzll((unsigned long long)lo);

  uint64_t hi = (uint64_t)(x >> 64);
  return (uint8_t)(64u + (uint8_t)__builtin_ctzll((unsigned long long)hi));
}

static inline uint8_t rr_u128_msb_be(unsigned __int128 x)
{
  // floor(log2(x)), x != 0
  uint64_t hi = (uint64_t)(x >> 64);
  if (hi)
    return (uint8_t)(64u + (uint8_t)(63u - (uint8_t)__builtin_clzll((unsigned long long)hi)));

  uint64_t lo = (uint64_t)x;
  return (uint8_t)(63u - (uint8_t)__builtin_clzll((unsigned long long)lo));
}

static inline size_t rr_align_up(size_t off, size_t a)
{
  return (off + (a - 1)) & ~(a - 1);
}

static inline uint64_t rr_lowbit_u32(uint32_t x)
{
  return x ? (uint64_t)(x & (~x + 1u)) : (1ULL << 32);
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
