#ifndef _H_RR_SHA256_
#define _H_RR_SHA256_

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>

#define RR_SHA256_DIGEST_SIZE 32
#define RR_SHA256_HEX_SIZE    65

typedef struct RRSHA256
{
  uint32_t state    [8];
  uint64_t totalSize;
  uint8_t  block    [64];
  size_t   blockSize;
}
RRSHA256;

void rr_sha256_init  (RRSHA256 *ctx);
void rr_sha256_update(RRSHA256 *ctx, const void *data, size_t size);
void rr_sha256_final (RRSHA256 *ctx,
  uint8_t digest[RR_SHA256_DIGEST_SIZE]);

/*
 * If rewindFile is true, seek to the start before reading. The stream is left
 * at EOF. On failure, error receives an errno-compatible value when non-NULL.
 */
bool rr_sha256_file(FILE *fp, bool rewindFile,
  uint8_t digest[RR_SHA256_DIGEST_SIZE], int *error);
void rr_sha256_hex(const uint8_t digest[RR_SHA256_DIGEST_SIZE],
  char hex[RR_SHA256_HEX_SIZE]);

#endif
