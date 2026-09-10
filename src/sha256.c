#include "sha256.h"

#include <errno.h>
#include <string.h>

static const uint32_t SHA256_CONSTANTS[64] =
{
  0x428a2f98U, 0x71374491U, 0xb5c0fbcfU, 0xe9b5dba5U,
  0x3956c25bU, 0x59f111f1U, 0x923f82a4U, 0xab1c5ed5U,
  0xd807aa98U, 0x12835b01U, 0x243185beU, 0x550c7dc3U,
  0x72be5d74U, 0x80deb1feU, 0x9bdc06a7U, 0xc19bf174U,
  0xe49b69c1U, 0xefbe4786U, 0x0fc19dc6U, 0x240ca1ccU,
  0x2de92c6fU, 0x4a7484aaU, 0x5cb0a9dcU, 0x76f988daU,
  0x983e5152U, 0xa831c66dU, 0xb00327c8U, 0xbf597fc7U,
  0xc6e00bf3U, 0xd5a79147U, 0x06ca6351U, 0x14292967U,
  0x27b70a85U, 0x2e1b2138U, 0x4d2c6dfcU, 0x53380d13U,
  0x650a7354U, 0x766a0abbU, 0x81c2c92eU, 0x92722c85U,
  0xa2bfe8a1U, 0xa81a664bU, 0xc24b8b70U, 0xc76c51a3U,
  0xd192e819U, 0xd6990624U, 0xf40e3585U, 0x106aa070U,
  0x19a4c116U, 0x1e376c08U, 0x2748774cU, 0x34b0bcb5U,
  0x391c0cb3U, 0x4ed8aa4aU, 0x5b9cca4fU, 0x682e6ff3U,
  0x748f82eeU, 0x78a5636fU, 0x84c87814U, 0x8cc70208U,
  0x90befffaU, 0xa4506cebU, 0xbef9a3f7U, 0xc67178f2U
};

static uint32_t rr_sha256_rotate_right(uint32_t value, unsigned amount)
{
  return (value >> amount) | (value << (32U - amount));
}

static uint32_t rr_sha256_load_be32(const uint8_t *data)
{
  return ((uint32_t)data[0] << 24) |
         ((uint32_t)data[1] << 16) |
         ((uint32_t)data[2] <<  8) |
          (uint32_t)data[3];
}

static void rr_sha256_transform(RRSHA256 *ctx, const uint8_t block[64])
{
  uint32_t schedule[64];

  for (size_t i = 0; i < 16; ++i)
    schedule[i] = rr_sha256_load_be32(block + i * 4);

  for (size_t i = 16; i < 64; ++i)
  {
    const uint32_t s0 = rr_sha256_rotate_right(schedule[i - 15],  7) ^
                        rr_sha256_rotate_right(schedule[i - 15], 18) ^
                        (schedule[i - 15] >> 3);
    const uint32_t s1 = rr_sha256_rotate_right(schedule[i - 2], 17) ^
                        rr_sha256_rotate_right(schedule[i - 2], 19) ^
                        (schedule[i - 2] >> 10);

    schedule[i] = schedule[i - 16] + s0 + schedule[i - 7] + s1;
  }

  uint32_t a = ctx->state[0];
  uint32_t b = ctx->state[1];
  uint32_t c = ctx->state[2];
  uint32_t d = ctx->state[3];
  uint32_t e = ctx->state[4];
  uint32_t f = ctx->state[5];
  uint32_t g = ctx->state[6];
  uint32_t h = ctx->state[7];

  for (size_t i = 0; i < 64; ++i)
  {
    const uint32_t s1       = rr_sha256_rotate_right(e,  6) ^
                              rr_sha256_rotate_right(e, 11) ^
                              rr_sha256_rotate_right(e, 25);
    const uint32_t choice   = (e & f) ^ (~e & g);
    const uint32_t temp1    = h + s1 + choice + SHA256_CONSTANTS[i] +
                              schedule[i];
    const uint32_t s0       = rr_sha256_rotate_right(a,  2) ^
                              rr_sha256_rotate_right(a, 13) ^
                              rr_sha256_rotate_right(a, 22);
    const uint32_t majority = (a & b) ^ (a & c) ^ (b & c);
    const uint32_t temp2    = s0 + majority;

    h = g;
    g = f;
    f = e;
    e = d + temp1;
    d = c;
    c = b;
    b = a;
    a = temp1 + temp2;
  }

  ctx->state[0] += a;
  ctx->state[1] += b;
  ctx->state[2] += c;
  ctx->state[3] += d;
  ctx->state[4] += e;
  ctx->state[5] += f;
  ctx->state[6] += g;
  ctx->state[7] += h;
}

void rr_sha256_init(RRSHA256 *ctx)
{
  ctx->state[0]  = 0x6a09e667U;
  ctx->state[1]  = 0xbb67ae85U;
  ctx->state[2]  = 0x3c6ef372U;
  ctx->state[3]  = 0xa54ff53aU;
  ctx->state[4]  = 0x510e527fU;
  ctx->state[5]  = 0x9b05688cU;
  ctx->state[6]  = 0x1f83d9abU;
  ctx->state[7]  = 0x5be0cd19U;
  ctx->totalSize = 0;
  ctx->blockSize = 0;
}

void rr_sha256_update(RRSHA256 *ctx, const void *data, size_t size)
{
  const uint8_t *input = data;

  if (!size)
    return;

  ctx->totalSize += size;

  if (ctx->blockSize)
  {
    const size_t copySize = size < sizeof(ctx->block) - ctx->blockSize ?
      size : sizeof(ctx->block) - ctx->blockSize;

    memcpy(ctx->block + ctx->blockSize, input, copySize);
    ctx->blockSize += copySize;
    input          += copySize;
    size           -= copySize;

    if (ctx->blockSize == sizeof(ctx->block))
    {
      rr_sha256_transform(ctx, ctx->block);
      ctx->blockSize = 0;
    }
  }

  while (size >= sizeof(ctx->block))
  {
    rr_sha256_transform(ctx, input);
    input += sizeof(ctx->block);
    size  -= sizeof(ctx->block);
  }

  if (size)
  {
    memcpy(ctx->block, input, size);
    ctx->blockSize = size;
  }
}

void rr_sha256_final(RRSHA256 *ctx,
  uint8_t digest[RR_SHA256_DIGEST_SIZE])
{
  const uint64_t bitLength = ctx->totalSize * 8U;

  ctx->block[ctx->blockSize++] = 0x80U;

  if (ctx->blockSize > 56)
  {
    memset(ctx->block + ctx->blockSize, 0,
      sizeof(ctx->block) - ctx->blockSize);
    rr_sha256_transform(ctx, ctx->block);
    ctx->blockSize = 0;
  }

  memset(ctx->block + ctx->blockSize, 0, 56 - ctx->blockSize);
  for (size_t i = 0; i < 8; ++i)
    ctx->block[63 - i] = (uint8_t)(bitLength >> (i * 8));

  rr_sha256_transform(ctx, ctx->block);

  for (size_t i = 0; i < 8; ++i)
  {
    digest[i * 4    ] = (uint8_t)(ctx->state[i] >> 24);
    digest[i * 4 + 1] = (uint8_t)(ctx->state[i] >> 16);
    digest[i * 4 + 2] = (uint8_t)(ctx->state[i] >>  8);
    digest[i * 4 + 3] = (uint8_t) ctx->state[i];
  }
}

bool rr_sha256_file(FILE *fp, bool rewindFile,
  uint8_t digest[RR_SHA256_DIGEST_SIZE], int *error)
{
  int failure = 0;

  if (!fp || !digest)
  {
    failure = EINVAL;
    goto fail;
  }

  if (rewindFile)
  {
    clearerr(fp);
    errno = 0;
    if (fseeko(fp, 0, SEEK_SET) != 0)
    {
      failure = errno ? errno : EIO;
      goto fail;
    }
  }
  else
    clearerr(fp);

  RRSHA256 ctx;
  uint8_t  buffer[32768];
  rr_sha256_init(&ctx);

  for (;;)
  {
    errno = 0;
    const size_t size = fread(buffer, 1, sizeof(buffer), fp);
    if (size)
      rr_sha256_update(&ctx, buffer, size);

    if (size != sizeof(buffer))
      break;
  }

  if (ferror(fp))
  {
    failure = errno ? errno : EIO;
    goto fail;
  }

  rr_sha256_final(&ctx, digest);
  if (error)
    *error = 0;
  return true;

fail:
  if (error)
    *error = failure;
  errno = failure;
  return false;
}

void rr_sha256_hex(const uint8_t digest[RR_SHA256_DIGEST_SIZE],
  char hex[RR_SHA256_HEX_SIZE])
{
  static const char digits[] = "0123456789abcdef";

  for (size_t i = 0; i < RR_SHA256_DIGEST_SIZE; ++i)
  {
    hex[i * 2    ] = digits[digest[i] >> 4];
    hex[i * 2 + 1] = digits[digest[i] & 0x0fU];
  }

  hex[RR_SHA256_HEX_SIZE - 1] = '\0';
}
