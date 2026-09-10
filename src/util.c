#include "util.h"
#include "log.h"

#include <stdlib.h>
#include <ctype.h>
#include <strings.h>
#include <stdarg.h>
#include <unicode/ucsdet.h>
#include <unicode/ucnv.h>
#include <unicode/utypes.h>

ssize_t rr_alloc_vsprintf(RRBuffer *buf, const char *fmt, va_list ap)
{
  if (!buf)
    return -1;

  size_t needed = 0;
  va_list ap_copy;
  va_copy(ap_copy, ap);
  needed = (size_t)vsnprintf(NULL, 0, fmt, ap_copy) + 1; // include NUL
  va_end(ap_copy);

  size_t offset = buf->pos;
  size_t required = offset + needed;
  if (!buf->buffer || buf->bufferSz < required)
  {
    size_t newSize = buf->bufferSz ? buf->bufferSz : 256;
    while (newSize < required)
      newSize *= 2;

    char *newBuffer = realloc(buf->buffer, newSize);
    if (!newBuffer)
    {
      LOG_ERROR("out of memory");
      return -1;
    }

    buf->buffer   = newBuffer;
    buf->bufferSz = newSize;
  }

  int written = vsnprintf(buf->buffer + offset, buf->bufferSz - offset, fmt, ap);

  if (written < 0)
    return -1;

  buf->pos = offset + (size_t)written;
  return (ssize_t)buf->pos;
}

ssize_t rr_alloc_sprintf(RRBuffer *buf, const char *fmt, ...)
{
  va_list ap;
  va_start(ap, fmt);
  ssize_t ret = rr_alloc_vsprintf(buf, fmt, ap);
  va_end(ap);
  return ret;
}

ssize_t rr_buffer_append_str(RRBuffer *buf, const char *str)
{
  if (!buf || !str)
    return -1;

  size_t offset = buf->pos;
  size_t len = strlen(str);
  size_t required = offset + len + 1; // include NUL

  if (!buf->buffer || buf->bufferSz < required)
  {
    size_t newSize = buf->bufferSz ? buf->bufferSz : 256;
    while (newSize < required)
      newSize *= 2;

    char *newBuffer = realloc(buf->buffer, newSize);
    if (!newBuffer)
    {
      LOG_ERROR("out of memory");
      return -1;
    }

    buf->buffer   = newBuffer;
    buf->bufferSz = newSize;
  }

  memcpy(buf->buffer + offset, str, len + 1);
  buf->pos = offset + len;
  return (ssize_t)buf->pos;
}

bool rr_buffer_appendf(RRBuffer *buf, const char *fmt, ...)
{
  va_list ap;
  va_start(ap, fmt);
  ssize_t new_pos = rr_alloc_vsprintf(buf, fmt, ap);
  va_end(ap);
  if (new_pos < 0)
    return false;

  return true;
}

void rr_buffer_reset(RRBuffer *buf)
{
  buf->pos = 0;
  if (buf->buffer)
    buf->buffer[0] = '\0';
}

void rr_buffer_free(RRBuffer *buf)
{
  free(buf->buffer);
  buf->buffer = NULL;
  buf->bufferSz = 0;
  buf->pos = 0;
}

static bool rr_email_is_alnum(char c)
{
  return
    (c >= 'a' && c <= 'z') ||
    (c >= 'A' && c <= 'Z') ||
    (c >= '0' && c <= '9');
}

static bool rr_email_is_local_char(char c)
{
  return
    rr_email_is_alnum(c) ||
    c == '!' || c == '#' || c == '$' || c == '%' || c == '&' ||
    c == '\'' || c == '*' || c == '+' || c == '-' || c == '/' ||
    c == '=' || c == '?' || c == '^' || c == '_' || c == '`' ||
    c == '{' || c == '|' || c == '}' || c == '~' || c == '.';
}

static bool rr_email_is_domain_char(char c)
{
  return rr_email_is_alnum(c) || c == '-' || c == '.';
}

static bool rr_email_is_token_char(char c)
{
  return rr_email_is_local_char(c) || rr_email_is_domain_char(c) || c == '@';
}

static bool rr_email_is_valid(const char *email, size_t at, size_t len)
{
  const size_t localLen  = at;
  const size_t domainLen = len - at - 1;

  if (localLen == 0 || localLen > 64 ||
      domainLen == 0 || domainLen > 253 || len > 254)
    return false;

  if (email[0] == '.' || email[at - 1] == '.')
    return false;

  for(size_t i = 1; i < at; ++i)
    if (email[i] == '.' && email[i - 1] == '.')
      return false;

  bool   sawDot     = false;
  size_t labelStart = at + 1;
  for(size_t i = labelStart; i <= len; ++i)
  {
    if (i < len && email[i] != '.')
      continue;

    const size_t labelLen = i - labelStart;
    if (labelLen == 0 || labelLen > 63 ||
        !rr_email_is_alnum(email[labelStart]) ||
        !rr_email_is_alnum(email[i - 1]))
      return false;

    if (i < len)
    {
      sawDot     = true;
      labelStart = i + 1;
    }
  }

  return sawDot;
}

static bool rr_email_list_contains(
  const char *list, size_t listLen, const char *email, size_t emailLen)
{
  size_t lineStart = 0;
  for(size_t i = 0; i <= listLen; ++i)
  {
    if (i < listLen && list[i] != '\n')
      continue;

    const size_t lineLen = i - lineStart;
    if (lineLen == emailLen &&
        strncasecmp(list + lineStart, email, emailLen) == 0)
      return true;

    lineStart = i + 1;
  }

  return false;
}

static void rr_email_list_append(
  char *dst, size_t dstSize, const char *email, size_t emailLen)
{
  if (!dst || dstSize == 0)
    return;

  const size_t dstLen = strnlen(dst, dstSize);
  if (dstLen == dstSize ||
      rr_email_list_contains(dst, dstLen, email, emailLen))
    return;

  const size_t separator = dstLen > 0 ? 1 : 0;
  const size_t available = dstSize - dstLen;
  if (separator + emailLen + 1 > available)
    return;

  size_t pos = dstLen;
  if (separator)
    dst[pos++] = '\n';

  memcpy(dst + pos, email, emailLen);
  dst[pos + emailLen] = '\0';
}

void rr_email_extract(
  char *dst, size_t dstSize, const char *text, size_t textSize)
{
  if (!dst || dstSize == 0 || !text)
    return;

  for(size_t at = 0; at < textSize; ++at)
  {
    if (text[at] != '@')
      continue;

    size_t start = at;
    while(start > 0 && rr_email_is_local_char(text[start - 1]))
      --start;

    size_t end = at + 1;
    while(end < textSize && rr_email_is_domain_char(text[end]))
      ++end;

    const size_t tokenEnd = end;
    while(end > at + 1 && text[end - 1] == '.')
      --end;

    if ((start > 0 && rr_email_is_token_char(text[start - 1])) ||
        (tokenEnd < textSize && rr_email_is_token_char(text[tokenEnd])) ||
        !rr_email_is_valid(text + start, at - start, end - start))
      continue;

    rr_email_list_append(dst, dstSize, text + start, end - start);
  }
}

static size_t scrub_invalid_utf8_inplace(char *s, size_t cap)
{
  if (!s || cap == 0) return 0;

  size_t len = strnlen(s, cap);
  size_t i = 0, o = 0;

  while (i < len && o + 1 < cap)
  {
    unsigned char b0 = (unsigned char)s[i];

    if (b0 < 0x80)
    {
      s[o++] = s[i++];
      continue;
    }

    size_t need = 0;
    // Determine sequence length + validate first continuation constraints
    if (b0 >= 0xC2 && b0 <= 0xDF) need = 2;
    else if (b0 == 0xE0) need = 3;
    else if (b0 >= 0xE1 && b0 <= 0xEC) need = 3;
    else if (b0 == 0xED) need = 3;
    else if (b0 >= 0xEE && b0 <= 0xEF) need = 3;
    else if (b0 == 0xF0) need = 4;
    else if (b0 >= 0xF1 && b0 <= 0xF3) need = 4;
    else if (b0 == 0xF4) need = 4;
    else { i++; continue; } // invalid lead byte

    if (i + need > len) break; // truncated sequence at end

    unsigned char b1 = (unsigned char)s[i + 1];
    if ((b1 & 0xC0) != 0x80) { i++; continue; }

    if (need == 3) {
      unsigned char b2 = (unsigned char)s[i + 2];
      if ((b2 & 0xC0) != 0x80) { i++; continue; }

      // E0: b1 >= A0 (avoid overlong)
      if (b0 == 0xE0 && b1 < 0xA0) { i++; continue; }
      // ED: b1 <= 9F (avoid surrogate range)
      if (b0 == 0xED && b1 > 0x9F) { i++; continue; }
    } else if (need == 4) {
      unsigned char b2 = (unsigned char)s[i + 2];
      unsigned char b3 = (unsigned char)s[i + 3];
      if (((b2 & 0xC0) != 0x80) || ((b3 & 0xC0) != 0x80)) { i++; continue; }

      // F0: b1 >= 90 (avoid overlong)
      if (b0 == 0xF0 && b1 < 0x90) { i++; continue; }
      // F4: b1 <= 8F (<= U+10FFFF)
      if (b0 == 0xF4 && b1 > 0x8F) { i++; continue; }
    }

    // Valid sequence: copy bytes as-is
    for (size_t k = 0; k < need && o + 1 < cap; k++)
      s[o++] = s[i + k];
    i += need;
  }

  s[o] = '\0';
  return o;
}

static bool is_valid_utf8(const char *text, size_t len)
{
  const unsigned char *bytes = (const unsigned char *)text;
  size_t               pos   = 0;

  while (pos < len)
  {
    const unsigned char lead = bytes[pos];
    size_t              need;

    if (lead < 0x80)
    {
      ++pos;
      continue;
    }

    if (lead >= 0xC2 && lead <= 0xDF)
      need = 2;
    else if (lead >= 0xE0 && lead <= 0xEF)
      need = 3;
    else if (lead >= 0xF0 && lead <= 0xF4)
      need = 4;
    else
      return false;

    if (need > len - pos)
      return false;

    for (size_t i = 1; i < need; ++i)
    {
      if ((bytes[pos + i] & 0xC0) != 0x80)
        return false;
    }

    const unsigned char next = bytes[pos + 1];
    if ((lead == 0xE0 && next < 0xA0) ||
        (lead == 0xED && next > 0x9F) ||
        (lead == 0xF0 && next < 0x90) ||
        (lead == 0xF4 && next > 0x8F))
      return false;

    pos += need;
  }

  return true;
}

bool rr_sanatize(char *text, size_t maxLen)
{
  bool   ret     = false;
  char  *buf     = NULL;
  size_t textLen = strlen(text);

  if (is_valid_utf8(text, textLen))
  {
    if (textLen >= maxLen)
      scrub_invalid_utf8_inplace(text, maxLen);

    return true;
  }

  UErrorCode status = U_ZERO_ERROR;
  UCharsetDetector *det = ucsdet_open(&status);
  if (U_FAILURE(status))
  {
    LOG_ERROR("ucsdet_open failed: %s", u_errorName(status));
    goto err;
  }

  ucsdet_setText(det, text, textLen, &status);
  if (U_FAILURE(status))
    goto err_ucsdet;

  const UCharsetMatch *m = ucsdet_detect(det, &status);
  if (U_FAILURE(status) || !m)
  {
    // failure to detect just means we clobber anything invalid
    // no error/warning needed
    goto err_ucsdet;
  }

  const char *name = ucsdet_getName(m, &status);
  if (U_FAILURE(status) || !name)
  {
    LOG_ERROR("ucsdet_getName failed: %s", u_errorName(status));
    goto err_ucsdet;
  }

  int32_t conf = ucsdet_getConfidence(m, &status);
  if (U_FAILURE(status))
  {
    LOG_ERROR("ucsdet_getConfidence failed: %s", u_errorName(status));
    goto err_ucsdet;
  }

  if (strcmp(name, "UTF-8"     ) == 0 ||
      strcmp(name, "US-ASCII"  ) == 0)
    goto out;

  // false positives but still need checking for non-ascii
  if (strcmp(name, "IBM424_rtl") == 0 ||
      strcmp(name, "IBM424_ltr") == 0 ||
      strcmp(name, "IBM420_rtl") == 0 ||
      strcmp(name, "IBM420_ltr") == 0)
    goto err_ucsdet;

  if (!(buf = (char *)malloc(maxLen)))
  {
    LOG_ERROR("out of memory");
    goto err_ucsdet;
  }

  status = U_ZERO_ERROR;
  size_t len = ucnv_convert("UTF-8", name, buf, maxLen-1, text, textLen, &status);
  if (U_FAILURE(status))
  {
    LOG_ERROR("ucnv_convert failed (%s -> UTF-8, conf: %d): %s", name, conf, u_errorName(status));
    goto err_ucsdet;
  }

  memcpy(text, buf, len);
  text[len] = '\0';

out:
  ret = true;
err_ucsdet:
  ucsdet_close(det);

  if (ret)
  {
    // there still can be invalid sequences to remove if utf-8 was detected
    scrub_invalid_utf8_inplace(text, maxLen);
  }
  else
  {
    // if failed to convert, just remove non-ascii chars
    char * p = text;
    for(int i = 0; i < textLen; ++i)
    {
      if (text[i] != '\n' && (text[i] < 32 || text[i] > 126))
        continue;

      *p = text[i];
      ++p;
    }
    *p = '\0';
    ret = true;
  }

err:
  free(buf);
  return ret;
}

int rr_parse_ipv6_decimal(const char *str, unsigned __int128 *host)
{
  return inet_pton(AF_INET6, str, host);
}

int rr_parse_ipv4_decimal(const char *str, uint32_t *host)
{
  const char *p = str;

  uint32_t parts[4];

  for (int i = 0; i < 4; i++)
  {
    if (*p == '\0' || !isdigit(*p))
      return 0;

    unsigned val = 0;
    int digits = 0;

    while (*p && isdigit(*p)) {
      val = val * 10u + (unsigned)(*p - '0');
      if (val > 255u)
        return 0;
      p++;
      if (++digits > 3)
        return 0;
    }

    parts[i] = (uint32_t)val;

    if (i < 3) {
      if (!*p || *p != '.')
        return 0;
      p++;
    }
  }

  if (*p != '\0')
    return 0;

  *host = (parts[0] << 24) | (parts[1] << 16) | (parts[2] << 8) | parts[3];
  return 1;
}

uint8_t rr_ipv4_to_cidr(const uint32_t start, const uint32_t end)
{
  uint32_t diff = start ^ end;
  return (diff == 0) ? 32 : (uint8_t)__builtin_clz((unsigned)diff);
}

uint8_t rr_ipv6_to_cidr(const unsigned __int128 start, const unsigned __int128 end)
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

bool rr_calc_ipv4_cidr_end(uint32_t start, unsigned prefix_len, uint32_t *end_out)
{
  if (prefix_len > 32)
    return false;

  if (prefix_len == 0)
    return false;

  uint32_t mask = UINT32_MAX << (32u - prefix_len);
  uint32_t net  = start & mask;
  *end_out = net | ~mask;
  return true;
}

bool rr_calc_ipv6_cidr_end(const unsigned __int128 *start, unsigned prefix_len, unsigned __int128 *end_out)
{
  const uint8_t *s = (const uint8_t *)start;
  uint8_t       *e = (uint8_t *)end_out;

  memcpy(e, s, 16);

  if (prefix_len >= 128)
    return false;

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

  return true;
}
