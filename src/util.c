#include "util.h"
#include "log.h"

#include <stdlib.h>
#include <ctype.h>
#include <unicode/ucsdet.h>
#include <unicode/ucnv.h>
#include <unicode/utypes.h>

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

bool rr_sanatize(char *text, size_t maxLen)
{
  bool ret = false;
  char *buf = NULL;
  size_t textLen = strlen(text);

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