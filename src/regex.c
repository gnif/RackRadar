#include "util.h"
#include "log.h"
#include "db.h"
#include "import.h"

#include <regex.h>
#include <stdlib.h>

#define RE_IPV4_CIDR \
  "([0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}/[1-9][0-9]?)"

#define RE_IPV6_CIDR \
  "([0-9A-Fa-f:.]*:[0-9A-Fa-f:.]*/[1-9][0-9]{0,2})"

static bool compile_regex(regex_t *re, const char *pattern)
{
  int rc = regcomp(re, pattern, REG_EXTENDED);
  if (rc != 0)
  {
    char errbuf[256];
    regerror(rc, re, errbuf, sizeof(errbuf));
    LOG_ERROR("regex compile failed: %s", errbuf);
    return false;
  }
  return true;
}

static bool scan_regex_matches(regex_t *re, const char *content, bool v4, RRDBNetBlock *netblock)
{
  regmatch_t m[2];
  const char *cursor = content;
  while (regexec(re, cursor, ARRAY_SIZE(m), m, 0) == 0)
  {
    regmatch_t *match = &m[0];
    if (match->rm_so < 0 || match->rm_eo < match->rm_so)
      return false;

    const char *str = cursor + match->rm_so;
    size_t len = (size_t)(match->rm_eo - match->rm_so);

    char buffer[128];
    if (len >= sizeof(buffer))
    {
      LOG_WARN("regex match too long to parse, skipping");
      cursor += m[0].rm_eo;
      continue;
    }

    memcpy(buffer, str, len);
    buffer[len] = '\0';

    char *savePtr = NULL;
    const char *addr = strtok_r(buffer, "/", &savePtr);
    if (!addr)
    {
      cursor += m[0].rm_eo;
      continue;
    }

    const char *prefix_len = strtok_r(NULL, "/", &savePtr);
    if (!prefix_len)
    {
      cursor += m[0].rm_eo;
      continue;
    }

    if (v4)
    {
      if (!rr_parse_ipv4_decimal(addr, &netblock->startAddr.v4))
      {
        cursor += m[0].rm_eo;
        continue;
      }

      netblock->prefixLen = (uint8_t)strtoul(prefix_len, NULL, 10);
      if (netblock->prefixLen == 0 || netblock->prefixLen > 32)
      {
        cursor += m[0].rm_eo;
        continue;
      }

      rr_calc_ipv4_cidr_end(netblock->startAddr.v4,
        netblock->prefixLen, &netblock->endAddr.v4);
      if (!rr_import_netblockv4_insert(netblock))
        return false;
    }
    else
    {
      if (!rr_parse_ipv6_decimal(addr, &netblock->startAddr.v6))
      {
        cursor += m[0].rm_eo;
        continue;
      }

      netblock->prefixLen = (uint8_t)strtoul(prefix_len, NULL, 10);
      if (netblock->prefixLen == 0 || netblock->prefixLen > 64)
      {
        cursor += m[0].rm_eo;
        continue;
      }

      rr_calc_ipv6_cidr_end(&netblock->startAddr.v6,
        netblock->prefixLen, &netblock->endAddr.v6);
      if (!rr_import_netblockv6_insert(netblock))
        return false;
    }

    cursor += m[0].rm_eo;
  }

  return true;
}

bool rr_regex_import_FILE(const char *registrar, FILE *fp,
  unsigned registrar_id, unsigned new_serial,
  const char *extra_v4, const char *extra_v6)
{
  if (fseek(fp, 0, SEEK_END) != 0)
  {
    LOG_ERROR("failed to seek REGEX source");
    return false;
  }

  long sz = ftell(fp);
  if (sz < 0)
  {
    LOG_ERROR("failed to determine REGEX source size");
    return false;
  }

  if (fseek(fp, 0, SEEK_SET) != 0)
  {
    LOG_ERROR("failed to rewind REGEX source");
    return false;
  }

  char *content = malloc(sz+1);
  if (!content)
  {
    LOG_ERROR("out of memory");
    return false;
  }
  content[sz] = '\0';

  if (fread(content, 1, (size_t)sz, fp) != (size_t)sz)
  {
    LOG_ERROR("failed to read REGEX content");
    free(content);
    return false;
  }

  RRDBNetBlock netblock =
  {
    .registrar_id = registrar_id,
    .serial       = new_serial
  };

  bool ret = true;
  regex_t re_v4, re_v6;

  if (!extra_v4)
    extra_v4 = RE_IPV4_CIDR;
  if (!extra_v6)
    extra_v6 = RE_IPV6_CIDR;

  if (*extra_v4 && (ret = compile_regex(&re_v4, extra_v4)))
  {
    ret = scan_regex_matches(&re_v4, content, true, &netblock);
    regfree(&re_v4);
  }

  if (ret && *extra_v6 && (ret = compile_regex(&re_v6, extra_v6)))
  {
    ret = scan_regex_matches(&re_v6, content, false, &netblock);
    regfree(&re_v6);
  }

  free(content);
  return ret;
}
