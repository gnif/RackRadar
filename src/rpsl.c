#include "rpsl.h"
#include "log.h"
#include "util.h"
#include "db.h"

#include <stdint.h>
#include <string.h>
#include <zlib.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <assert.h>

enum RecordType
{
  RECORD_TYPE_IGNORE,
  RECORD_TYPE_ORG,
  RECORD_TYPE_INETNUM,
  RECORD_TYPE_INET6NUM
};

struct ProcessState
{
  unsigned serial;
  bool isRIPE;

  bool            inRecord;
  enum RecordType recordType;

  unsigned registrar_id;
  RRDBStmt *stmtOrg;
  RRDBStmt *stmtNetBlockV4;
  RRDBStmt *stmtNetBlockV6;

  unsigned long long numIngore;
  unsigned long long numOrg;
  unsigned long long numInetnum;
  unsigned long long numInet6num;

  union
  {
    RRDBOrg        org;
    RRDBNetBlockV4 inetnum;
    RRDBNetBlockV6 inet6num;
  }
  x;

  RRDBStatistics stats;
};

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

static bool sanatize(char *text, size_t maxLen)
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

static void rr_rpsl_process_line(char * line, size_t len, struct ProcessState *state)
{
  unsigned long long ra;

  if (len == 0)
  {
    // end of record found
    if (state->inRecord)
    {
      switch(state->recordType)
      {
        case RECORD_TYPE_IGNORE:
          ++state->numIngore;
          break;

        case RECORD_TYPE_ORG:
          ++state->numOrg;
          state->x.org.registrar_id = state->registrar_id;
          state->x.org.serial       = state->serial;
          rr_db_execute_stmt(state->stmtOrg, &ra);
          if (ra == 1)
            ++state->stats.newOrgs;

          ++state->numOrg;
          break;

        case RECORD_TYPE_INETNUM:
          if (state->isRIPE && strncmp(state->x.inetnum.netname, "NON-RIPE-", 9) == 0)
            break;

          ++state->numInetnum;
          state->x.inetnum.registrar_id    = state->registrar_id;
          state->x.inetnum.serial          = state->serial;
          state->x.inetnum.org_id_str_null = state->x.inetnum.org_id_str[0] == '\0';

          // convert to host order and calculate the cidr prefix len
          state->x.inetnum.startAddr = ntohl(state->x.inetnum.startAddr);
          state->x.inetnum.endAddr   = ntohl(state->x.inetnum.endAddr  );
          state->x.inetnum.prefixLen = rr_ipv4_to_cidr(
            state->x.inetnum.startAddr,
            state->x.inetnum.endAddr);

          sanatize(state->x.inetnum.descr, sizeof(state->x.inetnum.descr));
          rr_db_execute_stmt(state->stmtNetBlockV4, &ra);
          if (ra == 1)
            ++state->stats.newIPv4;

          ++state->numInetnum;
          break;

        case RECORD_TYPE_INET6NUM:
          if (state->isRIPE && strncmp(state->x.inetnum.netname, "NON-RIPE-", 9) == 0)
            break;
        
          state->x.inet6num.registrar_id    = state->registrar_id;
          state->x.inet6num.serial          = state->serial;
          state->x.inet6num.org_id_str_null = state->x.inet6num.org_id_str[0] == '\0';

          // calculate the end of the segment
          rr_calc_ipv6_cidr_end(
            &state->x.inet6num.startAddr,
            state->x.inet6num.prefixLen,
            &state->x.inet6num.endAddr);

          sanatize(state->x.inet6num.descr, sizeof(state->x.inet6num.descr));
          rr_db_execute_stmt(state->stmtNetBlockV6, &ra);
          if (ra == 1)
            ++state->stats.newIPv6;

          ++state->numInet6num;
          break;
      }

      state->inRecord   = false;
      state->recordType = RECORD_TYPE_IGNORE;
      memset(&state->x, 0, sizeof(state->x));
    }
    return;
  }

  // skip comments
  {
    bool comment = true;
    for(int i = 0; i < len; ++i)
    {
      if (line[i] == ' ' || line[i] == '\t')
        continue;
      if (line[i] != '#')
        comment = false;
      break;
    }
    if (comment)
      return;
  }

  // if new record
  if (!state->inRecord)
  {
    state->inRecord = true;

    char *sp    = NULL;
    char *name  = __strtok_r(line, ":", &sp);
    if (strcmp(name, "organisation") == 0)
      state->recordType = RECORD_TYPE_ORG;
    else if (strcmp(name, "inetnum" ) == 0)
      state->recordType = RECORD_TYPE_INETNUM;
    else if (strcmp(name, "inet6num") == 0)
      state->recordType = RECORD_TYPE_INET6NUM;
    else
    {
      state->recordType = RECORD_TYPE_IGNORE;
      return;
    }

    char *value = __strtok_r(NULL, "" , &sp);
    if (!value)
    {
      // invalid record had no value
      state->recordType = RECORD_TYPE_IGNORE;
      return;
    }

    // trim leading whitespace
    while(*value == ' ')
      ++value;

    switch(state->recordType)
    {
      case RECORD_TYPE_ORG:
        strncpy(state->x.org.name, value, sizeof(state->x.org.name));
        return;

      case RECORD_TYPE_INETNUM:
      {
        sp = NULL;
        char *start = rr_trim(__strtok_r(value, "-", &sp));        
        char *end   = rr_trim(__strtok_r(NULL , "" , &sp));
        if (!start || !end)
        {         
          state->recordType = RECORD_TYPE_IGNORE;
          return;
        }

        static_assert(sizeof(state->x.inetnum.startAddr) == sizeof(struct in_addr));
        if ((inet_pton(AF_INET, start, (void *)&state->x.inetnum.startAddr) != 1) ||
            (inet_pton(AF_INET, end  , (void *)&state->x.inetnum.endAddr  ) != 1))
          state->recordType = RECORD_TYPE_IGNORE;

        return;
      }

      case RECORD_TYPE_INET6NUM:
      {
        sp = NULL;
        char *start     = rr_trim(__strtok_r(value, "/", &sp));        
        char *prefixLen = rr_trim(__strtok_r(NULL , "" , &sp));
        if (!start || !prefixLen)
        {
          state->recordType = RECORD_TYPE_IGNORE;
          return;
        }

        static_assert(sizeof(state->x.inet6num.startAddr) == sizeof(struct in6_addr));        
        if (inet_pton(AF_INET6, start, (void *)&state->x.inet6num.startAddr) != 1)
        {
          state->recordType = RECORD_TYPE_IGNORE;
          return;
        }

        state->x.inet6num.prefixLen = strtoul(prefixLen, NULL, 10);
        if (state->x.inet6num.prefixLen > 64)
          state->recordType = RECORD_TYPE_IGNORE;

        return;
      }
    }
  }
  
  char *sp    = NULL;
  char *name  = __strtok_r(line, ":", &sp);

  char  *dst      = NULL;
  size_t dstSz;
  bool   dstMulti;

#define MATCH(a, b, multi) \
  else if (strcmp(name, a) == 0) \
    dst = state->x.b, dstSz = sizeof(state->x.b), dstMulti = multi

  switch(state->recordType)
  {
    case RECORD_TYPE_ORG:
    {
      if (0) {}
      MATCH("org-name", org.org_name, false);
      MATCH("descr"   , org.descr   , true );
    }

    case RECORD_TYPE_INETNUM:
      if (0) {}      
      MATCH("org"    , inetnum.org_id_str, false);
      MATCH("netname", inetnum.netname   , false);
      MATCH("descr"  , inetnum.descr     , true );
      break;

    case RECORD_TYPE_INET6NUM:
      if (0) {}
      MATCH("org"    , inetnum.org_id_str, false);      
      MATCH("netname", inet6num.netname  , false);
      MATCH("descr"  , inet6num.descr    , true );
      break;
  }

#undef MATCH

  if (!dst)
    return;

  char *value = rr_trim(__strtok_r(NULL, "", &sp));
  if (!value)
    return;

  if (!dstMulti || dst[0] == '\0')
    strncpy(dst, value, dstSz);
  else
  {
    size_t len = strlen(dst);
    if (len + 2 < dstSz)
    {
      dst[len] = '\n';
      strncpy(dst + len + 1, value, dstSz - len + 1);
    }
  }
}

static bool rr_rpsl_import_gzFILE(const char *registrar, gzFile gz,
  RRDBCon *con, unsigned registar_id, unsigned new_serial)
{
  LOG_INFO("start import %s", registrar);
  uint64_t startTime = rr_microtime();
  bool ret = false;

  struct ProcessState state =
  {
    .isRIPE     = strcmp(registrar, "RIPE") == 0,
    .inRecord   = false,
    .recordType = RECORD_TYPE_IGNORE
  };

  if (!gz)
  {
    LOG_ERROR("gzopen failed");
    goto err_gzopen;
  }

  size_t bufSize = 64*1024;
  uint8_t *buf   = malloc(bufSize);
  if (!buf)
  {
    LOG_ERROR("out of memory");
    goto err_gzopen;
  }

  uint8_t *ptr  = buf;
  uint8_t *line = buf;
  unsigned long long lineNo = 0;
  int n;

  if (!rr_db_prepare_org_insert(con, &state.stmtOrg, &state.x.org))
  {
    LOG_ERROR("failed to prepare org statement");
    goto err_realloc;
  }

  if (!rr_db_prepare_netblockv4_insert(con, &state.stmtNetBlockV4, &state.x.inetnum))
  {
    LOG_ERROR("failed to prepare the netblockv4 statement");
    goto err_realloc;
  }

  if (!rr_db_prepare_netblockv6_insert(con, &state.stmtNetBlockV6, &state.x.inet6num))
  {
    LOG_ERROR("failed to prepare the netblockv6 statement");
    goto err_realloc;
  }

  rr_db_start(con);
  unsigned last_import;
  state.registrar_id = rr_db_get_registrar_id(con, registrar, true, &state.serial, &last_import);
  ++state.serial;
  
  while((n = gzread(gz, ptr, buf + bufSize - ptr)) > 0)
  {
    uint8_t *p = ptr;
    for(int i = 0; i < n; ++i)
    {
      if (*p++ == '\n')
      {
        p[-1] = '\0';
        rr_rpsl_process_line((char *)line, (size_t)(p - line - 1), &state);
        ++lineNo;
        line = p;
      }
    }

    ptr = p;
    if (p == buf + bufSize)
    {
      if (line == buf)
      {
        bufSize *= 2;
        uint8_t *newBuf = realloc(buf, bufSize);
        if (!newBuf)
        {
          LOG_ERROR("out of memory");
          goto err_realloc;
        }
        ptr  = newBuf + (ptr - buf);
        line = newBuf + (line - buf);
        buf  = newBuf;
      }
      else
      {
        size_t rem = (size_t)(ptr - line);
        memmove(buf, line, rem);
        line = buf;
        ptr = buf + rem;        
      }
    }
  }

  if (ptr > line)
    rr_rpsl_process_line((char *)line, (size_t)(ptr - line), &state);

  rr_rpsl_process_line("", 0, &state);

  ret = rr_db_finalize_registrar(con, state.registrar_id, state.serial, &state.stats);

err_realloc:
  rr_db_stmt_free(&state.stmtOrg);
  rr_db_stmt_free(&state.stmtNetBlockV4);
  rr_db_stmt_free(&state.stmtNetBlockV6);

  if (ret)
  {
    rr_db_commit(con);
    uint64_t elapsed = rr_microtime() - startTime;
    uint64_t sec     = elapsed / 1000000UL;
    uint64_t us      = elapsed % 1000000UL;

    LOG_INFO("RPSL Statistics");
    LOG_INFO("  Total Lines: %llu", lineNo           );
    LOG_INFO("  Orgs       : %llu", state.numOrg     );
    LOG_INFO("  Inetnum    : %llu", state.numInetnum );
    LOG_INFO("  Inet6num   : %llu", state.numInet6num);
    LOG_INFO("  Ignored    : %llu", state.numIngore  );
    LOG_INFO("  Elapsed    : %02u:%02u:%02u.%03u",
        (unsigned)(sec / 60 / 60),
        (unsigned)(sec / 60 % 60),
        (unsigned)(sec % 60),
        (unsigned)(us / 1000));
  }
  else
    rr_db_rollback(con);
  rr_db_putCon(&con);
  free(buf);
err_gzopen:
  LOG_INFO(ret ? "success" : "failure");
  return ret;
}

bool rr_rpsl_import_gz_FILE(const char *registrar, FILE *fp,
  RRDBCon *con, unsigned registrar_id, unsigned new_serial)
{
  int fd = fileno(fp);
  if (fd < 0)
  {
    LOG_ERROR("failed to get the fileno from the fp");
    return false;
  }

  //dup so gzclose wont close the provided one
  int fd2 = dup(fd);
  if (fd2 < 0)
  {
    LOG_ERROR("failed to dup the filehandle");
    return false;
  }

  if (lseek(fd2, 0, SEEK_SET) != 0)
  {
    close(fd2);
    LOG_ERROR("failed to seek to the start of the file");
    return false;
  }
  
  gzFile gz = gzdopen(fd2, "rb");
  if (!gz)
  {
    close(fd2);
    LOG_ERROR("gzdopen failed");
    return false;
  }

  bool ret = rr_rpsl_import_gzFILE(registrar, gz, con, registrar_id, new_serial);
  gzclose(gz);
  return ret;
}

bool rr_rpsl_import_gz(const char *registrar, const char *filename,
  RRDBCon *con, unsigned registrar_id, unsigned new_serial)
{
  gzFile gz = gzopen(filename, "rb");
  if (!gz)
  {
    LOG_ERROR("gzopen failed");
    return false;
  }

  bool ret = rr_rpsl_import_gzFILE(registrar, gz, con, registrar_id, new_serial);
  gzclose(gz);
  return ret;
}