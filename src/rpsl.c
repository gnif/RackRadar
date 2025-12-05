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

enum Registrar
{
  REGISTRAR_GENERIC,
  REGISTRAR_RIPE,
  REGISTRAR_APNIC
};

struct ProcessState
{
  unsigned serial;
  enum Registrar registrar;

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
    RRDBOrg      org;
    RRDBNetBlock inetnum;
  }
  x;

  RRDBStatistics stats;
};

static bool rr_rpsl_skip_inetnum(struct ProcessState *state)
{
  switch(state->registrar)
  {
    case REGISTRAR_RIPE:
      return
        (strncmp(state->x.inetnum.netname, "NON-RIPE-", 9) == 0) ||
        (strncmp(state->x.inetnum.netname, "IANA-BLK" , 8) == 0);

    case REGISTRAR_APNIC:
      return
        (strncmp(state->x.inetnum.netname, "IANA-NETBLOCK-" , 14) == 0) ||
        (strncmp(state->x.inetnum.netname, "IANA-BLOCK"     , 10) == 0) ||
        (strncmp(state->x.inetnum.netname, "ARIN-CIDR-BLOCK", 15) == 0);

    case REGISTRAR_GENERIC:
      return false;
  }
  return false;
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

          state->x.org.registrar_id = state->registrar_id;
          state->x.org.serial       = state->serial;
          rr_db_execute_stmt(state->stmtOrg, &ra);
          if (ra == 1)
            ++state->stats.newOrgs;
          ++state->numOrg;            
          break;

        case RECORD_TYPE_INETNUM:
        {
          if (rr_rpsl_skip_inetnum(state))
            break;

          state->x.inetnum.registrar_id = state->registrar_id;
          state->x.inetnum.serial       = state->serial;

          // calculate the cidr prefix len
          state->x.inetnum.prefixLen = rr_ipv4_to_cidr(
            state->x.inetnum.startAddr.v4,
            state->x.inetnum.endAddr  .v4);

          rr_sanatize(state->x.inetnum.descr, sizeof(state->x.inetnum.descr));
          rr_db_execute_stmt(state->stmtNetBlockV4, &ra);
          if (ra == 1)
            ++state->stats.newIPv4;

          ++state->numInetnum;
          break;

        case RECORD_TYPE_INET6NUM:
          if (rr_rpsl_skip_inetnum(state))
            break;
        
          state->x.inetnum.registrar_id = state->registrar_id;
          state->x.inetnum.serial       = state->serial;

          // calculate the end of the segment
          rr_calc_ipv6_cidr_end(
            &state->x.inetnum.startAddr.v6,
             state->x.inetnum.prefixLen,
            &state->x.inetnum.endAddr  .v6);

          rr_sanatize(state->x.inetnum.descr, sizeof(state->x.inetnum.descr));
          rr_db_execute_stmt(state->stmtNetBlockV6, &ra);
          if (ra == 1)
            ++state->stats.newIPv6;

          ++state->numInet6num;
          break;
        }
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
      case RECORD_TYPE_IGNORE:
        return;

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

        static_assert(sizeof(state->x.inetnum.startAddr.v4) == sizeof(struct in_addr));
        if ((inet_pton(AF_INET, start, (void *)&state->x.inetnum.startAddr.v4) != 1) ||
            (inet_pton(AF_INET, end  , (void *)&state->x.inetnum.endAddr  .v4) != 1))
          state->recordType = RECORD_TYPE_IGNORE;

        // convert to host order
        state->x.inetnum.startAddr.v4 = ntohl(state->x.inetnum.startAddr.v4);
        state->x.inetnum.endAddr  .v4 = ntohl(state->x.inetnum.endAddr  .v4);
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

        static_assert(sizeof(state->x.inetnum.startAddr.v6) == sizeof(struct in6_addr));        
        if (inet_pton(AF_INET6, start, (void *)&state->x.inetnum.startAddr.v6) != 1)
        {
          state->recordType = RECORD_TYPE_IGNORE;
          return;
        }

        state->x.inetnum.prefixLen = strtoul(prefixLen, NULL, 10);
        if (state->x.inetnum.prefixLen > 64)
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
    case RECORD_TYPE_IGNORE:
      return;

    case RECORD_TYPE_ORG:
      if (0) {}
      MATCH("org-name", org.org_name, false);
      MATCH("descr"   , org.descr   , true );
      break;

    case RECORD_TYPE_INETNUM:
      if (0) {}      
      MATCH("org"    , inetnum.org_id_str, false);
      MATCH("netname", inetnum.netname   , false);
      MATCH("descr"  , inetnum.descr     , true );
      break;

    case RECORD_TYPE_INET6NUM:
      if (0) {}
      MATCH("org"    , inetnum.org_id_str, false);      
      MATCH("netname", inetnum.netname   , false);
      MATCH("descr"  , inetnum.descr     , true );
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
  bool ret = false;

  struct ProcessState state =
  {
    .inRecord     = false,
    .recordType   = RECORD_TYPE_IGNORE,
    .registrar_id = registar_id,
    .serial       = new_serial
  };

  if     (strcmp(registrar, "RIPE" ) == 0) state.registrar = REGISTRAR_RIPE;
  else if(strcmp(registrar, "APNIC") == 0) state.registrar = REGISTRAR_APNIC;
  else                                     state.registrar = REGISTRAR_GENERIC;

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

  if (!rr_db_prepare_netblockv6_insert(con, &state.stmtNetBlockV6, &state.x.inetnum))
  {
    LOG_ERROR("failed to prepare the netblockv6 statement");
    goto err_realloc;
  }

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
        const uintptr_t ptrOff  = ptr  - buf;
        const uintptr_t lineOff = line - buf;

        bufSize *= 2;
        uint8_t *newBuf = realloc(buf, bufSize);
        if (!newBuf)
        {
          LOG_ERROR("out of memory");
          goto err_realloc;
        }
        ptr  = newBuf + ptrOff;
        line = newBuf + lineOff;
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
    LOG_INFO("RPSL Statistics");
    LOG_INFO("  Total Lines: %llu", lineNo           );
    LOG_INFO("  Orgs       : %llu", state.numOrg     );
    LOG_INFO("  Inetnum    : %llu", state.numInetnum );
    LOG_INFO("  Inet6num   : %llu", state.numInet6num);
    LOG_INFO("  Ignored    : %llu", state.numIngore  );
  }
  
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