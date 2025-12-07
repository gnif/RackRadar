#include "arin.h"
#include "log.h"
#include "zip.h"
#include "util.h"
#include "query.h"

#include <expat.h>

#define ARIN_XML_FILE "arin_db.xml"

enum RecordType
{
  RECORD_TYPE_IGNORE,
  RECORD_TYPE_ORG,
  RECORD_TYPE_NET
};

typedef struct AddrPair
{
  RRDBAddr start;
  RRDBAddr end;
  uint8_t  cidr;
}
AddrPair;

struct ProcessState
{
  XML_Parser p;

  RRDBStmt *stmtOrg;
  RRDBStmt *stmtNetBlockV4;
  RRDBStmt *stmtNetBlockV6;

  unsigned registrar_id;
  unsigned serial;
  RRDBStatistics stats;

  unsigned level;
  enum RecordType recordType;

  union
  {
    RRDBOrg      org;
    RRDBNetBlock inetnum;
  }
  x;

  char ipVersion[8];
  char ipType   [8];
  bool inNetBlocks;
  bool inNetBlock;
  char startAddr[40];
  char endAddr  [40];  

  AddrPair *addrs;
  size_t    nbAddrs;
  size_t    szAddrs;
  
  char   *textPtr;
  size_t  textPtrSz;

  bool    inComment;
  char   *commentPtr;
  size_t  commentPtrSz;
  size_t  commentPtrOff;
};

static void setup_comment(struct ProcessState *state)
{
  state->textPtr   = NULL;
  state->textPtrSz = 0;

  if (!state->commentPtr || state->commentPtrSz == 0)
    return;

  if (state->commentPtrOff >= state->commentPtrSz - 1)
    return;    

  if (state->commentPtrOff > 0)
  {
    if (state->commentPtrOff >= state->commentPtrSz - 1)
      return;
    state->commentPtr[state->commentPtrOff++] = '\n';
  }

  state->textPtr   = state->commentPtr   + state->commentPtrOff;
  state->textPtrSz = state->commentPtrSz - state->commentPtrOff;
}

static void add_netblock(struct ProcessState *state)
{
  if (!*state->startAddr || !*state->endAddr || !*state->ipType)
    return;

  // filter out types that are not authoritive
  static const char * types[] =  { "DA", "DS", "AR", "A", "S", NULL };
  bool keep = false;
  for(const char ** type = types; *type; ++type)
    if (strcmp(state->ipType, *type) == 0)
    {
      keep = true;
      break;
    }
  
  if (!keep)
    return;

  if (state->nbAddrs == state->szAddrs)
  {
    state->szAddrs *= 2;
    AddrPair *new = realloc(state->addrs, sizeof(*state->addrs) * state->szAddrs);
    if (!new)
    {
      LOG_ERROR("out of memory");
      XML_StopParser(state->p, XML_FALSE);    
      return;
    }
    state->addrs = new;
  }

  AddrPair *pair = &state->addrs[state->nbAddrs];
  if (strstr(state->startAddr, ":") != NULL)
  {
    if (inet_pton(AF_INET6, state->startAddr, (void *)&pair->start.v6) != 1 ||
        inet_pton(AF_INET6, state->endAddr  , (void *)&pair->end.v6  ) != 1)
    {
      LOG_WARN("v6 inet_pton failure: %s -> %s", state->startAddr, state->endAddr);
      return;
    }

    pair->cidr = rr_ipv6_to_cidr(pair->start.v6, pair->end.v6);
  }
  else
  {
    if ((rr_parse_ipv4_decimal(state->startAddr, (void *)&pair->start.v4) != 1) ||
        (rr_parse_ipv4_decimal(state->endAddr  , (void *)&pair->end  .v4) != 1))
    {
      LOG_WARN("v4 inet_pton failure: %s -> %s", state->startAddr, state->endAddr);    
      return;
    }

    pair->cidr = rr_ipv4_to_cidr(pair->start.v4, pair->end.v4);
  }

  ++state->nbAddrs;
}

static void xml_on_start(void *userData, const char *name, const char **atts)
{
#define MATCH(a, b) \
  else if (strcmp(name, a) == 0) \
    state->textPtr = state->b, state->textPtrSz = sizeof(state->b)

  struct ProcessState *state = userData;
  switch(state->level++)
  {
    case 0:
      if (strcmp(name, "bulkwhois") != 0)
      {
        LOG_ERROR("expected <bulkwhois> but instead got <%s>", name);
        goto err;
      }
      break;

    case 1:
      if (strcmp(name, "org") == 0)
        state->recordType = RECORD_TYPE_ORG;
      else if (strcmp(name, "net") == 0)
      {
        state->recordType   = RECORD_TYPE_NET;
        state->nbAddrs      = 0;
        state->inNetBlocks  = false;
        state->inNetBlock   = false;
        state->ipVersion[0] = '\0';
      }
      else
      {
        state->recordType = RECORD_TYPE_IGNORE;
        return;
      }
      memset(&state->x, 0, sizeof(state->x));
      break;

    case 2:
      switch(state->recordType)
      {
        case RECORD_TYPE_IGNORE:
          break;

        case RECORD_TYPE_ORG:
          if (0);
          MATCH("name"   , x.org.org_name);
          MATCH("handle" , x.org.name    );
          else if (strcmp(name, "comment") == 0)
          {
            state->inComment     = true;          
            state->commentPtr    = state->x.org.descr;
            state->commentPtrSz  = sizeof(state->x.org.descr);
            state->commentPtrOff = 0;
          }
          break;

        case RECORD_TYPE_NET:
          if (0);
          MATCH("orgHandle", x.inetnum.org_id_str);
          MATCH("name"     , x.inetnum.netname   );
          MATCH("version"  , ipVersion           );
          else if (strcmp(name, "netBlocks") == 0)
            state->inNetBlocks = true;
          else if (strcmp(name, "comment") == 0)
          {
            state->inComment     = true;          
            state->commentPtr    = state->x.inetnum.descr;
            state->commentPtrSz  = sizeof(state->x.inetnum.descr);
            state->commentPtrOff = 0;
          }
          break;
      }
      break;

    case 3:
      if (state->recordType == RECORD_TYPE_IGNORE)
        break;

      if (state->inComment)
      {
        if (strcmp(name, "line") == 0)
        {
          setup_comment(state);
          break;
        }
        break;
      }

      if (state->recordType == RECORD_TYPE_NET &&
          state->inNetBlocks &&
          strcmp(name, "netBlock") == 0)
        {
          state->inNetBlock = true;
          state->startAddr[0] = '\0';
          state->endAddr  [0] = '\0';
        }
      break;

    case 4:
      if (!state->inNetBlock)
        break;

      if (0);
      MATCH("startAddress", startAddr);
      MATCH("endAddress"  , endAddr  );
      MATCH("type"        , ipType   );
      break;
  }
  return;
err:
  XML_StopParser(state->p, XML_FALSE);
  return;

#undef MATCH
}

static void xml_on_end(void *userData, const char *name)
{
  struct ProcessState *state = userData;
  unsigned long long ar;

  state->textPtr = NULL;
  switch(state->level--)
  {
    case 0:
      LOG_ERROR("invalid schema");
      XML_StopParser(state->p, XML_FALSE);
      return;

    // bulkwhois
    case 1:
      return;

    case 2:
      switch(state->recordType)
      {
        case RECORD_TYPE_IGNORE:
          return;

        case RECORD_TYPE_ORG:
          //filter out the generic top level org
          if (strcmp(state->x.org.name, "ARIN") == 0)
            break;

          state->x.org.registrar_id = state->registrar_id;
          state->x.org.serial       = state->serial;
          rr_sanatize(state->x.org.descr, sizeof(state->x.org.descr));
          if (!rr_db_stmt_execute(state->stmtOrg, &ar))
          {
            XML_StopParser(state->p, XML_FALSE);
            return;
          }
          if (ar == 1)
            ++state->stats.newOrgs;
          break;

        case RECORD_TYPE_NET:
        {
          //filter out the generic top level netblocks
          if (strcmp(state->x.inetnum.org_id_str, "ARIN") == 0)
            break;

          state->x.inetnum.registrar_id = state->registrar_id;
          state->x.inetnum.serial       = state->serial;

          unsigned long long ar;
          unsigned long long *count;
          RRDBStmt *stmt;
          
          if (state->ipVersion[0] == '4')
          {
             stmt  = state->stmtNetBlockV4;
             count = &state->stats.newIPv4;
          }
          else
          {
            stmt  = state->stmtNetBlockV6;
            count = &state->stats.newIPv6;
          }

          for(size_t i = 0; i < state->nbAddrs; ++i)
          {
            AddrPair *pair = &state->addrs[i];
            state->x.inetnum.startAddr = pair->start;
            state->x.inetnum.endAddr   = pair->end;
            state->x.inetnum.prefixLen = pair->cidr;
            if (!rr_db_stmt_execute(stmt, &ar))
            {
              XML_StopParser(state->p, XML_FALSE);
              return;
            }
            if (ar == 1)
              ++(*count);
          }
          break;
        }        
      }
      break;

    case 3:
      switch(state->recordType)
      {
        case RECORD_TYPE_IGNORE:
          return;

        case RECORD_TYPE_ORG:
          state->inComment = false;
          break;

        case RECORD_TYPE_NET:
          state->inComment   = false;
          state->inNetBlocks = false;
          break;
      }
      break;

    case 4:
      if (!state->inNetBlock)
        break;

      add_netblock(state);
      state->inNetBlock = false;
      break;
  }  
}

static void xml_on_text(void *userData, const XML_Char *s, int len)
{
  struct ProcessState *state = userData;
  if (!state->textPtr || state->textPtrSz == 0)
    return;

  size_t avail = state->textPtrSz;
  size_t n     = (len > 0) ? (size_t)len : 0;

  if (n >= avail)
    n = avail - 1;

  memcpy(state->textPtr, s, n);
  if (state->inComment)
    state->commentPtrOff += n;

  state->textPtr   += n;
  state->textPtrSz -= n;
  *state->textPtr   = '\0';
}

bool rr_arin_import_zip_FILE(const char *registrar, FILE *fp,
  RRDBCon *con, unsigned registrar_id, unsigned new_serial)
{
  unzFile uz = rr_zip_openFILE(fp);
  if (!uz)
  {
    LOG_ERROR("rr_zip_openFile failed");
    return false;
  }

  int rc = unzLocateFile(uz, ARIN_XML_FILE, 1);
  if (rc != UNZ_OK)
  {
    LOG_ERROR("%s not found in zip", ARIN_XML_FILE);
    goto err;
  }

  rc = unzOpenCurrentFile(uz);
  if (rc != UNZ_OK)
  {
    LOG_ERROR("unzOpenCurrentFile failed: %d", rc);
    goto err;
  }

  bool ret = false;
  struct ProcessState state =
  {
    .registrar_id = registrar_id,
    .serial       = new_serial
  };

  state.szAddrs = 32;
  state.addrs   = calloc(state.szAddrs, sizeof(*state.addrs));
  if (!state.addrs)
  {
    LOG_ERROR("out of memory");
    goto err;
  }

  if (!rr_query_prepare_org_insert(con, &state.stmtOrg, &state.x.org))
  {
    LOG_ERROR("failed to prepare org statement");
    goto err_addrs;
  }

  if (!rr_query_prepare_netblockv4_insert(con, &state.stmtNetBlockV4, &state.x.inetnum))
  {
    LOG_ERROR("failed to prepare the netblockv4 statement");
    goto err_addrs;
  }

  if (!rr_query_prepare_netblockv6_insert(con, &state.stmtNetBlockV6, &state.x.inetnum))
  {
    LOG_ERROR("failed to prepare the netblockv6 statement");
    goto err_addrs;
  }

  state.p = XML_ParserCreate(NULL);
  if (!state.p)
  {
    LOG_ERROR("XML_ParserCreate failed");
    goto err_addrs;
  }

  XML_SetUserData            (state.p, &state);
  XML_SetElementHandler      (state.p, xml_on_start, xml_on_end);
  XML_SetCharacterDataHandler(state.p, xml_on_text);

  char buf[1024*64];
  for(;;)
  {
    int n = unzReadCurrentFile(uz, buf, sizeof(buf));
    if (n < 0)
    {
      LOG_ERROR("unzReadCurrentFile failed");
      goto err_xml_parser;
    }

    if (n == 0)
    {
      if (XML_Parse(state.p, buf, 0, XML_TRUE) == XML_STATUS_ERROR)
      {
        LOG_ERROR("XML_Parse failed");
        goto err_xml_parser;
      }
      break;
    }

    if (XML_Parse(state.p, buf, n, XML_FALSE) == XML_STATUS_ERROR)
    {
      LOG_ERROR("XML_Parse failed");
      goto err_xml_parser;
    }
  }

  ret = rr_query_finalize_registrar(con, registrar_id, new_serial, &state.stats);

err_xml_parser:
  XML_ParserFree(state.p);
err_addrs:
  free(state.addrs);
  unzCloseCurrentFile(uz);  
err:
  rr_db_stmt_free(&state.stmtOrg);
  rr_db_stmt_free(&state.stmtNetBlockV4);
  rr_db_stmt_free(&state.stmtNetBlockV6);
  unzClose(uz);
  return ret;
}

bool rr_arin_import_zip(const char *registrar, const char *filename,
  RRDBCon *con, unsigned registrar_id, unsigned new_serial)
{
  FILE *fp = fopen(filename, "rb");
  if (!fp)
  {
    LOG_ERROR("Failed to open %s", filename);
    return false;
  }

  bool ret = rr_arin_import_zip_FILE(registrar, fp, con, registrar_id, new_serial);
  fclose(fp);
  return ret;
}