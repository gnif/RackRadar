#ifndef _H_RR_QUERY_
#define _H_RR_QUERY_

#include "db.h"

typedef struct RRDBRegistrar
{
  unsigned id;
  unsigned serial;
  unsigned last_import;
  char     name[32];
}
RRDBRegistrar;

typedef struct RRDBOrg
{
  unsigned registrar_id;
  unsigned serial;
  char     name    [32];
  char     org_name[1024];
  char     descr   [8192];
}
RRDBOrg;

typedef union RRDBAddr
{
  uint32_t          v4;
  unsigned __int128 v6;
}
RRDBAddr;

typedef struct RRDBNetBlock
{
  unsigned registrar_id;
  unsigned serial;
  char     org_id_str[33];
  RRDBAddr startAddr;
  RRDBAddr endAddr;
  uint8_t  prefixLen;
  char     netname[256 ];
  char     descr  [8192];
}
RRDBNetBlock;

typedef struct RRDBStatistics
{
  unsigned long long
    newOrgs,
    deletedOrgs,

    newIPv4,
    deletedIPv4,

    newIPv6,
    deletedIPv6;
}
RRDBStatistics;

typedef struct RRDBIPInfo
{
  unsigned long long id;
  unsigned registrar_id;
  char     org_id_str[33];
  char     org_name[1024];
  RRDBAddr start_ip;
  RRDBAddr end_ip;
  uint8_t  prefix_len;
  char     netname[256];
  char     descr  [8192];
}
RRDBIPInfo;

typedef struct DBQueryData
{
  struct
  {
    // insert a new registrar record
    struct
    {
      RRDBStmt *stmt;
      char      in_name[32];
    }
    insert;

    // lookup a registrar record by name
    struct
    {
      RRDBStmt      *stmt;
      char           in_name[32];
      RRDBRegistrar  out;
    }
    by_name;  
  }
  registrar;

  struct
  {
    // lookup best match by address
    struct
    {
      RRDBStmt  *stmt;
      uint32_t   in_ipv4;
      RRDBIPInfo out;
    }
    by_addr;
  }
  netblock_v4;

  struct
  {
    // lookup best match by address
    struct
    {
      RRDBStmt          *stmt;
      unsigned __int128 in_ipv6;
      RRDBIPInfo        out;
    }
    by_addr;
  }
  netblock_v6;
}
DBQueryData;

bool rr_query_init(RRDBCon *con, void **udata);
bool rr_query_deinit(RRDBCon *con, void **udata);

bool rr_query_prepare_org_insert(RRDBCon *con, RRDBStmt **stmt, RRDBOrg *bind);
bool rr_query_prepare_netblockv4_insert(RRDBCon *con, RRDBStmt **stmt, RRDBNetBlock *bind);
bool rr_query_prepare_netblockv6_insert(RRDBCon *con, RRDBStmt **stmt, RRDBNetBlock *bind);
bool rr_query_finalize_registrar(RRDBCon *con, unsigned registrar_id, unsigned serial, RRDBStatistics *stats);

bool rr_query_registrar_by_name(
  RRDBCon *con,
  const char *in_name,
  unsigned *out_registrar_id,
  unsigned *out_serial,
  unsigned *out_last_import);

bool rr_query_registrar_insert(
  RRDBCon *con,
  const char *in_name,
  unsigned *out_registrar_id);  

#endif