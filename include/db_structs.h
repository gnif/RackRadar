#ifndef _H_RR_DB_STRUCTS_
#define _H_RR_DB_STRUCTS_

#include <stdint.h>

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

#endif
