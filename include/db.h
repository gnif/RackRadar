#ifndef _H_RR_DB_
#define _H_RR_DB_

#include <stdbool.h>
#include <stdint.h>

typedef struct RRDBCon  RRDBCon;
typedef struct RRDBStmt RRDBStmt;

bool rr_db_init(void);
void rr_db_deinit(void);

bool rr_db_getCon(RRDBCon **con);
void rr_db_putCon(RRDBCon **con);

void rr_db_start   (RRDBCon *con);
void rr_db_commit  (RRDBCon *con);
void rr_db_rollback(RRDBCon *con);

bool rr_db_execute_stmt(RRDBStmt *stmt, unsigned long long *affectedRows);
void rr_db_stmt_free   (RRDBStmt **rs);

typedef struct RRDBOrg
{
  unsigned registrar_id;
  unsigned serial;
  char     name    [32];
  char     org_name[1024];
  char     descr   [8192];
}
RRDBOrg;

typedef struct RRDBNetBlockV4
{
  unsigned registrar_id;
  unsigned serial;
  char     org_id_str[33];
  char     org_id_str_null;
  uint32_t startAddr;
  uint32_t endAddr;
  uint8_t  prefixLen;
  char     org    [256 ];
  char     netname[256 ];
  char     descr  [8192];
}
RRDBNetBlockV4;

typedef struct RRDBNetBlockV6
{
  unsigned registrar_id;
  unsigned serial;
  char     org_id_str[33];
  char     org_id_str_null;
  unsigned __int128 startAddr;
  unsigned __int128 endAddr;
  uint8_t  prefixLen;
  char     org    [256 ];
  char     netname[256 ];
  char     descr  [8192];
}
RRDBNetBlockV6;

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

unsigned rr_db_get_registrar_id(RRDBCon *con, const char *name, bool create, unsigned *serial, unsigned *last_import);
bool rr_db_prepare_org_insert(RRDBCon *con, RRDBStmt **stmt, RRDBOrg *bind);
bool rr_db_prepare_netblockv4_insert(RRDBCon *con, RRDBStmt **stmt, RRDBNetBlockV4 *bind);
bool rr_db_prepare_netblockv6_insert(RRDBCon *con, RRDBStmt **stmt, RRDBNetBlockV6 *bind);
bool rr_db_finalize_registrar(RRDBCon *con, unsigned registrar_id, unsigned serial, RRDBStatistics *stats);

#endif