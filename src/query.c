#include "query.h"
#include "log.h"

#include <stdlib.h>
#include <string.h>

#include "query_macros.h"

typedef struct DBQueryData
{
  // lookup a registrar record by name
  STMT_STRUCT(registrar_by_name,
    char           in_name[32];
    RRDBRegistrar  out;  
  );

  // insert a new registrar record
  STMT_STRUCT(registrar_insert,
    char in_name[32];
  );

  // lookup best match by address
  STMT_STRUCT(lookup_ipv4_by_addr,  
    uint32_t   in_ipv4;
    RRDBIPInfo out;
  );

  // lookup best match by address
  STMT_STRUCT(lookup_ipv6_by_addr,
    unsigned __int128 in_ipv6;
    RRDBIPInfo out;
  );
}
DBQueryData;

#define STATEMENTS \
  X(registrar_by_name  ) \
  X(registrar_insert   ) \
  X(lookup_ipv4_by_addr) \
  X(lookup_ipv6_by_addr)

#pragma region registrar_by_name
DEFAULT_STMT(registrar_by_name,
  RRDBStmt *st = rr_db_stmt_prepare(
    con,
    "SELECT id, name, serial, last_import FROM registrar WHERE name = ?",
    &(RRDBParam){ .type = RRDB_TYPE_STRING, .bind = this->in_name },
    RRDB_PARAM_OUT,
    &(RRDBParam){ .type = RRDB_TYPE_UINT, .bind = &this->out.id          },
    &(RRDBParam){ .type = RRDB_TYPE_UINT, .bind =  this->out.name        },
    &(RRDBParam){ .type = RRDB_TYPE_UINT, .bind = &this->out.serial      },
    &(RRDBParam){ .type = RRDB_TYPE_UINT, .bind = &this->out.last_import },
    NULL);
);

bool rr_query_registrar_by_name(
  RRDBCon *con,
  const char *in_name,
  unsigned *out_registrar_id,
  unsigned *out_serial,
  unsigned *out_last_import)
{
  DBQueryData *qd = rr_db_get_con_udata(con);

  strcpy(qd->registrar_by_name.in_name, in_name);
  if (rr_db_stmt_fetch_one(qd->registrar_by_name.stmt))
  {
    *out_registrar_id = qd->registrar_by_name.out.id;
    *out_serial       = qd->registrar_by_name.out.serial;
    *out_last_import  = qd->registrar_by_name.out.last_import;
    return true;
  }
  return false;
}
#pragma endregion

#pragma region registrar_insert
DEFAULT_STMT(registrar_insert,
  RRDBStmt *st = rr_db_stmt_prepare(
    con,
    "INSERT INTO registrar (name, serial, last_import) VALUES (?, 0, 0)",
    &(RRDBParam){ .type = RRDB_TYPE_STRING, .bind = this->in_name },
    NULL);
);

bool rr_query_registrar_insert(
  RRDBCon *con,
  const char *in_name,
  unsigned *out_registrar_id)
{
  DBQueryData *qd = rr_db_get_con_udata(con);

  strcpy(qd->registrar_insert.in_name, in_name);
  if (!rr_db_stmt_execute(qd->registrar_insert.stmt, NULL))
    return false;

  *out_registrar_id = rr_db_stmt_insert_id(qd->registrar_insert.stmt);
  return true;
}
#pragma endregion

#pragma region lookup_ipv4_by_addr
DEFAULT_STMT(lookup_ipv4_by_addr,
  RRDBStmt *st = rr_db_stmt_prepare(con,
    "SELECT "
      "a.id, "
      "a.registrar_id, "
      "a.org_id_str, "
      "b.org_name, "
      "a.start_ip, "
      "a.end_ip, "
      "a.prefix_len, "
      "a.netname, "
      "a.descr "
    "FROM "
      "netblock_v4   AS a "
      "LEFT JOIN org AS b ON b.id = a.org_id "
    "WHERE "
      "start_ip <= ? AND end_ip >= ? "
    "ORDER BY "
      "start_ip DESC "
    "LIMIT 1",

    &(RRDBParam){ .type = RRDB_TYPE_UINT, .bind = &this->in_ipv4, .size = sizeof(this->in_ipv4) },
    &(RRDBParam){ .type = RRDB_TYPE_UINT, .bind = &this->in_ipv4, .size = sizeof(this->in_ipv4) },

    RRDB_PARAM_OUT,

    &(RRDBParam){ .type = RRDB_TYPE_UBIGINT, .bind = &this->out.id                                                  },
    &(RRDBParam){ .type = RRDB_TYPE_UINT   , .bind = &this->out.registrar_id                                        },
    &(RRDBParam){ .type = RRDB_TYPE_STRING , .bind = &this->out.org_id_str   , .size = sizeof(this->out.org_id_str) },
    &(RRDBParam){ .type = RRDB_TYPE_STRING , .bind = &this->out.org_name     , .size = sizeof(this->out.org_name  ) },
    &(RRDBParam){ .type = RRDB_TYPE_UINT   , .bind = &this->out.start_ip.v4                                         },
    &(RRDBParam){ .type = RRDB_TYPE_UINT   , .bind = &this->out.end_ip  .v4                                         },
    &(RRDBParam){ .type = RRDB_TYPE_UINT8  , .bind = &this->out.prefix_len                                          },
    &(RRDBParam){ .type = RRDB_TYPE_STRING , .bind = &this->out.netname     , .size = sizeof(this->out.netname    ) },
    &(RRDBParam){ .type = RRDB_TYPE_STRING , .bind = &this->out.descr       , .size = sizeof(this->out.descr      ) },

    NULL);
)

bool rr_query_netblockv4_by_ip(
  RRDBCon     *con,
  uint32_t     in_ipv4,
  RRDBIPInfo  *out)
{
  DBQueryData *qd = rr_db_get_con_udata(con);

  qd->lookup_ipv4_by_addr.in_ipv4 = in_ipv4;
  if (!rr_db_stmt_fetch_one(qd->lookup_ipv4_by_addr.stmt))
    return false;

  memcpy(out, &qd->lookup_ipv4_by_addr.out, sizeof(*out));
  return true;
}
#pragma endregion

#pragma region lookup_ipv6_by_addr
DEFAULT_STMT(lookup_ipv6_by_addr,
  RRDBStmt *st = rr_db_stmt_prepare(con,
    "SELECT "
      "a.id, "
      "a.registrar_id, "
      "a.org_id_str, "
      "b.org_name, "
      "a.start_ip, "
      "a.end_ip, "
      "a.prefix_len, "
      "a.netname, "
      "a.descr "
    "FROM "
      "netblock_v6   AS a "
      "LEFT JOIN org AS b ON b.id = a.org_id "
    "WHERE "
      "start_ip <= ? AND end_ip >= ? "
    "ORDER BY "
      "start_ip DESC "
    "LIMIT 1",

    &(RRDBParam){ .type = RRDB_TYPE_BINARY, .bind = &this->in_ipv6, .size = sizeof(this->in_ipv6) },
    &(RRDBParam){ .type = RRDB_TYPE_BINARY, .bind = &this->in_ipv6, .size = sizeof(this->in_ipv6) },

    RRDB_PARAM_OUT,

    &(RRDBParam){ .type = RRDB_TYPE_UBIGINT, .bind = &this->out.id                                             },
    &(RRDBParam){ .type = RRDB_TYPE_UINT   , .bind = &this->out.registrar_id                                   },
    &(RRDBParam){ .type = RRDB_TYPE_STRING , .bind = &this->out.org_id_str  , .size = sizeof(this->out.org_id_str ) },
    &(RRDBParam){ .type = RRDB_TYPE_STRING , .bind = &this->out.org_name    , .size = sizeof(this->out.org_name   ) },
    &(RRDBParam){ .type = RRDB_TYPE_BINARY , .bind = &this->out.start_ip.v6 , .size = sizeof(this->out.start_ip.v6) },
    &(RRDBParam){ .type = RRDB_TYPE_BINARY , .bind = &this->out.end_ip  .v6 , .size = sizeof(this->out.start_ip.v4) },
    &(RRDBParam){ .type = RRDB_TYPE_UINT8  , .bind = &this->out.prefix_len                                          },
    &(RRDBParam){ .type = RRDB_TYPE_STRING , .bind = &this->out.netname     , .size = sizeof(this->out.netname    ) },
    &(RRDBParam){ .type = RRDB_TYPE_STRING , .bind = &this->out.descr       , .size = sizeof(this->out.descr      ) },

    NULL);
)

bool rr_query_netblockv6_by_ip(
  RRDBCon           *con,
  unsigned __int128  in_ipv6,
  RRDBIPInfo        *out)
{
  DBQueryData *qd = rr_db_get_con_udata(con);

  qd->lookup_ipv6_by_addr.in_ipv6 = in_ipv6;
  if (!rr_db_stmt_fetch_one(qd->lookup_ipv6_by_addr.stmt))
    return false;

  memcpy(out, &qd->lookup_ipv6_by_addr.out, sizeof(*out));
  return true;
}
#pragma endregion

#pragma region query_setup
bool rr_query_init(RRDBCon *con, void **udata)
{
  DBQueryData *qd = calloc(1, sizeof(*qd));
  if (!qd)
  {
    LOG_ERROR("out of memory");
    return false;
  }
  *udata = qd;

  #define X(x) \
    if (!rr_query_prepare_ ##x(con, qd)) \
    { \
      LOG_ERROR("rr_query_prepare_" #x " failed"); \
      return false; \
    }
  STATEMENTS
  #undef X
  return true;
}

bool rr_query_deinit(RRDBCon *con, void **udata)
{
  if (!*udata)
    return true;

  DBQueryData *qd = *udata;

  #define X(x) rr_query_free_ ##x(con, qd);
  STATEMENTS
  #undef X

  free(qd);
  *udata = NULL;  
  return true;
}
#pragma endregion


bool rr_query_prepare_org_insert(RRDBCon *con, RRDBStmt **stmt, RRDBOrg *in)
{
  RRDBStmt *st = rr_db_stmt_prepare(
    con,
    "INSERT INTO org ("
      "registrar_id, "
      "serial, "
      "name, "
      "org_name, "
      "descr"
    ") VALUES ("
      "?,"
      "?,"
      "?,"
      "?,"
      "?"
    ") ON DUPLICATE KEY UPDATE "
      "serial   = VALUES(serial), "
      "org_name = VALUES(org_name), "
      "descr    = VALUES(descr)",

    &(RRDBParam){ .type = RRDB_TYPE_UINT  , .bind = &in->registrar_id },
    &(RRDBParam){ .type = RRDB_TYPE_UINT,   .bind = &in->serial       },
    &(RRDBParam){ .type = RRDB_TYPE_STRING, .bind = &in->name         },
    &(RRDBParam){ .type = RRDB_TYPE_STRING, .bind = &in->org_name     },
    &(RRDBParam){ .type = RRDB_TYPE_STRING, .bind = &in->descr        },
    NULL);

  if (!st)
    return false;

  *stmt = st;
  return true;
}

bool rr_query_prepare_netblockv4_insert(RRDBCon *con, RRDBStmt **stmt, RRDBNetBlock *in)
{
  RRDBStmt *st = rr_db_stmt_prepare(
    con,
    "INSERT INTO netblock_v4 ("
      "registrar_id, "
      "serial, "
      "org_id_str, "
      "start_ip, "
      "end_ip, "
      "prefix_len, "
      "netname, "
      "descr"
    ") VALUES ("
      "?,"
      "?,"
      "?,"
      "?,"
      "?,"
      "?,"
      "?,"
      "?"
    ") ON DUPLICATE KEY UPDATE "
      "serial  = VALUES(serial), "
      "netname = VALUES(netname), "
      "descr   = VALUES(descr)",

    &(RRDBParam){ .type = RRDB_TYPE_UINT  , .bind = &in->registrar_id },
    &(RRDBParam){ .type = RRDB_TYPE_UINT  , .bind = &in->serial       },
    &(RRDBParam){ .type = RRDB_TYPE_STRING, .bind = &in->org_id_str   },
    &(RRDBParam){ .type = RRDB_TYPE_UINT  , .bind = &in->startAddr.v4 },
    &(RRDBParam){ .type = RRDB_TYPE_UINT  , .bind = &in->endAddr  .v4 },
    &(RRDBParam){ .type = RRDB_TYPE_UINT8 , .bind = &in->prefixLen    },
    &(RRDBParam){ .type = RRDB_TYPE_STRING, .bind = &in->netname      },
    &(RRDBParam){ .type = RRDB_TYPE_STRING, .bind = &in->descr        },
    NULL
  );

  if (!st)
    return false;

  *stmt = st;
  return true;
}

bool rr_query_prepare_netblockv6_insert(RRDBCon *con, RRDBStmt **stmt, RRDBNetBlock *in)
{
  RRDBStmt *st = rr_db_stmt_prepare(
    con,
    "INSERT INTO netblock_v6 ("
      "registrar_id, "
      "serial, "
      "org_id_str, "
      "start_ip, "
      "end_ip, "
      "prefix_len, "
      "netname, "
      "descr"
    ") VALUES ("
      "?,"
      "?,"
      "?,"
      "?,"
      "?,"
      "?,"
      "?,"
      "?"
    ") ON DUPLICATE KEY UPDATE "
      "serial  = VALUES(serial), "
      "netname = VALUES(netname), "
      "descr   = VALUES(descr)",

    &(RRDBParam){ .type = RRDB_TYPE_UINT  , .bind = &in->registrar_id },
    &(RRDBParam){ .type = RRDB_TYPE_UINT  , .bind = &in->serial       },
    &(RRDBParam){ .type = RRDB_TYPE_STRING, .bind = &in->org_id_str   },
    &(RRDBParam){ .type = RRDB_TYPE_BINARY, .bind = &in->startAddr.v6, .size = sizeof(in->startAddr) },
    &(RRDBParam){ .type = RRDB_TYPE_BINARY, .bind = &in->endAddr  .v6, .size = sizeof(in->endAddr  ) },
    &(RRDBParam){ .type = RRDB_TYPE_UINT8 , .bind = &in->prefixLen    },
    &(RRDBParam){ .type = RRDB_TYPE_STRING, .bind = &in->netname      },
    &(RRDBParam){ .type = RRDB_TYPE_STRING, .bind = &in->descr        },

    NULL
  );

  if (!st)
    return false;

  *stmt = st;
  return true;
}

bool rr_query_finalize_registrar(RRDBCon *con, unsigned registrar_id, unsigned serial, RRDBStatistics *stats)
{
  RRDBStmt *st;

  // update the registrar serial & import timestamp
  st = rr_db_stmt_prepare(con,
    "UPDATE registrar SET serial = ?, last_import = UNIX_TIMESTAMP() WHERE id = ?",
    &(RRDBParam){ .type = RRDB_TYPE_UINT, .bind = &serial       },
    &(RRDBParam){ .type = RRDB_TYPE_UINT, .bind = &registrar_id },
    NULL);

  if (!st || !rr_db_stmt_execute(st, NULL))
  {
    rr_db_stmt_free(&st);
    return false;
  }
  rr_db_stmt_free(&st);

  // delete all old records
  st = rr_db_stmt_prepare(con,
    "DELETE FROM netblock_v4 WHERE registrar_id = ? AND serial != ?",
    &(RRDBParam){ .type = RRDB_TYPE_UINT, .bind = &registrar_id },
    &(RRDBParam){ .type = RRDB_TYPE_UINT, .bind = &serial       },
    NULL);

  if (!st || !rr_db_stmt_execute(st, &stats->deletedIPv4))
  {
    rr_db_stmt_free(&st);
    return false;
  }
  rr_db_stmt_free(&st);

  st = rr_db_stmt_prepare(con,
    "DELETE FROM netblock_v6 WHERE registrar_id = ? AND serial != ?",
    &(RRDBParam){ .type = RRDB_TYPE_UINT, .bind = &registrar_id },
    &(RRDBParam){ .type = RRDB_TYPE_UINT, .bind = &serial       },
    NULL);

  if (!st || !rr_db_stmt_execute(st, &stats->deletedIPv6))
  {
    rr_db_stmt_free(&st);
    return false;
  }
  rr_db_stmt_free(&st);

  st = rr_db_stmt_prepare(con,
    "DELETE FROM org WHERE registrar_id = ? AND serial != ?",
    &(RRDBParam){ .type = RRDB_TYPE_UINT, .bind = &registrar_id },
    &(RRDBParam){ .type = RRDB_TYPE_UINT, .bind = &serial       },
    NULL);    

  if (!st || !rr_db_stmt_execute(st, &stats->deletedOrgs))
  {
    rr_db_stmt_free(&st);
    return false;
  }  
  rr_db_stmt_free(&st);

  // link orgs to netblocks
  st = rr_db_stmt_prepare(con,
    "UPDATE netblock_v4 nb "
    "LEFT JOIN org o "
    "ON o.registrar_id = nb.registrar_id "
    "AND o.name = nb.org_id_str "
    "SET nb.org_id = o.id",
    NULL);

  if (!st || !rr_db_stmt_execute(st, NULL))
  {
    rr_db_stmt_free(&st);
    return false;
  }
  rr_db_stmt_free(&st);

  st = rr_db_stmt_prepare(con,
    "UPDATE netblock_v6 nb "
    "LEFT JOIN org o "
    "ON o.registrar_id = nb.registrar_id "
    "AND o.name = nb.org_id_str "
    "SET nb.org_id = o.id",
    NULL);

  if (!st || !rr_db_stmt_execute(st, NULL))
  {
    rr_db_stmt_free(&st);
    return false;
  }
  rr_db_stmt_free(&st);    

  LOG_INFO("Import Statistics");
  LOG_INFO("Organizations:");
  LOG_INFO("  New    : %llu", stats->newOrgs    );
  LOG_INFO("  Deleted: %llu", stats->deletedOrgs);
  LOG_INFO("IPv4:");
  LOG_INFO("  New    : %llu", stats->newIPv4    );
  LOG_INFO("  Deleted: %llu", stats->deletedIPv4);
  LOG_INFO("IPv6:");
  LOG_INFO("  New    : %llu", stats->newIPv6    );
  LOG_INFO("  Deleted: %llu", stats->deletedIPv6);

  return true;
};