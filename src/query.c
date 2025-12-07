#include "query.h"
#include "log.h"

#include <stdlib.h>
#include <string.h>

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

static bool rr_query_prepare_registrar_by_name(RRDBCon *con, RRDBStmt **stmt, char *name, RRDBRegistrar *out)
{
  RRDBStmt *st = rr_db_stmt_prepare(
    con,
    "SELECT id, name, serial, last_import FROM registrar WHERE name = ?",
    &(RRDBParam){ .type = RRDB_TYPE_STRING, .bind = name },
    RRDB_PARAM_OUT,
    &(RRDBParam){ .type = RRDB_TYPE_UINT, .bind = &out->id          },
    &(RRDBParam){ .type = RRDB_TYPE_UINT, .bind = out->name         },
    &(RRDBParam){ .type = RRDB_TYPE_UINT, .bind = &out->serial      },
    &(RRDBParam){ .type = RRDB_TYPE_UINT, .bind = &out->last_import },
    NULL);

  if (!st)
    return false;

  *stmt = st;
  return true;
}

bool rr_query_registrar_by_name(
  RRDBCon *con,
  const char *in_name,
  unsigned *out_registrar_id,
  unsigned *out_serial,
  unsigned *out_last_import)
{
  DBQueryData *qd = rr_db_get_con_udata(con);

  strcpy(qd->registrar.by_name.in_name, in_name);
  if (rr_db_stmt_fetch_one(qd->registrar.by_name.stmt))
  {
    *out_registrar_id = qd->registrar.by_name.out.id;
    *out_serial       = qd->registrar.by_name.out.serial;
    *out_last_import  = qd->registrar.by_name.out.last_import;
    return true;
  }
  return false;
}

static bool rr_query_prepare_registrar_insert(RRDBCon *con, RRDBStmt **stmt, char *name)
{
  RRDBStmt *st = rr_db_stmt_prepare(
    con,
    "INSERT INTO registrar (name, serial, last_import) VALUES (?, 0, 0)",
    &(RRDBParam){ .type = RRDB_TYPE_STRING, .bind = name },
    NULL);

  if (!st)
    return false;

  *stmt = st;
  return true;
}

bool rr_query_registrar_insert(
  RRDBCon *con,
  const char *in_name,
  unsigned *out_registrar_id)
{
  DBQueryData *qd = rr_db_get_con_udata(con);

  strcpy(qd->registrar.insert.in_name, in_name);
  if (!rr_db_stmt_execute(qd->registrar.insert.stmt, NULL))
    return false;
    
  *out_registrar_id = rr_db_stmt_insert_id(qd->registrar.insert.stmt);
  return true;
}

static bool rr_query_prepare_lookup_ipv4(RRDBCon *con, RRDBStmt **stmt, uint32_t *in, RRDBIPInfo *out)
{
  RRDBStmt *st;

  st = rr_db_stmt_prepare(con,
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

    &(RRDBParam){ .type = RRDB_TYPE_UINT, .bind = in, .size = sizeof(*in) },
    &(RRDBParam){ .type = RRDB_TYPE_UINT, .bind = in, .size = sizeof(*in) },

    RRDB_PARAM_OUT,

    &(RRDBParam){ .type = RRDB_TYPE_UBIGINT, .bind = &out->id                                            },
    &(RRDBParam){ .type = RRDB_TYPE_UINT   , .bind = &out->registrar_id                                  },
    &(RRDBParam){ .type = RRDB_TYPE_STRING , .bind = &out->org_id_str   , .size = sizeof(out->org_id_str) },
    &(RRDBParam){ .type = RRDB_TYPE_STRING , .bind = &out->org_name     , .size = sizeof(out->org_name  ) },
    &(RRDBParam){ .type = RRDB_TYPE_UINT   , .bind = &out->start_ip.v4                                   },
    &(RRDBParam){ .type = RRDB_TYPE_UINT   , .bind = &out->end_ip  .v4                                   },
    &(RRDBParam){ .type = RRDB_TYPE_UINT8  , .bind = &out->prefix_len                                    },
    &(RRDBParam){ .type = RRDB_TYPE_STRING , .bind = &out->netname     , .size = sizeof(out->netname   ) },
    &(RRDBParam){ .type = RRDB_TYPE_STRING , .bind = &out->descr       , .size = sizeof(out->descr     ) },

    NULL);

  if (!st)
    return false;

  *stmt = st;
  return true;
}

static bool rr_query_prepare_lookup_ipv6(RRDBCon *con, RRDBStmt **stmt, unsigned __int128 *in, RRDBIPInfo *out)
{
  RRDBStmt *st;

  st = rr_db_stmt_prepare(con,
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

    &(RRDBParam){ .type = RRDB_TYPE_BINARY, .bind = in, .size = sizeof(*in) },
    &(RRDBParam){ .type = RRDB_TYPE_BINARY, .bind = in, .size = sizeof(*in) },

    RRDB_PARAM_OUT,

    &(RRDBParam){ .type = RRDB_TYPE_UBIGINT, .bind = &out->id                                             },
    &(RRDBParam){ .type = RRDB_TYPE_UINT   , .bind = &out->registrar_id                                   },
    &(RRDBParam){ .type = RRDB_TYPE_STRING , .bind = &out->org_id_str  , .size = sizeof(out->org_id_str ) },
    &(RRDBParam){ .type = RRDB_TYPE_STRING , .bind = &out->org_name    , .size = sizeof(out->org_name   ) },
    &(RRDBParam){ .type = RRDB_TYPE_BINARY , .bind = &out->start_ip.v6 , .size = sizeof(out->start_ip.v6) },
    &(RRDBParam){ .type = RRDB_TYPE_BINARY , .bind = &out->end_ip  .v6 , .size = sizeof(out->start_ip.v4) },
    &(RRDBParam){ .type = RRDB_TYPE_UINT8  , .bind = &out->prefix_len                                     },
    &(RRDBParam){ .type = RRDB_TYPE_STRING , .bind = &out->netname     , .size = sizeof(out->netname    ) },
    &(RRDBParam){ .type = RRDB_TYPE_STRING , .bind = &out->descr       , .size = sizeof(out->descr      ) },

    NULL);

  if (!st)
    return false;

  *stmt = st;
  return true;
}

bool rr_query_init(RRDBCon *con, void **udata)
{
  DBQueryData *cd = calloc(1, sizeof(*cd));
  if (!cd)
  {
    LOG_ERROR("out of memory");
    return false;
  }
  *udata = cd;

  if (!rr_query_prepare_registrar_by_name(con,
    &cd->registrar.by_name.stmt,
     cd->registrar.by_name.in_name,
    &cd->registrar.by_name.out))
  {
    LOG_ERROR("rr_query_prepare_registrar_by_name failed");
    return false;
  }

  if (!rr_query_prepare_registrar_insert(con,
    &cd->registrar.insert.stmt,
     cd->registrar.insert.in_name))
  {
    LOG_ERROR("rr_query_prepare_registrar_insert failed");
    return false;
  }


  if (!rr_query_prepare_lookup_ipv4(con,
    &cd->netblock_v4.by_addr.stmt,
    &cd->netblock_v4.by_addr.in_ipv4,
    &cd->netblock_v4.by_addr.out))
  {
    LOG_ERROR("rr_query_prepare_lookup_ipv4 failed");
    return false;
  }

  if (!rr_query_prepare_lookup_ipv6(con,
    &cd->netblock_v6.by_addr.stmt,
    &cd->netblock_v6.by_addr.in_ipv6,
    &cd->netblock_v6.by_addr.out))
  {
    LOG_ERROR("rr_query_prepare_lookup_ipv6 failed");
    return false;
  }

  return true;
}

bool rr_query_deinit(RRDBCon *con, void **udata)
{
  if (!*udata)
    return true;

  DBQueryData *cd = *udata;

  rr_db_stmt_free(&cd->registrar.by_name  .stmt);
  rr_db_stmt_free(&cd->registrar.insert   .stmt);
  rr_db_stmt_free(&cd->netblock_v4.by_addr.stmt);
  rr_db_stmt_free(&cd->netblock_v6.by_addr.stmt);

  free(cd);
  *udata = NULL;  
  return true;
}