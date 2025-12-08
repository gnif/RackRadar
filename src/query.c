#include "query.h"
#include "log.h"
#include "import.h" // temp

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

#define STATEMENTS(X) \
  X(registrar_by_name  ) \
  X(registrar_insert   ) \
  X(lookup_ipv4_by_addr) \
  X(lookup_ipv6_by_addr)

#pragma region registrar_by_name
DEFAULT_STMT(DBQueryData, registrar_by_name,
  "SELECT id, name, serial, last_import FROM registrar WHERE name = ?",
  &(RRDBParam){ .type = RRDB_TYPE_STRING, .bind = this->in_name },
  RRDB_PARAM_OUT,
  &(RRDBParam){ .type = RRDB_TYPE_UINT, .bind = &this->out.id          },
  &(RRDBParam){ .type = RRDB_TYPE_UINT, .bind =  this->out.name        },
  &(RRDBParam){ .type = RRDB_TYPE_UINT, .bind = &this->out.serial      },
  &(RRDBParam){ .type = RRDB_TYPE_UINT, .bind = &this->out.last_import }
);

int rr_query_registrar_by_name(
  RRDBCon *con,
  const char *in_name,
  unsigned *out_registrar_id,
  unsigned *out_serial,
  unsigned *out_last_import)
{
  DBQueryData *qd = rr_db_get_con_gudata(con);

  strcpy(qd->registrar_by_name.in_name, in_name);
  int rc = rr_db_stmt_fetch_one(qd->registrar_by_name.stmt);
  if (rc < 1)
    return rc;

  *out_registrar_id = qd->registrar_by_name.out.id;
  *out_serial       = qd->registrar_by_name.out.serial;
  *out_last_import  = qd->registrar_by_name.out.last_import;
  return 1;
}
#pragma endregion

#pragma region registrar_insert
DEFAULT_STMT(DBQueryData, registrar_insert,
  "INSERT INTO registrar (name, serial, last_import) VALUES (?, 0, 0)",
  &(RRDBParam){ .type = RRDB_TYPE_STRING, .bind = this->in_name }
);

int rr_query_registrar_insert(
  RRDBCon *con,
  const char *in_name,
  unsigned *out_registrar_id)
{
  DBQueryData *qd = rr_db_get_con_gudata(con);

  strcpy(qd->registrar_insert.in_name, in_name);
  int rc = rr_db_stmt_execute(qd->registrar_insert.stmt, NULL);
  if (rc < 1)
    return rc;

  *out_registrar_id = rr_db_stmt_insert_id(qd->registrar_insert.stmt);
  return 1;
}
#pragma endregion

#pragma region lookup_ipv4_by_addr
DEFAULT_STMT(DBQueryData, lookup_ipv4_by_addr,
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
    /* Guard: only proceed if IP is inside the union coverage */
    "netblock_v4_union u "
    "JOIN ( "
      "SELECT end_ip "
      "FROM netblock_v4_union "
      "WHERE start_ip <= ? "
      "ORDER BY start_ip DESC "
      "LIMIT 1 "
    ") g ON g.end_ip >= ? "
    /* Real lookup */
    "JOIN ( "
      "SELECT id "
      "FROM netblock_v4 "
      "WHERE start_ip <= ? AND end_ip >= ? "
      "ORDER BY start_ip DESC "
      "LIMIT 1 "
    ") x ON 1=1 "
    "JOIN netblock_v4 AS a ON a.id = x.id "
    "LEFT JOIN org AS b ON b.id = a.org_id",

  /* union guard inputs */
  &(RRDBParam){ .type = RRDB_TYPE_UINT, .bind = &this->in_ipv4, .size = sizeof(this->in_ipv4) },
  &(RRDBParam){ .type = RRDB_TYPE_UINT, .bind = &this->in_ipv4, .size = sizeof(this->in_ipv4) },

  /* real lookup inputs */
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
  &(RRDBParam){ .type = RRDB_TYPE_STRING , .bind = &this->out.descr       , .size = sizeof(this->out.descr      ) }
)

int rr_query_netblockv4_by_ip(
  RRDBCon     *con,
  uint32_t     in_ipv4,
  RRDBIPInfo  *out)
{
  DBQueryData *qd = rr_db_get_con_gudata(con);

  qd->lookup_ipv4_by_addr.in_ipv4 = in_ipv4;
  int rc = rr_db_stmt_fetch_one(qd->lookup_ipv4_by_addr.stmt);
  if (rc < 1)
    return rc;

  memcpy(out, &qd->lookup_ipv4_by_addr.out, sizeof(*out));
  return 1;
}
#pragma endregion

#pragma region lookup_ipv6_by_addr
DEFAULT_STMT(DBQueryData, lookup_ipv6_by_addr,
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
    /* Guard: only proceed if IP is inside the union coverage */
    "(SELECT end_ip "
    " FROM netblock_v6_union "
    " WHERE start_ip <= ? "
    " ORDER BY start_ip DESC "
    " LIMIT 1) g "
    /* Real lookup */
    "JOIN ("
      "SELECT id "
      "FROM netblock_v6 FORCE INDEX (idx_start_end) "
      "WHERE start_ip <= ? AND end_ip >= ? "
      "ORDER BY start_ip DESC "
      "LIMIT 1"
    ") x ON g.end_ip >= ? "
  "JOIN netblock_v6 AS a ON a.id = x.id "
  "LEFT JOIN org AS b ON b.id = a.org_id",

  /* union guard: start_ip <= ip */
  &(RRDBParam){ .type = RRDB_TYPE_BINARY, .bind = &this->in_ipv6, .size = sizeof(this->in_ipv6) },

  /* real lookup: start_ip <= ip AND end_ip >= ip */
  &(RRDBParam){ .type = RRDB_TYPE_BINARY, .bind = &this->in_ipv6, .size = sizeof(this->in_ipv6) },
  &(RRDBParam){ .type = RRDB_TYPE_BINARY, .bind = &this->in_ipv6, .size = sizeof(this->in_ipv6) },

  /* guard check: g.end_ip >= ip */
  &(RRDBParam){ .type = RRDB_TYPE_BINARY, .bind = &this->in_ipv6, .size = sizeof(this->in_ipv6) },

  RRDB_PARAM_OUT,

  &(RRDBParam){ .type = RRDB_TYPE_UBIGINT, .bind = &this->out.id                                                  },
  &(RRDBParam){ .type = RRDB_TYPE_UINT   , .bind = &this->out.registrar_id                                        },
  &(RRDBParam){ .type = RRDB_TYPE_STRING , .bind = &this->out.org_id_str  , .size = sizeof(this->out.org_id_str ) },
  &(RRDBParam){ .type = RRDB_TYPE_STRING , .bind = &this->out.org_name    , .size = sizeof(this->out.org_name   ) },
  &(RRDBParam){ .type = RRDB_TYPE_BINARY , .bind = &this->out.start_ip.v6 , .size = sizeof(this->out.start_ip.v6) },
  &(RRDBParam){ .type = RRDB_TYPE_BINARY , .bind = &this->out.end_ip  .v6 , .size = sizeof(this->out.end_ip.v6  ) },
  &(RRDBParam){ .type = RRDB_TYPE_UINT8  , .bind = &this->out.prefix_len                                          },
  &(RRDBParam){ .type = RRDB_TYPE_STRING , .bind = &this->out.netname     , .size = sizeof(this->out.netname    ) },
  &(RRDBParam){ .type = RRDB_TYPE_STRING , .bind = &this->out.descr       , .size = sizeof(this->out.descr      ) }
)

int rr_query_netblockv6_by_ip(
  RRDBCon           *con,
  unsigned __int128  in_ipv6,
  RRDBIPInfo        *out)
{
  DBQueryData *qd = rr_db_get_con_gudata(con);

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
  STMT_PREPARE(STATEMENTS, qd);
  return true;
}

bool rr_query_deinit(RRDBCon *con, void **udata)
{
  if (!*udata)
    return true;

  DBQueryData *qd = *udata;
  STMT_FREE(STATEMENTS, qd);

  free(qd);
  *udata = NULL;
  return true;
}
#pragma endregion
