#include "import.h"
#include "config.h"
#include "util.h"
#include "log.h"
#include "download.h"
#include "db.h"
#include "query.h"
#include "query_macros.h"

#include "rpsl.h"
#include "arin.h"

#include <string.h>
#include <assert.h>
#include <stdarg.h>

typedef struct RRImport
{
  RRDownload    *dl;
  RRDBCon       *con;
  RRDBStatistics stats;

  STMT_STRUCT(registrar_insert,
    char in_name[32];
  );

  STMT_STRUCT(registrar_update_serial,
    unsigned in_registrar_id;
    unsigned in_serial;
  );

  STMT_STRUCT(org_insert,
    RRDBOrg in;
  );

  STMT_STRUCT(org_delete_old,
    unsigned in_registrar_id;
    unsigned in_serial;
  );

  STMT_STRUCT(netblockv4_insert,
    RRDBNetBlock in;
  );

  STMT_STRUCT(netblockv4_delete_old,
    unsigned in_registrar_id;
    unsigned in_serial;
  );

  STMT_STRUCT(netblockv4_link_org,);

  STMT_STRUCT(netblockv6_insert,
    RRDBNetBlock in;
  );

  STMT_STRUCT(netblockv6_delete_old,
    unsigned in_registrar_id;
    unsigned in_serial;
  );

  STMT_STRUCT(netblockv6_link_org,);

  STMT_STRUCT(netblockv4_union_truncate,);
  STMT_STRUCT(netblockv6_union_truncate,);
  STMT_STRUCT(netblockv4_union_populate,);
  STMT_STRUCT(netblockv6_union_populate,);

  STMT_STRUCT(list_insert,
    char in_list_name[32];
  );

  STMT_STRUCT(netblockv4_list_delete      , unsigned in_list_id; );
  STMT_STRUCT(netblockv6_list_delete      , unsigned in_list_id; );
  STMT_STRUCT(netblockv4_list_union_delete, unsigned in_list_id; );
  STMT_STRUCT(netblockv6_list_union_delete, unsigned in_list_id; );

  STMT_STRUCT(netblockv4_list_union_insert,
    unsigned in_list_id;
    unsigned in_ip;
    uint8_t  in_prefix_len;
  );

  STMT_STRUCT(netblockv6_list_union_insert,
    unsigned in_list_id;
    unsigned __int128 in_ip;
    uint8_t  in_prefix_len;
  );

  struct
  {
    char in_list_name[32];
    RRDBStmt *stmt[2];
  }
  *lists_prepare;
}
RRImport;
RRImport s_import = { 0 };

#define STATEMENTS(X) \
  X(registrar_insert              ) \
  X(registrar_update_serial       ) \
  X(org_insert                    ) \
  X(org_delete_old                ) \
  X(netblockv4_insert             ) \
  X(netblockv4_delete_old         ) \
  X(netblockv4_link_org           ) \
  X(netblockv6_insert             ) \
  X(netblockv6_delete_old         ) \
  X(netblockv6_link_org           ) \
  X(netblockv4_union_truncate     ) \
  X(netblockv6_union_truncate     ) \
  X(netblockv4_union_populate     ) \
  X(netblockv6_union_populate     ) \
  X(list_insert                   ) \
  X(netblockv4_list_delete        ) \
  X(netblockv6_list_delete        ) \
  X(netblockv4_list_union_delete  ) \
  X(netblockv6_list_union_delete  ) \
  X(netblockv4_list_union_insert  ) \
  X(netblockv6_list_union_insert  )

#pragma region statements
DEFAULT_STMT(RRImport, registrar_insert,
  "INSERT INTO registrar (name, serial, last_import) VALUES (?, 0, 0)",
  &(RRDBParam){ .type = RRDB_TYPE_STRING, .bind = this->in_name }
);

DEFAULT_STMT(RRImport, registrar_update_serial,
  "UPDATE registrar SET serial = ?, last_import = UNIX_TIMESTAMP() WHERE id = ?",
  &(RRDBParam){ .type = RRDB_TYPE_UINT, .bind = &this->in_serial       },
  &(RRDBParam){ .type = RRDB_TYPE_UINT, .bind = &this->in_registrar_id }
);

DEFAULT_STMT(RRImport, org_insert,
  "INSERT INTO org ("
    "registrar_id, "
    "serial, "
    "handle, "
    "name, "
    "descr"
  ") VALUES ("
    "?,"
    "?,"
    "?,"
    "?,"
    "?"
  ") ON DUPLICATE KEY UPDATE "
    "serial = VALUES(serial), "
    "name   = VALUES(name), "
    "descr  = VALUES(descr)",

  &(RRDBParam){ .type = RRDB_TYPE_UINT  , .bind = &this->in.registrar_id },
  &(RRDBParam){ .type = RRDB_TYPE_UINT,   .bind = &this->in.serial       },
  &(RRDBParam){ .type = RRDB_TYPE_STRING, .bind = &this->in.handle       },
  &(RRDBParam){ .type = RRDB_TYPE_STRING, .bind = &this->in.name         },
  &(RRDBParam){ .type = RRDB_TYPE_STRING, .bind = &this->in.descr        }
);

DEFAULT_STMT(RRImport, org_delete_old,
  "DELETE FROM org WHERE registrar_id = ? AND serial != ?",
  &(RRDBParam){ .type = RRDB_TYPE_UINT, .bind = &this->in_registrar_id },
  &(RRDBParam){ .type = RRDB_TYPE_UINT, .bind = &this->in_serial       }
);

DEFAULT_STMT(RRImport, netblockv4_insert,
  "INSERT INTO netblock_v4 ("
    "registrar_id, "
    "serial, "
    "org_handle, "
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

  &(RRDBParam){ .type = RRDB_TYPE_UINT  , .bind = &this->in.registrar_id },
  &(RRDBParam){ .type = RRDB_TYPE_UINT  , .bind = &this->in.serial       },
  &(RRDBParam){ .type = RRDB_TYPE_STRING, .bind = &this->in.org_handle   },
  &(RRDBParam){ .type = RRDB_TYPE_UINT  , .bind = &this->in.startAddr.v4 },
  &(RRDBParam){ .type = RRDB_TYPE_UINT  , .bind = &this->in.endAddr  .v4 },
  &(RRDBParam){ .type = RRDB_TYPE_UINT8 , .bind = &this->in.prefixLen    },
  &(RRDBParam){ .type = RRDB_TYPE_STRING, .bind = &this->in.netname      },
  &(RRDBParam){ .type = RRDB_TYPE_STRING, .bind = &this->in.descr        }
);

DEFAULT_STMT(RRImport, netblockv4_delete_old,
  "DELETE FROM netblock_v4 WHERE registrar_id = ? AND serial != ?",
  &(RRDBParam){ .type = RRDB_TYPE_UINT, .bind = &this->in_registrar_id },
  &(RRDBParam){ .type = RRDB_TYPE_UINT, .bind = &this->in_serial       }
);

DEFAULT_STMT(RRImport, netblockv4_link_org,
  "UPDATE netblock_v4 nb "
    "LEFT JOIN org o "
    "ON o.registrar_id = nb.registrar_id "
    "AND o.handle = nb.org_handle "
    "SET nb.org_id = o.id"
);

DEFAULT_STMT(RRImport, netblockv6_insert,
  "INSERT INTO netblock_v6 ("
    "registrar_id, "
    "serial, "
    "org_handle, "
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

  &(RRDBParam){ .type = RRDB_TYPE_UINT  , .bind = &this->in.registrar_id },
  &(RRDBParam){ .type = RRDB_TYPE_UINT  , .bind = &this->in.serial       },
  &(RRDBParam){ .type = RRDB_TYPE_STRING, .bind = &this->in.org_handle   },
  &(RRDBParam){ .type = RRDB_TYPE_BINARY, .bind = &this->in.startAddr.v6, .size = sizeof(this->in.startAddr) },
  &(RRDBParam){ .type = RRDB_TYPE_BINARY, .bind = &this->in.endAddr  .v6, .size = sizeof(this->in.endAddr  ) },
  &(RRDBParam){ .type = RRDB_TYPE_UINT8 , .bind = &this->in.prefixLen    },
  &(RRDBParam){ .type = RRDB_TYPE_STRING, .bind = &this->in.netname      },
  &(RRDBParam){ .type = RRDB_TYPE_STRING, .bind = &this->in.descr        }
);

DEFAULT_STMT(RRImport, netblockv6_delete_old,
  "DELETE FROM netblock_v6 WHERE registrar_id = ? AND serial != ?",
  &(RRDBParam){ .type = RRDB_TYPE_UINT, .bind = &this->in_registrar_id },
  &(RRDBParam){ .type = RRDB_TYPE_UINT, .bind = &this->in_serial       }
);

DEFAULT_STMT(RRImport, netblockv6_link_org,
  "UPDATE netblock_v6 nb "
    "LEFT JOIN org o "
    "ON o.registrar_id = nb.registrar_id "
    "AND o.handle = nb.org_handle "
    "SET nb.org_id = o.id"
);

DEFAULT_STMT(RRImport, netblockv4_union_truncate,
  "TRUNCATE TABLE netblock_v4_union"
);

DEFAULT_STMT(RRImport, netblockv6_union_truncate,
  "TRUNCATE TABLE netblock_v6_union"
);

DEFAULT_STMT(RRImport, netblockv4_union_populate,
  "INSERT INTO netblock_v4_union (start_ip, end_ip) "
  "SELECT MIN(start_ip) AS start_ip, MAX(running_end) AS end_ip "
  "FROM ( "
    "SELECT "
      "t.start_ip, "
      "(@grp := @grp + (t.start_ip > @end)) AS grp, "
      "(@end := IF(t.start_ip > @end, t.end_ip, GREATEST(@end, t.end_ip))) AS running_end "
    "FROM ( "
      "SELECT start_ip, end_ip "
      "FROM netblock_v4 FORCE INDEX (idx_start_end) "
      "ORDER BY start_ip, end_ip "
    ") t "
    "CROSS JOIN (SELECT @grp := -1, @end := -1) vars "
  ") x "
  "GROUP BY grp"
);

DEFAULT_STMT(RRImport, netblockv6_union_populate,
  "INSERT INTO netblock_v6_union (start_ip, end_ip) "
  "SELECT MIN(start_ip) AS start_ip, MAX(running_end) AS end_ip "
  "FROM ( "
    "SELECT "
      "t.start_ip, "
      "(@grp := @grp + (t.start_ip > CAST(@end AS BINARY(16)))) AS grp, "
      "(@end := IF( "
        "t.start_ip > CAST(@end AS BINARY(16)), "
        "t.end_ip, "
        "IF(t.end_ip > CAST(@end AS BINARY(16)), t.end_ip, CAST(@end AS BINARY(16))) "
      ")) AS running_end "
    "FROM ( "
      "SELECT start_ip, end_ip "
      "FROM netblock_v6 FORCE INDEX (idx_start_end) "
      "ORDER BY start_ip, end_ip "
    ") t "
    "CROSS JOIN ( "
      "SELECT @grp := -1, @end := CAST(UNHEX('00000000000000000000000000000000') AS BINARY(16)) "
    ") vars "
  ") x "
  "GROUP BY grp"
);

DEFAULT_STMT(RRImport, list_insert,
  "INSERT IGNORE INTO list (name) VALUES (?)",
  &(RRDBParam){ .type = RRDB_TYPE_STRING, .bind = &this->in_list_name }
);

DEFAULT_STMT(RRImport, netblockv4_list_delete,
  "DELETE FROM netblock_v4_list WHERE list_id = ?",
  &(RRDBParam){ .type = RRDB_TYPE_UINT, .bind = &this->in_list_id }
);

DEFAULT_STMT(RRImport, netblockv6_list_delete,
  "DELETE FROM netblock_v6_list WHERE list_id = ?",
  &(RRDBParam){ .type = RRDB_TYPE_UINT, .bind = &this->in_list_id }
);

DEFAULT_STMT(RRImport, netblockv4_list_union_delete,
  "DELETE FROM netblock_v4_list_union WHERE list_id = ?",
  &(RRDBParam){ .type = RRDB_TYPE_UINT, .bind = &this->in_list_id }
);

DEFAULT_STMT(RRImport, netblockv6_list_union_delete,
  "DELETE FROM netblock_v6_list_union WHERE list_id = ?",
  &(RRDBParam){ .type = RRDB_TYPE_UINT, .bind = &this->in_list_id }
);

DEFAULT_STMT(RRImport, netblockv4_list_union_insert,
  "INSERT INTO netblock_v4_list_union (list_id, ip, prefix_len) VALUES (?, ?, ?)",
  &(RRDBParam){ .type = RRDB_TYPE_UINT , .bind = &this->in_list_id    },
  &(RRDBParam){ .type = RRDB_TYPE_UINT , .bind = &this->in_ip         },
  &(RRDBParam){ .type = RRDB_TYPE_UINT8, .bind = &this->in_prefix_len }
);

DEFAULT_STMT(RRImport, netblockv6_list_union_insert,
  "INSERT INTO netblock_v6_list_union (list_id, ip, prefix_len) VALUES (?, ?, ?)",
  &(RRDBParam){ .type = RRDB_TYPE_UINT  , .bind = &this->in_list_id    },
  &(RRDBParam){ .type = RRDB_TYPE_BINARY, .bind = &this->in_ip, .size = sizeof(this->in_ip) },
  &(RRDBParam){ .type = RRDB_TYPE_UINT8 , .bind = &this->in_prefix_len }
);
#pragma endregion

#pragma region statement_interfaces

static int rr_import_registrar_insert(const char *in_name, unsigned *out_registrar_id)
{
  strcpy(s_import.registrar_insert.in_name, in_name);
  int rc = rr_db_stmt_execute(s_import.registrar_insert.stmt, NULL);
  if (rc < 1)
  {
    LOG_ERROR(
      "rr_import_registrar_insert failed:\n"
      "  name: %s\n",
      in_name
    );

    return rc;
  }

  *out_registrar_id = rr_db_stmt_insert_id(s_import.registrar_insert.stmt);
  return 1;
}

static bool rr_import_registrar_update_serial(unsigned in_registrar_id, unsigned in_serial)
{
  s_import.registrar_update_serial.in_registrar_id = in_registrar_id;
  s_import.registrar_update_serial.in_serial       = in_serial;
  return rr_db_stmt_execute(s_import.registrar_update_serial.stmt, NULL);
}

bool rr_import_org_insert(RRDBOrg *in_org)
{
  unsigned long long ra;
  memcpy(&s_import.org_insert.in, in_org, sizeof(*in_org));
  if (rr_db_stmt_execute(s_import.org_insert.stmt, &ra))
  {
    if (ra == 1)
      ++s_import.stats.newOrgs;
    return true;
  }

  LOG_ERROR(
    "rr_import_org_insert failed:\n"
    "  registrar_id: %u\n"
    "  serial      : %u\n"
    "  handle      : %s\n"
    "  name        : %s\n"
    "  descr       : %s\n",
    in_org->registrar_id,
    in_org->serial,
    in_org->handle,
    in_org->name,
    in_org->descr
  );

  return false;
}

static bool rr_import_org_delete_old(unsigned in_registrar_id, unsigned in_serial)
{
  s_import.org_delete_old.in_registrar_id = in_registrar_id;
  s_import.org_delete_old.in_serial       = in_serial;
  return rr_db_stmt_execute(s_import.org_delete_old.stmt, &s_import.stats.deletedOrgs);
}

bool rr_import_netblockv4_insert(RRDBNetBlock *in_netblock)
{
  unsigned long long ra;
  memcpy(&s_import.netblockv4_insert.in, in_netblock, sizeof(*in_netblock));
  if (rr_db_stmt_execute(s_import.netblockv4_insert.stmt, &ra))
  {
    if (ra == 1)
      ++s_import.stats.newIPv4;
    return true;
  }

  char sAddr[32];
  char eAddr[32];
  uint32_t s = htonl(in_netblock->startAddr.v4);
  uint32_t e = htonl(in_netblock->endAddr  .v4);
  inet_ntop(AF_INET, &s, sAddr, sizeof(sAddr));
  inet_ntop(AF_INET, &e, eAddr, sizeof(eAddr));

  LOG_ERROR(
    "rr_import_netblockv4_insert insert failed:\n"
    "  registrar_id: %u\n"
    "  serial      : %u\n"
    "  startAddr   : %s\n"
    "  endAddr     : %s\n"
    "  prefix_len  : %u\n"
    "  netname     : %s\n"
    "  org_handle  : %s\n"
    "  descr       : %s\n",
    in_netblock->registrar_id,
    in_netblock->serial,
    sAddr,
    eAddr,
    in_netblock->prefixLen,
    in_netblock->netname,
    in_netblock->org_handle,
    in_netblock->descr);

  return false;
}

static bool rr_import_netblockv4_delete_old(unsigned in_registrar_id, unsigned in_serial)
{
  s_import.netblockv4_delete_old.in_registrar_id = in_registrar_id;
  s_import.netblockv4_delete_old.in_serial       = in_serial;
  return rr_db_stmt_execute(s_import.netblockv4_delete_old.stmt, &s_import.stats.deletedIPv4);
}

static bool rr_import_netblockv4_link_org(void)
{
  return rr_db_stmt_execute(s_import.netblockv4_link_org.stmt, NULL);
}

bool rr_import_netblockv6_insert(RRDBNetBlock *in_netblock)
{
  unsigned long long ra;
  memcpy(&s_import.netblockv6_insert.in, in_netblock, sizeof(*in_netblock));
  if (rr_db_stmt_execute(s_import.netblockv6_insert.stmt, &ra))
  {
    if (ra == 1)
      ++s_import.stats.newIPv6;
    return true;
  }

  char sAddr[64];
  char eAddr[64];
  inet_ntop(AF_INET6, &in_netblock->startAddr.v6, sAddr, sizeof(sAddr));
  inet_ntop(AF_INET6, &in_netblock->endAddr  .v6, eAddr, sizeof(eAddr));

  LOG_ERROR(
    "rr_import_netblockv6_insert insert failed:\n"
    "  registrar_id: %u\n"
    "  serial      : %u\n"
    "  startAddr   : %s\n"
    "  endAddr     : %s\n"
    "  prefix_len  : %u\n"
    "  netname     : %s\n"
    "  org_handle  : %s\n"
    "  descr       : %s\n",
    in_netblock->registrar_id,
    in_netblock->serial,
    sAddr,
    eAddr,
    in_netblock->prefixLen,
    in_netblock->netname,
    in_netblock->org_handle,
    in_netblock->descr);

  return false;
}

static bool rr_import_netblockv6_delete_old(unsigned in_registrar_id, unsigned in_serial)
{
  s_import.netblockv6_delete_old.in_registrar_id = in_registrar_id;
  s_import.netblockv6_delete_old.in_serial       = in_serial;
  return rr_db_stmt_execute(s_import.netblockv6_delete_old.stmt, &s_import.stats.deletedIPv6);
}

static bool rr_import_netblockv6_link_org(void)
{
  return rr_db_stmt_execute(s_import.netblockv6_link_org.stmt, NULL);
}

static bool rr_import_netblockv4_union_truncate(void)
{
  return rr_db_stmt_execute(s_import.netblockv4_union_truncate.stmt, NULL);
}

static bool rr_import_netblockv6_union_truncate(void)
{
  return rr_db_stmt_execute(s_import.netblockv6_union_truncate.stmt, NULL);
}

static bool rr_import_netblockv4_union_populate(void)
{
  return rr_db_stmt_execute(s_import.netblockv4_union_populate.stmt, NULL);
}

static bool rr_import_netblockv6_union_populate(void)
{
  return rr_db_stmt_execute(s_import.netblockv6_union_populate.stmt, NULL);
}

static bool rr_import_list_insert(const char *in_list_name)
{
  strcpy(s_import.list_insert.in_list_name, in_list_name);
  return rr_db_stmt_execute(s_import.list_insert.stmt, NULL);
}

static bool rr_import_netblockv4_list_delete(unsigned in_list_id)
{
  s_import.netblockv4_list_delete.in_list_id = in_list_id;
  return rr_db_stmt_execute(s_import.netblockv4_list_delete.stmt, NULL);
}

static bool rr_import_netblockv6_list_delete(unsigned in_list_id)
{
  s_import.netblockv6_list_delete.in_list_id = in_list_id;
  return rr_db_stmt_execute(s_import.netblockv6_list_delete.stmt, NULL);
}

static bool rr_import_netblockv4_list_union_delete(unsigned in_list_id)
{
  s_import.netblockv4_list_union_delete.in_list_id = in_list_id;
  return rr_db_stmt_execute(s_import.netblockv4_list_union_delete.stmt, NULL);
}

static bool rr_import_netblockv6_list_union_delete(unsigned in_list_id)
{
  s_import.netblockv6_list_union_delete.in_list_id = in_list_id;
  return rr_db_stmt_execute(s_import.netblockv6_list_union_delete.stmt, NULL);
}

static bool rr_import_netblockv4_list_union_insert(unsigned in_list_id, unsigned in_ip, uint8_t in_prefix_len)
{
  s_import.netblockv4_list_union_insert.in_list_id    = in_list_id;
  s_import.netblockv4_list_union_insert.in_ip         = in_ip;
  s_import.netblockv4_list_union_insert.in_prefix_len = in_prefix_len;
  return rr_db_stmt_execute(s_import.netblockv4_list_union_insert.stmt, NULL);
}

static bool rr_import_netblockv6_list_union_insert(unsigned in_list_id, unsigned __int128 in_ip, uint8_t in_prefix_len)
{
  s_import.netblockv6_list_union_insert.in_list_id    = in_list_id;
  s_import.netblockv6_list_union_insert.in_ip         = in_ip;
  s_import.netblockv6_list_union_insert.in_prefix_len = in_prefix_len;
  return rr_db_stmt_execute(s_import.netblockv6_list_union_insert.stmt, NULL);
}

#pragma endregion

typedef struct
{
  ConfigList **order;
  unsigned     count;
}
ListOrder;

static int find_list_index(const char *name)
{
  for (unsigned i = 0; i < g_config.nbLists; ++i)
    if (strcmp(g_config.lists[i].name, name) == 0)
      return (int)i;
  return -1;
}

static bool build_list_order(ListOrder *lo)
{
  if (!g_config.lists || !lo)
    return true;

  unsigned n = g_config.nbLists;
  lo->order = calloc(n, sizeof(*lo->order));
  if (!lo->order)
    return false;

  unsigned *indeg   = calloc(n, sizeof(*indeg));
  unsigned *out_pos = calloc(n, sizeof(*out_pos));
  if (!indeg || !out_pos)
  {
    free(indeg);
    free(out_pos);
    free(lo->order);
    lo->order = NULL;
    return false;
  }

  unsigned edge_cap = 0, edge_count = 0;
  typedef struct { unsigned from, to; } Edge;
  Edge *edges = NULL;

  for (unsigned i = 0; i < n; ++i)
  {
    const ConfigList *cl = &g_config.lists[i];
    if (!cl->include)
      continue;

    for (const char **inc = cl->include; *inc; ++inc)
    {
      int from = find_list_index(*inc);
      if (from < 0)
        continue;

      ++indeg[i];
      if (edge_count == edge_cap)
      {
        edge_cap = edge_cap ? edge_cap * 2 : 16;
        Edge *tmp = realloc(edges, edge_cap * sizeof(*edges));
        if (!tmp)
          goto err;
        edges = tmp;
      }

      edges[edge_count++] = (Edge){ .from = (unsigned)from, .to = i };
    }
  }

  // Kahn's algorithm
  unsigned out_idx = 0;
  for (unsigned i = 0; i < n; ++i)
    if (indeg[i] == 0)
      out_pos[i] = 1; // mark as queued

  for (unsigned iter = 0; iter < n; ++iter)
  {
    bool progressed = false;
    for (unsigned i = 0; i < n; ++i)
    {
      if (indeg[i] == 0 && out_pos[i] == 1)
      {
        lo->order[out_idx++] = &g_config.lists[i];
        out_pos[i] = 2; // processed
        progressed = true;

        for (unsigned e = 0; e < edge_count; ++e)
        {
          if (edges[e].from == i && indeg[edges[e].to] > 0)
            --indeg[edges[e].to];
        }
      }
    }

    if (!progressed)
      break;
  }

  // Append any remaining (cycle or missing) lists in original order
  for (unsigned i = 0; i < n; ++i)
    if (out_pos[i] != 2)
      lo->order[out_idx++] = &g_config.lists[i];

  lo->count = out_idx;
  free(edges);
  free(indeg);
  free(out_pos);
  return true;

err:
  free(edges);
  free(indeg);
  free(out_pos);
  free(lo->order);
  lo->order = NULL;
  return false;
}

static typeof(s_import.lists_prepare) db_find_prepared_list(const char *name)
{
  for(typeof(s_import.lists_prepare) list = s_import.lists_prepare; list && list->stmt[0]; ++list)
    if (strcmp(list->in_list_name, name) == 0)
      return list;
  return NULL;
}

static bool db_build_list_query_where(ConfigList *cl, RRBuffer *qb, int stmt_idx, bool skip_prepared_includes)
{
  #define ADD_CONDITION(x, y, z) \
    if (cl->x ##_ ##y.z) \
      for(const char **str = cl->x ##_ ##y.z; *str; ++str, ++conditions) \
        if (!rr_buffer_appendf(qb, "%s" #x "." #y " LIKE '%s'", conditions > 0 ? " OR " : "", *str)) \
        { \
          LOG_ERROR("out of memory"); \
          return false; \
        }

  bool started = false;
  if (cl->has_matches)
  {
    started = true;
    if (!rr_buffer_appendf(qb, "("))
    {
      LOG_ERROR("out of memory");
      return false;
    }

    int conditions = 0;
    if (!rr_buffer_appendf(qb, "("))
    {
      LOG_ERROR("out of memory");
      return false;
    }

    #define X(x, y) ADD_CONDITION(x, y, match)
    CONFIG_LIST_FIELDS
    #undef X
    if (!rr_buffer_appendf(qb, ")"))
    {
      LOG_ERROR("out of memory");
      return false;
    }

    if (cl->has_ignores)
    {
      if (!rr_buffer_appendf(qb, " AND NOT ("))
      {
        LOG_ERROR("out of memory");
        return false;
      }

      conditions = 0;
      #define X(x, y) ADD_CONDITION(x, y, ignore)
      CONFIG_LIST_FIELDS
      #undef X
      if (!rr_buffer_appendf(qb, ")"))
      {
        LOG_ERROR("out of memory");
        return false;
      }
    }

    if (!rr_buffer_appendf(qb, ")"))
    {
      LOG_ERROR("out of memory");
      return false;
    }
  }

  if (cl->include)
  {
    for(const char **include = cl->include; *include; ++include)
      for(ConfigList *l = g_config.lists; l->name; ++l)
      {
        if (strcmp(l->name, *include) == 0)
        {
          // prevent infinite recursion
          if (l->include_seen)
            break;
          l->include_seen = true;

          if (skip_prepared_includes)
          {
            typeof(s_import.lists_prepare) prepared = db_find_prepared_list(*include);
            if (prepared && prepared->stmt[stmt_idx])
              break;
          }

          size_t before_or = qb->pos;
          if (started && !rr_buffer_appendf(qb, " OR ")) return false;

          size_t before_child = qb->pos;
          if (!db_build_list_query_where(l, qb, stmt_idx, skip_prepared_includes))
            return false;
          if (qb->pos == before_child)
          {
            qb->pos = before_or;
            qb->buffer[qb->pos] = '\0';
            break;
          }

          started = true;
          break;
        }
      }
  }

  if (cl->exclude)
  {
    size_t before_prefix = qb->pos;
    if (started)
    {
      if (!rr_buffer_appendf(qb, " AND NOT ("))
      {
        LOG_ERROR("out of memory");
        return false;
      }
    }
    else
    {
      if (!rr_buffer_appendf(qb, " NOT ("))
      {
        LOG_ERROR("out of memory");
        return false;
      }
    }

    bool any_exclude = false;
    for(const char **exclude = cl->exclude; *exclude; ++exclude)
      for(ConfigList *l = g_config.lists; l->name; ++l)
      {
        if (strcmp(l->name, *exclude) == 0)
        {
          // prevent infinite recursion
          if (l->exclude_seen)
            break;
          l->exclude_seen = true;

          size_t before_or = qb->pos;
          if (any_exclude && !rr_buffer_appendf(qb, " OR ")) return false;

          size_t before_child = qb->pos;
          if (!db_build_list_query_where(l, qb, stmt_idx, skip_prepared_includes))
            return false;

          if (qb->pos == before_child)
          {
            qb->pos = before_or;
            qb->buffer[qb->pos] = '\0';
            break;
          }

          any_exclude = true;
          break;
        }
      }

    if (!any_exclude)
    {
      qb->pos = before_prefix;
      qb->buffer[qb->pos] = '\0';
    }
    else if (!rr_buffer_appendf(qb, ")"))
    {
      LOG_ERROR("out of memory");
      return false;
    }
  }

  qb->buffer[qb->pos] = '\0';
  #undef ADD_CONDITION
  return true;
}

static bool db_init_fn(RRDBCon *con, void **udata)
{
  *udata = &s_import;
  STMT_PREPARE(STATEMENTS, *udata);

  if (!g_config.lists)
    return true;

  s_import.lists_prepare = calloc(g_config.nbLists + 1, sizeof(*s_import.lists_prepare));
  if (!s_import.lists_prepare)
  {
    LOG_ERROR("out of memory");
    return false;
  }

  bool ret = false;
  RRBuffer qb = { 0 };
  RRBuffer union_qb = { 0 };
  RRBuffer exclude_qb = { 0 };

  ListOrder order = { 0 };
  if (!build_list_order(&order))
  {
    LOG_ERROR("out of memory");
    goto err;
  }

  typeof(s_import.lists_prepare) list = s_import.lists_prepare;
  for(unsigned idx = 0; idx < order.count; ++idx)
  {
    ConfigList *cl = order.order[idx];
    for(int n = 0; n < 2; ++n)
    {
      const char *ver = n == 0 ? "v4" : "v6";
      const char *netblock_id = n == 0 ? "netblock_v4_id" : "netblock_v6_id";

      rr_buffer_reset(&qb);
      rr_buffer_reset(&union_qb);
      rr_buffer_reset(&exclude_qb);

      if (!rr_buffer_appendf(&qb,
        "INSERT INTO netblock_%s_list "
        "SELECT "
          "list.id, "
          "ip.id, "
          "ip.start_ip, "
          "ip.end_ip, "
          "ip.prefix_len "
        "FROM "
          "netblock_%s     AS ip "
          "RIGHT JOIN list AS list ON list.name = '%s' "
          "LEFT  JOIN org  AS org  ON org.id    = ip.org_id ",
        ver,
        ver,
        cl->name))
      {
        LOG_ERROR("out of memory");
        goto err;
      }

      if (!rr_buffer_appendf(&qb, "WHERE "))
      {
        LOG_ERROR("out of memory");
        goto err;
      }
      size_t start = qb.pos;

      // reset the seen state
      for(ConfigList *l = g_config.lists; l->name; ++l)
        l->include_seen = l->exclude_seen = false;

      if (!db_build_list_query_where(cl, &qb, n, true))
        goto err;

      // build exclusions separately to re-use for UNION selects
      if (cl->exclude)
      {
        for(ConfigList *l = g_config.lists; l->name; ++l)
          l->include_seen = l->exclude_seen = false;

        bool exclude_started = false;
        for(const char **exclude = cl->exclude; *exclude; ++exclude)
          for(ConfigList *l = g_config.lists; l->name; ++l)
          {
            if (strcmp(l->name, *exclude) == 0)
            {
              if (l->exclude_seen)
                break;
              l->exclude_seen = true;

              if (exclude_started)
              {
                if (!rr_buffer_appendf(&exclude_qb, " OR ")) goto err;
              }

              if (!db_build_list_query_where(l, &exclude_qb, n, false))
                goto err;
              exclude_started = true;
              break;
            }
          }
      }

      // add UNION SELECTs for already prepared lists
      if (cl->include)
      {
        for(const char **include = cl->include; *include; ++include)
        {
          typeof(s_import.lists_prepare) prepared = db_find_prepared_list(*include);
          if (!prepared || !prepared->stmt[n])
            continue;

          if (!rr_buffer_appendf(&union_qb,
            " UNION SELECT "
              "list.id, "
              "nbl.%s, "
              "nbl.start_ip, "
              "nbl.end_ip, "
              "nbl.prefix_len "
            "FROM "
              "netblock_%s_list AS nbl "
              "JOIN netblock_%s AS ip ON ip.id = nbl.%s "
              "LEFT JOIN org AS org ON org.id = ip.org_id "
              "JOIN list AS include_list ON include_list.id = nbl.list_id "
              "JOIN list AS list ON list.name = '%s' "
            "WHERE include_list.name = '%s'",
            netblock_id,
            ver,
            ver,
            netblock_id,
            cl->name,
            *include))
            goto err;

          if (exclude_qb.pos > 0)
          {
            if (!rr_buffer_appendf(&union_qb, " AND NOT (%.*s)", (int)exclude_qb.pos, exclude_qb.buffer))
            {
              LOG_ERROR("out of memory");
              goto err;
            }
          }
        }
      }

      if (start == qb.pos)
      {
        if (union_qb.pos > 0 && !rr_buffer_appendf(&qb, "0"))
        {
          LOG_ERROR("out of memory");
          goto err;
        }
        else if (exclude_qb.pos > 0 && !rr_buffer_appendf(&qb, "1"))
        {
          LOG_ERROR("out of memory");
          goto err;
        }
      }

      if (start == qb.pos && exclude_qb.pos == 0 && union_qb.pos == 0)
      {
        // no conditions or unions, skip preparing this list
        continue;
      }

      if (exclude_qb.pos > 0)
      {
        if (!rr_buffer_appendf(&qb, " AND NOT (%.*s)", (int)exclude_qb.pos, exclude_qb.buffer))
        {
          LOG_ERROR("out of memory");
          goto err;
        }
      }

      if (union_qb.pos == 0 && start != qb.pos)
      {
        if (!rr_buffer_appendf(&qb, " ORDER BY ip.start_ip ASC"))
        {
          LOG_ERROR("out of memory");
          goto err;
        }
      }

      if (union_qb.pos > 0 && !rr_buffer_append_str(&qb, union_qb.buffer))
      {
        LOG_ERROR("out of memory");
        goto err;
      }

      qb.buffer[qb.pos] = '\0';

      strcpy(list->in_list_name, cl->name);
      LOG_INFO("%s", qb.buffer);
      list->stmt[n] = rr_db_stmt_prepare(con, qb.buffer, NULL);

      if (!list->stmt[n])
      {
        LOG_ERROR("failed to prepare %s statement for list %s", ver, cl->name);
        continue;
      }
    }

    ++list;
  }

  #undef CONFIG_LIST_FIELDS
  ret = true;

err:
  rr_buffer_free(&qb);
  rr_buffer_free(&union_qb);
  rr_buffer_free(&exclude_qb);
  free(order.order);
  return ret;
}

static bool db_deinit_fn(RRDBCon *con, void **udata)
{
  STMT_FREE(STATEMENTS, *udata);

  for(typeof(s_import.lists_prepare) list = s_import.lists_prepare; list; ++list)
  {
    for(int n = 0; n < ARRAY_SIZE(list->stmt); ++n)
      rr_db_stmt_free(&list->stmt[n]);
  }
  free(s_import.lists_prepare);
  s_import.lists_prepare = NULL;

  *udata = NULL;
  return true;
}

bool rr_import_init(void)
{
  if (!rr_download_init(&s_import.dl))
  {
    LOG_ERROR("rr_download_init failed");
    return false;
  }

  // reserve a connection for imports only
  if (!rr_db_reserve(&s_import.con, db_init_fn, db_deinit_fn))
  {
    LOG_ERROR("rr_db_reserve failed");
    return false;
  }

  return true;
}

void rr_import_deinit(void)
{
  rr_db_release(&s_import.con);
  rr_download_deinit(&s_import.dl);
}

static bool rr_emit_ipv4_range_as_cidrs(unsigned list_id, uint32_t start, uint32_t end)
{
  uint64_t cur = (uint64_t)start;
  uint64_t last = (uint64_t)end;

  while (cur <= last)
  {
    uint64_t max_size = rr_lowbit_u32((uint32_t)cur);          // alignment-limited
    uint64_t remain   = last - cur + 1ULL;                     // range-limited

    while (max_size > remain)
      max_size >>= 1;

    uint8_t prefix_len = (uint8_t)(32u - (uint8_t)__builtin_ctzll(max_size));
    if (!rr_import_netblockv4_list_union_insert(list_id, cur, prefix_len))
      return false;

    cur += max_size;
  }

  return true;
}

static bool rr_import_netblockv4_list_union_populate(RRDBCon *con, unsigned list_id)
{
  uint32_t start_ip, end_ip;
  uint8_t  prefix_len;
  uint32_t run_start = 0, run_end = 0;
  bool     have_run = false;

  if (!rr_query_netblockv4_list_start(con, list_id, true))
    return false;

  int rc;
  while ((rc = rr_query_netblockv4_list_fetch(con, &start_ip, &end_ip, &prefix_len)) == 1)
  {
    if (!have_run)
    {
      run_start = start_ip;
      run_end   = end_ip;
      have_run  = 1;
      continue;
    }

    if ((uint64_t)start_ip <= (uint64_t)run_end + 1ULL)
    {
      if (end_ip > run_end)
        run_end = end_ip;
      continue;
    }

    if (!rr_emit_ipv4_range_as_cidrs(list_id, run_start, run_end))
    {
      rr_query_netblockv4_list_end(con);
      return false;
    }

    run_start = start_ip;
    run_end   = end_ip;
  }

  if (have_run && !rr_emit_ipv4_range_as_cidrs(list_id, run_start, run_end))
  {
    rr_query_netblockv4_list_end(con);
    return false;
  }

  if (rc < 0)
  {
    rr_query_netblockv4_list_end(con);
    return false;
  }

  rr_query_netblockv4_list_end(con);
  return true;
}

static bool rr_emit_ipv6_range_as_cidrs(unsigned list_id, unsigned __int128 start_raw, unsigned __int128 end_raw)
{
  const unsigned __int128 U128_MAX = (unsigned __int128)-1;

  // Convert once; do ALL math/comparisons in BE-numeric.
  unsigned __int128 start = rr_raw_to_be(start_raw);
  unsigned __int128 end   = rr_raw_to_be(end_raw);

  if (start > end)
    return true;

  // ::/0
  if (start == 0 && end == U128_MAX)
    return rr_import_netblockv6_list_union_insert(list_id, (unsigned __int128)0, (uint8_t)0);

  unsigned __int128 cur  = start;
  unsigned __int128 last = end;

  while (cur <= last)
  {
    unsigned __int128 remain = (last - cur) + 1;   // safe (not the ::/0 case)

    uint8_t tz  = rr_u128_ctz_be(cur);             // alignment limit (as exponent)
    uint8_t msb = rr_u128_msb_be(remain);          // range limit (as exponent)
    uint8_t exp = (tz < msb) ? tz : msb;

    unsigned __int128 block_size = ((unsigned __int128)1 << exp);
    unsigned __int128 block_end  = cur + block_size - 1;

    // Convert back to RAW(network bytes) for your CIDR helper + insert
    unsigned __int128 cur_raw      = rr_be_to_raw(cur);
    unsigned __int128 block_end_raw= rr_be_to_raw(block_end);

    uint8_t prefix_len = rr_ipv6_to_cidr(cur_raw, block_end_raw);

    if (!rr_import_netblockv6_list_union_insert(list_id, cur_raw, prefix_len))
      return false;

    cur += block_size;
  }

  return true;
}

static bool rr_import_netblockv6_list_union_populate(RRDBCon *con, unsigned list_id)
{
  unsigned __int128 start_raw, end_raw;
  uint8_t  prefix_len;

  const unsigned __int128 U128_MAX = (unsigned __int128)-1;

  unsigned __int128 run_start = 0, run_end = 0; // BE-numeric domain
  bool have_run = false;

  if (!rr_query_netblockv6_list_start(con, list_id, true))
    return false;

  int rc;
  while ((rc = rr_query_netblockv6_list_fetch(con, &start_raw, &end_raw, &prefix_len)) == 1)
  {
    (void)prefix_len;

    unsigned __int128 start = rr_raw_to_be(start_raw);
    unsigned __int128 end   = rr_raw_to_be(end_raw);

    if (!have_run)
    {
      run_start = start;
      run_end   = end;
      have_run  = true;
      continue;
    }

    // overlap OR adjacent (guard +1 overflow)
    bool overlaps = (start <= run_end);
    bool adjacent = (run_end != U128_MAX) && (start == run_end + 1);

    if (overlaps || adjacent)
    {
      if (end > run_end) run_end = end;
      continue;
    }

    if (!rr_emit_ipv6_range_as_cidrs(list_id, rr_be_to_raw(run_start), rr_be_to_raw(run_end)))
    {
      rr_query_netblockv6_list_end(con);
      return false;
    }

    run_start = start;
    run_end   = end;
  }

  if (have_run && !rr_emit_ipv6_range_as_cidrs(list_id, rr_be_to_raw(run_start), rr_be_to_raw(run_end)))
  {
    rr_query_netblockv6_list_end(con);
    return false;
  }

  rr_query_netblockv6_list_end(con);
  return (rc >= 0);
}

static bool rr_import_build_lists_internal(RRDBCon *con)
{
  if (!g_config.lists)
    return true;

  LOG_INFO("rebuilding lists");
  for(typeof(s_import.lists_prepare) list = s_import.lists_prepare; list->stmt[0] && list->stmt[1]; ++list)
  {
    LOG_INFO("  Building: %s", list->in_list_name);
    unsigned list_id;
    if (
      !rr_db_start(con) ||
      !rr_import_list_insert(list->in_list_name) ||
      rr_query_list_by_name(con, list->in_list_name, &list_id) != 1 ||
      !rr_import_netblockv4_list_delete(list_id) ||
      !rr_import_netblockv6_list_delete(list_id) ||
      !rr_db_stmt_execute(list->stmt[0], NULL) ||
      !rr_db_stmt_execute(list->stmt[1], NULL) ||
      !rr_import_netblockv4_list_union_delete  (list_id) ||
      !rr_import_netblockv6_list_union_delete  (list_id) ||
      !rr_import_netblockv4_list_union_populate(con, list_id) ||
      !rr_import_netblockv6_list_union_populate(con, list_id) ||
      !rr_db_commit(con))
    {
      rr_db_rollback(con);
      LOG_ERROR("failed");
      return false;
    }
  }

  LOG_INFO("done");
  return true;
}

bool rr_import_build_lists(void)
{
  RRDBCon *con = s_import.con;
  if (!rr_db_get(&con))
  {
    LOG_ERROR("failed to get the reserved connection");
    return false;
  }
  bool result = rr_import_build_lists_internal(con);
  rr_db_put(&con);
  return result;
}

bool rr_import_run(void)
{
  int rc;
  bool rebuild_unions = false;
  bool rebuild_lists  = false;
  while(true)
  {
    RRDBCon *con = s_import.con;
    if (!rr_db_get(&con))
    {
      LOG_ERROR("failed to get the reserved connection");
      goto fail;
    }

    for(unsigned i = 0; i < g_config.nbSources; ++i)
    {
      typeof(*g_config.sources) *src = &g_config.sources[i];
      if (src->type == SOURCE_TYPE_INVALID)
        continue;

      if (!rr_db_start(con))
      {
        rr_db_put(&con);
        goto fail;
      }

      memset(&s_import.stats, 0, sizeof(s_import.stats));
      unsigned registrar_id = 0;
      unsigned serial       = 0;
      unsigned last_import  = 0;

      rc = rr_query_registrar_by_name(con, src->name,
        &registrar_id,
        &serial,
        &last_import);

      if (rc < 0)
        goto fail_con;

      if (rc == 0)
      {
        LOG_INFO("Registrar not found, inserting new record...");
        rc = rr_import_registrar_insert(src->name, &registrar_id);
        if (rc < 0)
          goto fail_con;

        if (rc == 0)
        {
          LOG_ERROR("Failed to insert a new registrar");
          if (!rr_db_rollback(con))
            goto fail_con;
          continue;
        }
        LOG_INFO("New registrar inserted");
      }

      if (last_import > 0 && time(NULL) - last_import < src->frequency)
      {
        if (!rr_db_rollback(con))
          goto fail_con;
        continue;
      }

      LOG_INFO("Fetching source: %s", src->name);
      if (src->user && src->pass)
        rr_download_set_auth(s_import.dl, src->user, src->pass);
      else
        rr_download_clear_auth(s_import.dl);

      FILE *fp;
      if (!rr_download_to_tmpfile(s_import.dl, src->url, &fp))
      {
        LOG_ERROR("failed fetch for %s", src->name);
        if (!rr_db_rollback(con))
          goto fail_con;
        continue;
      }

      LOG_INFO("start import %s", src->name);
      uint64_t startTime = rr_microtime();

      ++serial;
      bool success = false;
      switch(src->type)
      {
        case SOURCE_TYPE_RPSL:
          success = rr_rpsl_import_gz_FILE(src->name, fp, registrar_id, serial);
          break;

        case SOURCE_TYPE_ARIN:
          success = rr_arin_import_zip_FILE(src->name, fp, registrar_id, serial);
          break;

        default:
          assert(false);
      }
      fclose(fp);

      const char *resultStr;
      if (success)
      {
        //finalize the registrar
        LOG_INFO("finalizing");
        if (
          !rr_import_org_delete_old         (registrar_id, serial) ||
          !rr_import_netblockv4_delete_old  (registrar_id, serial) ||
          !rr_import_netblockv6_delete_old  (registrar_id, serial) ||
          !rr_import_netblockv4_link_org    () ||
          !rr_import_netblockv6_link_org    () ||
          !rr_import_registrar_update_serial(registrar_id, serial) ||
          !rr_db_commit                     (con))
        {
          LOG_ERROR("failed to finalize");
          if (!rr_db_rollback(con))
            goto fail_con;
          continue;
        }

        resultStr = "succeeded";
        rebuild_unions = true;
        rebuild_lists  = true;
      }
      else
      {
        if (!rr_db_rollback(con))
          goto fail_con;
        resultStr = "failed";
      }

      uint64_t elapsed = rr_microtime() - startTime;
      uint64_t sec     = elapsed / 1000000UL;
      uint64_t us      = elapsed % 1000000UL;
      LOG_INFO("import of %s %s in %02u:%02u:%02u.%03u",
        src->name,
        resultStr,
        (unsigned)(sec / 60 / 60),
        (unsigned)(sec / 60 % 60),
        (unsigned)(sec % 60),
        (unsigned)(us / 1000));

      LOG_INFO("Import Statistics (%s)", src->name);
      LOG_INFO("Orgs:");
      LOG_INFO("  New    : %llu", s_import.stats.newOrgs    );
      LOG_INFO("  Deleted: %llu", s_import.stats.deletedOrgs);
      LOG_INFO("IPv4:");
      LOG_INFO("  New    : %llu", s_import.stats.newIPv4    );
      LOG_INFO("  Deleted: %llu", s_import.stats.deletedIPv4);
      LOG_INFO("IPv6:");
      LOG_INFO("  New    : %llu", s_import.stats.newIPv6    );
      LOG_INFO("  Deleted: %llu", s_import.stats.deletedIPv6);
    }

    if (rebuild_unions)
    {
      LOG_INFO("rebuilding unions");
      if (
        !rr_db_start(con) ||
        !rr_import_netblockv4_union_truncate() ||
        !rr_import_netblockv6_union_truncate() ||
        !rr_import_netblockv4_union_populate() ||
        !rr_import_netblockv6_union_populate() ||
        !rr_db_commit(con))
      {
        LOG_ERROR("failed");
        rr_db_rollback(con);
        goto fail_con;
      }
      LOG_INFO("done");
      rebuild_unions = false;
    }

    if (rebuild_lists && rr_import_build_lists_internal(con))
      rebuild_lists = false;

    fail_con:
    rr_db_put(&con);
    fail:
    usleep(1000000);
  }

  return true;
}
