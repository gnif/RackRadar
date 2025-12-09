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

  STMT_STRUCT(netblockv4_list_delete,
    char in_list_name[32];
  );
  STMT_STRUCT(netblockv6_list_delete,
    char in_list_name[32];
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
  X(registrar_insert         ) \
  X(registrar_update_serial  ) \
  X(org_insert               ) \
  X(org_delete_old           ) \
  X(netblockv4_insert        ) \
  X(netblockv4_delete_old    ) \
  X(netblockv4_link_org      ) \
  X(netblockv6_insert        ) \
  X(netblockv6_delete_old    ) \
  X(netblockv6_link_org      ) \
  X(netblockv4_union_truncate) \
  X(netblockv6_union_truncate) \
  X(netblockv4_union_populate) \
  X(netblockv6_union_populate) \
  X(list_insert              ) \
  X(netblockv4_list_delete   ) \
  X(netblockv6_list_delete   )

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

  &(RRDBParam){ .type = RRDB_TYPE_UINT  , .bind = &this->in.registrar_id },
  &(RRDBParam){ .type = RRDB_TYPE_UINT,   .bind = &this->in.serial       },
  &(RRDBParam){ .type = RRDB_TYPE_STRING, .bind = &this->in.name         },
  &(RRDBParam){ .type = RRDB_TYPE_STRING, .bind = &this->in.org_name     },
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

  &(RRDBParam){ .type = RRDB_TYPE_UINT  , .bind = &this->in.registrar_id },
  &(RRDBParam){ .type = RRDB_TYPE_UINT  , .bind = &this->in.serial       },
  &(RRDBParam){ .type = RRDB_TYPE_STRING, .bind = &this->in.org_id_str   },
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
    "AND o.name = nb.org_id_str "
    "SET nb.org_id = o.id"
);

DEFAULT_STMT(RRImport, netblockv6_insert,
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

  &(RRDBParam){ .type = RRDB_TYPE_UINT  , .bind = &this->in.registrar_id },
  &(RRDBParam){ .type = RRDB_TYPE_UINT  , .bind = &this->in.serial       },
  &(RRDBParam){ .type = RRDB_TYPE_STRING, .bind = &this->in.org_id_str   },
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
    "AND o.name = nb.org_id_str "
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
  "DELETE FROM netblock_v4_list WHERE list_id IN (SELECT id FROM list WHERE name = ?)",
  &(RRDBParam){ .type = RRDB_TYPE_STRING, .bind = &this->in_list_name }
);

DEFAULT_STMT(RRImport, netblockv6_list_delete,
  "DELETE FROM netblock_v6_list WHERE list_id IN (SELECT id FROM list WHERE name = ?)",
  &(RRDBParam){ .type = RRDB_TYPE_STRING, .bind = &this->in_list_name }
);
#pragma endregion

#pragma region statement_interfaces

static int rr_import_registrar_insert(const char *in_name, unsigned *out_registrar_id)
{
  strcpy(s_import.registrar_insert.in_name, in_name);
  int rc = rr_db_stmt_execute(s_import.registrar_insert.stmt, NULL);
  if (rc < 1)
    return rc;

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

static bool rr_import_netblockv4_list_delete(const char *in_list_name)
{
  strcpy(s_import.netblockv4_list_delete.in_list_name, in_list_name);
  return rr_db_stmt_execute(s_import.netblockv4_list_delete.stmt, NULL);
}

static bool rr_import_netblockv6_list_delete(const char *in_list_name)
{
  strcpy(s_import.netblockv6_list_delete.in_list_name, in_list_name);
  return rr_db_stmt_execute(s_import.netblockv6_list_delete.stmt, NULL);
}
#pragma endregion

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

  typeof(s_import.lists_prepare) list = s_import.lists_prepare;
  for(ConfigList *cl = g_config.lists; cl->name; ++cl)
  {
    #define ADD_CONDITION(x, y, z) \
      if (cl->x ##_ ##y.z) \
        for(const char **str = cl->x ##_ ##y.z; *str; ++str, ++conditions) \
          pos += snprintf(query + pos, sizeof(query) - pos, \
            "%s" #x "." #y " LIKE '%s'", \
            conditions > 0 ? " OR " : "", \
            *str \
          ); \

    if (!cl->has_matches)
      continue;

    char query[8192];
    for(int n = 0; n < 2; ++n)
    {
      off_t pos = 0;
      const char *ver = n == 0 ? "v4" : "v6";
      pos += snprintf(query + pos, sizeof(query) - pos,
        "INSERT INTO netblock_%s_list "
        "SELECT "
          "list.id, "
          "ip.id, "
          "ip.start_ip, "
          "ip.prefix_len "
        "FROM "
          "netblock_%s     AS ip "
          "RIGHT JOIN list AS list ON list.name = ? "
          "LEFT  JOIN org  AS org  ON org.id    = ip.org_id "
        "WHERE (",
        ver,
        ver);

      int conditions = 0;
      #define X(x, y) ADD_CONDITION(x, y, match)
      CONFIG_LIST_FIELDS
      #undef X
      pos += snprintf(query + pos, sizeof(query) - pos, ")");

      if (cl->has_ignores)
      {
        pos += snprintf(query + pos, sizeof(query) - pos, " AND NOT (");
        conditions = 0;
        #define X(x, y) ADD_CONDITION(x, y, ignore)
        CONFIG_LIST_FIELDS
        #undef X
        pos += snprintf(query + pos, sizeof(query) - pos, ")");
      }

      strcpy(list->in_list_name, cl->name);
      list->stmt[n] = rr_db_stmt_prepare(con, query,
        &(RRDBParam){ .type = RRDB_TYPE_STRING, .bind = &list->in_list_name },
        NULL
      );

      if (!list->stmt[n])
      {
        LOG_ERROR("failed to prepare %s statement for list %s", ver, cl->name);
        continue;
      }
    }

    ++list;
  }

  #undef ADD_CONDITION
  #undef CONFIG_LIST_FIELDS
  return true;
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

static bool rr_import_build_lists_internal(RRDBCon *con)
{
  if (!g_config.lists)
    return true;

  LOG_INFO("rebuilding lists");
  for(typeof(s_import.lists_prepare) list = s_import.lists_prepare; list->stmt[0] && list->stmt[1]; ++list)
  {
    LOG_INFO("  Building: %s", list->in_list_name);
    if (
      !rr_db_start(con) ||
      !rr_import_list_insert(list->in_list_name) ||
      !rr_import_netblockv4_list_delete(list->in_list_name) ||
      !rr_import_netblockv6_list_delete(list->in_list_name) ||
      !rr_db_stmt_execute(list->stmt[0], NULL) ||
      !rr_db_stmt_execute(list->stmt[1], NULL) ||
      !rr_db_commit(con))
    {
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
