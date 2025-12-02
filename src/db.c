#include "db.h"
#include "config.h"
#include "log.h"

#include <mysql.h>
#include <string.h>

static struct
{
  bool  initialized;
  MYSQL con;
}
db = { 0 };

struct RRDBCon
{
  MYSQL *con;
};

struct RRDBStmt
{
  MYSQL_STMT    *stmt;
  MYSQL_BIND    *bind;
  unsigned long *lengths;
  my_bool       *is_null;
  size_t         nparams;

  MYSQL_BIND    *rbind;
  unsigned long *rlengths;
  my_bool       *ris_null;
  size_t         nresults;
};

typedef struct RRDBParam
{
  enum enum_field_types type;
  void    *bind;
  my_bool *is_null;

  bool is_unsigned;
  bool is_binary;
  size_t size;
}
RRDBParam;

bool rr_db_init(void)
{
  if (!mysql_init(&db.con))
  {
    LOG_ERROR("mysql_init failed");
    return false;
  }

  mysql_options(&db.con, MYSQL_SET_CHARSET_NAME, "utf8mb4");

  if (!mysql_real_connect(
    &db.con,
    g_config.database.port != 0 ? g_config.database.host : NULL,
    g_config.database.user,
    g_config.database.pass,
    g_config.database.name,
    g_config.database.port,
    g_config.database.port == 0 ? g_config.database.host : NULL,
    0)
  )
  {
    LOG_ERROR("%s", mysql_error(&db.con));
    return false;
  };

  mysql_query(&db.con, "SET collation_connection = 'utf8mb4_unicode_ci'");
  mysql_autocommit(&db.con, 0);

  db.initialized = true;
  return true;
}

void rr_db_deinit(void)
{
  if (!db.initialized)
    return;

  mysql_close(&db.con);
  db.initialized = false;
}

bool rr_db_getCon(RRDBCon **con)
{
  //TODO: use a pool of connections
  static RRDBCon t;
  t.con = &db.con;
  *con = &t;
  return true;
}

void rr_db_putCon(RRDBCon **con)
{
}

void rr_db_start(RRDBCon *con)
{
  static const char * q = "START TRANSACTION";
  mysql_real_query(con->con, q, sizeof(q)-1);
}

void rr_db_commit(RRDBCon *con)
{
  static const char * q = "COMMIT";
  mysql_real_query(con->con, q, sizeof(q)-1);
}

void rr_db_rollback(RRDBCon *con)
{
  static const char * q = "ROLLBACK";
  mysql_real_query(con->con, q, sizeof(q)-1);
}

static bool rr_db_is_stringish(enum enum_field_types t)
{
  switch (t)
  {
    case MYSQL_TYPE_STRING:
    case MYSQL_TYPE_VAR_STRING:
    case MYSQL_TYPE_VARCHAR:
    case MYSQL_TYPE_TINY_BLOB:
    case MYSQL_TYPE_BLOB:
    case MYSQL_TYPE_MEDIUM_BLOB:
    case MYSQL_TYPE_LONG_BLOB:
    case MYSQL_TYPE_JSON:
      return true;
    default:
      return false;
  }
}

static RRDBStmt *rr_db_query_stmt(MYSQL *con, const char *sql, ...)
{
  MYSQL_STMT *stmt = mysql_stmt_init(con);
  if (!stmt)
    return NULL;

  if (mysql_stmt_prepare(stmt, sql, (unsigned long)strlen(sql)) != 0)
  {
    LOG_ERROR("prepare stmt failed: %s", mysql_error(con));
    LOG_ERROR("%s", sql);
    mysql_stmt_close(stmt);
    return NULL;
  }

  va_list ap;
  va_start(ap, sql);

  size_t nparams = 0;
  for (;;)
  {
    RRDBParam *param = va_arg(ap, RRDBParam *);
    if (param == NULL)
      break;
    nparams++;
  }

  va_end(ap);

  RRDBStmt *rs = (RRDBStmt *)calloc(1, sizeof(*rs));
  if (!rs)
  {
    LOG_ERROR("out of memory");
    mysql_stmt_close(stmt);
    return NULL;
  }

  rs->stmt = stmt;
  rs->nparams = nparams;

  if (nparams == 0)
    return rs;

  rs->bind    = (MYSQL_BIND *   )calloc(nparams, sizeof(*rs->bind   ));
  rs->lengths = (unsigned long *)calloc(nparams, sizeof(*rs->lengths));
  rs->is_null = (my_bool *      )calloc(nparams, sizeof(*rs->is_null));

  if (!rs->bind || !rs->lengths || !rs->is_null)
  {
    LOG_ERROR("out of memory");
    mysql_stmt_close(stmt);
    free(rs->bind);
    free(rs->lengths);
    free(rs->is_null);
    free(rs);
    return NULL;
  }

  va_start(ap, sql);
  for (size_t i = 0; i < nparams; i++)
  {
    RRDBParam *param = va_arg(ap, RRDBParam *);

    rs->bind[i].buffer_type = param->type;
    rs->bind[i].buffer      = param->bind;

    rs->is_null[i] = param->bind == NULL;
    rs->bind[i].is_null = param->is_null;

    if (param->bind == NULL)
    {
      rs->bind[i].buffer_length = 0;
      rs->bind[i].length = NULL;
      continue;
    }

    if (rr_db_is_stringish(rs->bind[i].buffer_type))
    {
      rs->lengths[i] = 0;
      rs->bind[i].buffer_length = rs->lengths[i];
      rs->bind[i].length = &rs->lengths[i];

      if (param->is_binary)
      {
        rs->bind[i].flags |= BINARY_FLAG;
        rs->lengths[i]     = param->size;
      }
    }
    else
    {
      rs->bind[i].buffer_length = 0;
      rs->bind[i].length = NULL;
      rs->bind[i].is_unsigned = param->is_unsigned;
    }
  }

  // null terminator
  (void)va_arg(ap, void *);
  va_end(ap);

  if (mysql_stmt_bind_param(stmt, rs->bind) != 0)
  {
    LOG_ERROR("bind failed: %s", mysql_error(con));
    mysql_stmt_close(stmt);
    free(rs->bind); free(rs->lengths); free(rs->is_null);
    free(rs);
    return NULL;
  }

  return rs;
}

bool rr_db_execute_stmt(RRDBStmt *stmt)
{
  for(int i = 0; i < stmt->nparams; ++i)
    if (rr_db_is_stringish(stmt->bind[i].buffer_type))
    {
      const char * str = stmt->bind[i].buffer;
      if (!(stmt->bind[i].flags & BINARY_FLAG))
        stmt->lengths[i] = (unsigned long)strlen(str);
    }

  if (mysql_stmt_execute(stmt->stmt) != 0)
  {
    LOG_ERROR("execute failed: %s", mysql_stmt_error(stmt->stmt));
    return false;
  }
  return true;
}


bool rr_db_stmt_bind_results(RRDBStmt *s, ...)
{
  // Free any existing result bindings
  free(s->rbind);
  free(s->rlengths);
  free(s->ris_null);

  s->rbind    = NULL;
  s->rlengths = NULL;
  s->ris_null = NULL;
  s->nresults = 0;

  // Count columns
  va_list ap;
  va_start(ap, s);

  size_t n = 0;
  for (;;)
  {
    int t = va_arg(ap, int);
    if (t == MYSQL_TYPE_NULL) break;
    (void)va_arg(ap, void *);
    (void)va_arg(ap, unsigned long);
    n++;
  }
  va_end(ap);

  s->nresults = n;
  if (n == 0)
    return true;

  s->rbind    = (MYSQL_BIND *   )calloc(n, sizeof(*s->rbind   ));
  s->rlengths = (unsigned long *)calloc(n, sizeof(*s->rlengths));
  s->ris_null = (my_bool *      )calloc(n, sizeof(*s->ris_null));
  if (!s->rbind || !s->rlengths || !s->ris_null)
  {
    LOG_ERROR("out of memory");
    return false;
  }

  va_start(ap, s);
  for (size_t i = 0; i < n; i++)
  {
    int t = va_arg(ap, int);
    void *out = va_arg(ap, void *);
    unsigned long cap = va_arg(ap, unsigned long);

    s->rbind[i].buffer_type = (enum enum_field_types)t;
    s->rbind[i].buffer      = out;

    s->ris_null[i] = (out == NULL);
    s->rbind[i].is_null = &s->ris_null[i];

    // always track returned length (esp. for strings/blobs)
    s->rbind[i].length = &s->rlengths[i];

    if (out == NULL)
    {
      s->rbind[i].buffer_length = 0;
      continue;
    }

    if (rr_db_is_stringish(s->rbind[i].buffer_type))
      s->rbind[i].buffer_length = cap;
    else
      s->rbind[i].buffer_length = cap;
  }
  (void)va_arg(ap, int); // terminator
  va_end(ap);

  return (mysql_stmt_bind_result(s->stmt, s->rbind) == 0);
}

void rr_db_stmt_free(RRDBStmt **rs)
{
  if (!rs || !*rs)
    return;

  if ((*rs)->stmt)
  {
    mysql_stmt_free_result((*rs)->stmt);
    mysql_stmt_close((*rs)->stmt);
  }

  free((*rs)->bind);
  free((*rs)->lengths);
  free((*rs)->is_null);

  free((*rs)->rbind);
  free((*rs)->rlengths);
  free((*rs)->ris_null);

  free(*rs);
  *rs = NULL;
}

unsigned rr_db_get_registrar_id(RRDBCon *con, const char *name, bool create, unsigned *serial)
{
  RRDBStmt *st = NULL;
  unsigned ret = 0;

  if (create)
  {
    if (!(st = rr_db_query_stmt(con->con,
      "INSERT IGNORE INTO registrar (name) VALUES (?)",
      &(RRDBParam){ .type = MYSQL_TYPE_STRING, .bind = (char *)name  },
      NULL)))
    {
      LOG_ERROR("prepare failed");
      goto err;
    }

    if (!rr_db_execute_stmt(st))
      goto err;
  }

  if (!(st = rr_db_query_stmt(con->con,
    "SELECT id, serial FROM registrar WHERE name = ?",
    &(RRDBParam){ .type = MYSQL_TYPE_STRING, .bind = (char *)name  },
    NULL)))
  {
    LOG_ERROR("prepare failed");
    goto err;
  }

  if (!rr_db_stmt_bind_results(st,
    MYSQL_TYPE_LONG, &ret  , 0,
    MYSQL_TYPE_LONG, serial, 0,
    MYSQL_TYPE_NULL))
  {
    LOG_ERROR("bind results failed");
    goto err;
  }

  if (!rr_db_execute_stmt(st))
    goto err;

  if (mysql_stmt_store_result(st->stmt) != 0)
  {
    LOG_ERROR("store result failed");
    goto err;
  }

  if (mysql_stmt_fetch(st->stmt) == MYSQL_NO_DATA)
  {
    LOG_WARN("record not found");
    goto err;
  }

err:
  rr_db_stmt_free(&st);
  return ret;
}

bool rr_db_prepare_org_insert(RRDBCon *con, RRDBStmt **stmt, RRDBOrg *bind)
{
  RRDBStmt *st = rr_db_query_stmt(
    con->con,
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
      "serial = VALUES(serial), "
      "org_name = VALUES(org_name), "
      "descr = VALUES(descr)",

    &(RRDBParam){ .type = MYSQL_TYPE_LONG  , .bind = &bind->registrar_id, .is_unsigned = true },
    &(RRDBParam){ .type = MYSQL_TYPE_LONG,   .bind = &bind->serial      , .is_unsigned = true },
    &(RRDBParam){ .type = MYSQL_TYPE_STRING, .bind = &bind->name     },
    &(RRDBParam){ .type = MYSQL_TYPE_STRING, .bind = &bind->org_name },
    &(RRDBParam){ .type = MYSQL_TYPE_STRING, .bind = &bind->descr    },
    NULL);

  if (!st)
    return false;

  *stmt = st;
  return true;
}

bool rr_db_prepare_netblockv4_insert(RRDBCon *con, RRDBStmt **stmt, RRDBNetBlockV4 *bind)
{
  RRDBStmt *st = rr_db_query_stmt(
    con->con,
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
      "serial = VALUES(serial), "
      "org_id_str = VALUES(org_id_str), "
      "netname = VALUES(netname), "
      "descr = VALUES(descr)",

    &(RRDBParam){ .type = MYSQL_TYPE_LONG,   .bind = &bind->registrar_id, .is_unsigned = true },
    &(RRDBParam){ .type = MYSQL_TYPE_LONG,   .bind = &bind->serial      , .is_unsigned = true },
    &(RRDBParam){ .type = MYSQL_TYPE_STRING, .bind = &bind->org_id_str  ,
      .is_null = &bind->org_id_str_null },
    &(RRDBParam){ .type = MYSQL_TYPE_LONG  , .bind = &bind->startAddr   , .is_unsigned = true },
    &(RRDBParam){ .type = MYSQL_TYPE_LONG  , .bind = &bind->endAddr     , .is_unsigned = true },
    &(RRDBParam){ .type = MYSQL_TYPE_TINY  , .bind = &bind->prefixLen   , .is_unsigned = true },
    &(RRDBParam){ .type = MYSQL_TYPE_STRING, .bind = &bind->netname },
    &(RRDBParam){ .type = MYSQL_TYPE_STRING, .bind = &bind->descr   },
    NULL
  );

  if (!st)
    return false;

  *stmt = st;
  return true;
}

bool rr_db_prepare_netblockv6_insert(RRDBCon *con, RRDBStmt **stmt, RRDBNetBlockV6 *bind)
{
  RRDBStmt *st = rr_db_query_stmt(
    con->con,
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
      "serial = VALUES(serial), "
      "org_id_str = VALUES(org_id_str), "
      "netname = VALUES(netname), "
      "descr = VALUES(descr)",

    &(RRDBParam){ .type = MYSQL_TYPE_LONG,   .bind = &bind->registrar_id, .is_unsigned = true },
    &(RRDBParam){ .type = MYSQL_TYPE_LONG,   .bind = &bind->serial      , .is_unsigned = true },
    &(RRDBParam){ .type = MYSQL_TYPE_STRING, .bind = &bind->org_id_str  ,
      .is_null = &bind->org_id_str_null },
    &(RRDBParam){ .type = MYSQL_TYPE_STRING, .bind = &bind->startAddr   , .is_binary   = true, .size = sizeof(bind->startAddr) },
    &(RRDBParam){ .type = MYSQL_TYPE_STRING, .bind = &bind->endAddr     , .is_binary   = true, .size = sizeof(bind->endAddr  ) },
    &(RRDBParam){ .type = MYSQL_TYPE_TINY  , .bind = &bind->prefixLen   , .is_unsigned = true },
    &(RRDBParam){ .type = MYSQL_TYPE_STRING, .bind = &bind->netname },
    &(RRDBParam){ .type = MYSQL_TYPE_STRING, .bind = &bind->descr   },
    NULL
  );

  if (!st)
    return false;

  *stmt = st;
  return true;
}

bool rr_db_finalize_registrar(RRDBCon *con, unsigned registrar_id, unsigned serial)
{
  RRDBStmt *st;

  // update the registrar serial
  st = rr_db_query_stmt(con->con,
    "UPDATE registrar SET serial = ? WHERE id = ?",
    &(RRDBParam){ .type = MYSQL_TYPE_LONG, .bind = &serial      , .is_unsigned = true },
    &(RRDBParam){ .type = MYSQL_TYPE_LONG, .bind = &registrar_id, .is_unsigned = true },
    NULL);

  if (!st || !rr_db_execute_stmt(st))
  {
    rr_db_stmt_free(&st);
    return false;
  }
  rr_db_stmt_free(&st);

  // delete all old records
  st = rr_db_query_stmt(con->con,
    "DELETE FROM netblock_v4 WHERE registrar_id = ? AND serial != ?",
    &(RRDBParam){ .type = MYSQL_TYPE_LONG, .bind = &registrar_id, .is_unsigned = true },
    &(RRDBParam){ .type = MYSQL_TYPE_LONG, .bind = &serial      , .is_unsigned = true },
    NULL);

  if (!st || !rr_db_execute_stmt(st))
  {
    rr_db_stmt_free(&st);
    return false;
  }
  rr_db_stmt_free(&st);

  st = rr_db_query_stmt(con->con,
    "DELETE FROM netblock_v6 WHERE registrar_id = ? AND serial != ?",
    &(RRDBParam){ .type = MYSQL_TYPE_LONG, .bind = &registrar_id, .is_unsigned = true },
    &(RRDBParam){ .type = MYSQL_TYPE_LONG, .bind = &serial      , .is_unsigned = true },
    NULL);

  if (!st || !rr_db_execute_stmt(st))
  {
    rr_db_stmt_free(&st);
    return false;
  }
  rr_db_stmt_free(&st);

  st = rr_db_query_stmt(con->con,
    "DELETE FROM org WHERE registrar_id = ? AND serial != ?",
    &(RRDBParam){ .type = MYSQL_TYPE_LONG, .bind = &registrar_id, .is_unsigned = true },
    &(RRDBParam){ .type = MYSQL_TYPE_LONG, .bind = &serial      , .is_unsigned = true },
    NULL);

  if (!st || !rr_db_execute_stmt(st))
  {
    rr_db_stmt_free(&st);
    return false;
  }
  rr_db_stmt_free(&st);

  // link orgs to netblocks
  st = rr_db_query_stmt(con->con,
    "UPDATE netblock_v4 nb "
    "LEFT JOIN org o "
    "ON o.registrar_id = nb.registrar_id "
    "AND o.name = nb.org_id_str "
    "SET nb.org_id = o.id",
    NULL);

  if (!st || !rr_db_execute_stmt(st))
  {
    rr_db_stmt_free(&st);
    return false;
  }
  rr_db_stmt_free(&st);

  st = rr_db_query_stmt(con->con,
    "UPDATE netblock_v6 nb "
    "LEFT JOIN org o "
    "ON o.registrar_id = nb.registrar_id "
    "AND o.name = nb.org_id_str "
    "SET nb.org_id = o.id",
    NULL);

  if (!st || !rr_db_execute_stmt(st))
  {
    rr_db_stmt_free(&st);
    return false;
  }
  rr_db_stmt_free(&st);

  return true;
};