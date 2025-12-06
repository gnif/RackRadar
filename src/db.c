#include "db.h"
#include "config.h"
#include "log.h"
#include "util.h"

#include <mysql.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <assert.h>

struct RRDBCon
{
  unsigned id;
  bool     in_use;
  MYSQL    con;
  void    *udata;
};

static struct
{
  bool  initialized;

  DBUdataFn  udataInitFn;
  DBUdataFn  udataDeInitFn;
  void      *udata;

  RRDBCon         *pool;
  size_t           sz_pool;
  pthread_mutex_t  pool_lock;

  bool      running;
  pthread_t thread;
}
db = { 0 };

struct RRDBStmt
{
  RRDBCon       *con;
  MYSQL_STMT    *stmt;

  size_t         in_params;
  RRDBType      *types;  
  MYSQL_BIND    *bind;
  unsigned long *lengths;
  my_bool       *is_null;
  
  size_t         out_params;
  RRDBType      *rtypes;
  MYSQL_BIND    *rbind;
  unsigned long *rlengths;
  my_bool       *ris_null;
  size_t         nresults;
};

#define RRDB_PARAM_OUT ((void *)((uintptr_t)-1))

static void * rr_db_thread(void *opaque)
{
  LOG_INFO("db thread started");
  unsigned ticks = 0;
  while(db.running)
  {
    ++ticks;
    if (ticks % 10 == 0)
    {
      pthread_mutex_lock(&db.pool_lock);
      for(size_t i = 0; i < db.sz_pool; ++i)
      {
        RRDBCon *con = db.pool + i;
        if (con->in_use)
          continue;

        mysql_ping(&con->con);
      }
      pthread_mutex_unlock(&db.pool_lock);
    }

    usleep(1000000);
  }
  LOG_INFO("db thread finished");
  return NULL;
}

bool rr_db_init(DBUdataFn udataInitFn, DBUdataFn udataDeInitFn)
{
  if (db.initialized)
    rr_db_deinit();

  db.udataInitFn   = udataInitFn;
  db.udataDeInitFn = udataDeInitFn;

  LOG_INFO("Setitng up %d connections", g_config.database.pool);

  db.pool = calloc(g_config.database.pool, sizeof(RRDBCon));
  if (!db.pool)
  {
    LOG_ERROR("out of memory");
    return false;
  }

  for(size_t i = 0; i < g_config.database.pool; ++i)
  {
    RRDBCon *con = db.pool + i;
    con->id = i;

    if (!mysql_init(&con->con))
    {
      LOG_ERROR("mysql_init failed for connection %lu", i);
      rr_db_deinit();
      return false;
    }

    my_bool reconnect = true;
    mysql_options(&con->con, MYSQL_SET_CHARSET_NAME, "utf8mb4" );    
    mysql_options(&con->con, MYSQL_OPT_RECONNECT   , &reconnect);
    
    if (!mysql_real_connect(
      &con->con,
      g_config.database.port != 0 ? g_config.database.host : NULL,
      g_config.database.user,
      g_config.database.pass,
      g_config.database.name,
      g_config.database.port,
      g_config.database.port == 0 ? g_config.database.host : NULL,
      0)
    )
    {
      LOG_ERROR("%s", mysql_error(&con->con));
      rr_db_deinit();
      return false;
    };
    ++db.sz_pool;

    mysql_query     (&con->con, "SET collation_connection = 'utf8mb4_unicode_ci'");
    mysql_autocommit(&con->con, 1);

    if (db.udataInitFn && !db.udataInitFn(con, &con->udata))
    {
      LOG_ERROR("udataInitFn returned false");
      rr_db_deinit();
      return false;
    }
  }

  db.running = true;
  pthread_mutex_init(&db.pool_lock, NULL);
  if (pthread_create(&db.thread, NULL, rr_db_thread, NULL) != 0)
  { 
    LOG_ERROR("Failed to create the database thread");
    rr_db_deinit();
    return false;
  }

  db.initialized = true;
  LOG_INFO("database pool ready");
  return true;
}

void rr_db_deinit(void)
{
  db.running = false;
  pthread_join(db.thread, NULL);

  for(size_t i = 0; i < db.sz_pool; ++i)
  {
    RRDBCon *con = db.pool + i;

    if (db.udataDeInitFn && !db.udataDeInitFn(con, &con->udata))
      LOG_ERROR("udataDeInitFn returned false");

    mysql_close(&con->con);    
  }
  free(db.pool);
  memset(&db, 0, sizeof(db));  
}

bool rr_db_get(RRDBCon **out, void **udata)
{
  if (!db.initialized)
    return false;

  pthread_mutex_lock(&db.pool_lock);
  for(size_t i = 0; i < db.sz_pool; ++i)
  {
    RRDBCon *con = db.pool + i;
    if (!con->in_use)
    {
      con->in_use = true;
      pthread_mutex_unlock(&db.pool_lock);

      *out = con;
      if (udata)
        *udata = con->udata;
      return true;
    }
  }
  pthread_mutex_unlock(&db.pool_lock);
  return false;
}

void rr_db_put(RRDBCon **con)
{
  pthread_mutex_lock(&db.pool_lock);
  (*con)->in_use = false;
  pthread_mutex_unlock(&db.pool_lock);
  *con = NULL;
}

void rr_db_start(RRDBCon *con)
{
  if (mysql_query(&con->con, "START TRANSACTION") != 0)
    LOG_ERROR("failed to start the transaction: %s", mysql_error(&con->con));
}

void rr_db_commit(RRDBCon *con)
{
  if (mysql_query(&con->con, "COMMIT") != 0)
    LOG_ERROR("failed to commit the transaction: %s", mysql_error(&con->con));  
}

void rr_db_rollback(RRDBCon *con)
{
  if (mysql_query(&con->con, "ROLLBACK") != 0)
    LOG_ERROR("failed to rollback the transaction: %s", mysql_error(&con->con));    
}

static inline enum enum_field_types rr_db_type_to_mysql_type(RRDBType type)
{
  switch(type)
  {
    case RRDB_TYPE_INT8   : return MYSQL_TYPE_TINY    ;
    case RRDB_TYPE_UINT8  : return MYSQL_TYPE_TINY    ;
    case RRDB_TYPE_INT    : return MYSQL_TYPE_LONG    ;
    case RRDB_TYPE_UINT   : return MYSQL_TYPE_LONG    ;
    case RRDB_TYPE_BIGINT : return MYSQL_TYPE_LONGLONG;
    case RRDB_TYPE_UBIGINT: return MYSQL_TYPE_LONGLONG;
    case RRDB_TYPE_FLOAT  : return MYSQL_TYPE_FLOAT   ;
    case RRDB_TYPE_DOUBLE : return MYSQL_TYPE_DOUBLE  ;
    case RRDB_TYPE_STRING : return MYSQL_TYPE_STRING  ;
    case RRDB_TYPE_BINARY : return MYSQL_TYPE_BLOB    ;
  }
  assert(false);
};

static inline bool rr_db_type_is_unsigned(RRDBType type)
{
  switch(type)
  {
    case RRDB_TYPE_UINT8:
    case RRDB_TYPE_UINT:
    case RRDB_TYPE_UBIGINT:
      return true;

    default:
      return false;
  }
}

static inline bool rr_db_type_is_stringish(RRDBType type)
{
  switch (type)
  {
    case RRDB_TYPE_STRING:
    case RRDB_TYPE_BINARY:
      return true;

    default:
      return false;
  }
}

static inline unsigned long rr_db_type_length(RRDBType type)
{
  switch(type)
  {
    case RRDB_TYPE_INT8   : return sizeof(int8_t            );
    case RRDB_TYPE_UINT8  : return sizeof(uint8_t           );
    case RRDB_TYPE_INT    : return sizeof(int               );
    case RRDB_TYPE_UINT   : return sizeof(unsigned          );
    case RRDB_TYPE_BIGINT : return sizeof(long long         );
    case RRDB_TYPE_UBIGINT: return sizeof(unsigned long long);
    case RRDB_TYPE_FLOAT  : return sizeof(float             );
    case RRDB_TYPE_DOUBLE : return sizeof(double            );
    case RRDB_TYPE_STRING : return 0;
    case RRDB_TYPE_BINARY : return 0;
  }
  assert(false);
}

static RRDBStmt *rr_db_query_stmt(RRDBCon *con, const char *sql, ...)
{
  MYSQL_STMT *stmt = mysql_stmt_init(&con->con);
  if (!stmt)
    return NULL;

  if (mysql_stmt_prepare(stmt, sql, (unsigned long)strlen(sql)) != 0)
  {
    LOG_ERROR("prepare stmt failed: %s", mysql_error(&con->con));
    LOG_ERROR("%s", sql);
    mysql_stmt_close(stmt);
    return NULL;
  }

  va_list ap;
  va_start(ap, sql);
  size_t in_params  = 0;
  size_t out_params = 0;
  bool   in         = true;
  for (;;)
  {    
    RRDBParam *param = va_arg(ap, RRDBParam *);
    if (param == RRDB_PARAM_OUT)
    {
      in = false;
      continue;
    }

    if (param == NULL)
      break;

    if (in)
      ++in_params;
    else
      ++out_params;
  }
  va_end(ap);


  #define RRDB_FIELDS(T, X, ...) \
    X(T, types   , in_params , __VA_ARGS__); \
    X(T, bind    , in_params , __VA_ARGS__); \
    X(T, lengths , in_params , __VA_ARGS__); \
    X(T, is_null , in_params , __VA_ARGS__); \
    X(T, rtypes  , out_params, __VA_ARGS__); \
    X(T, rbind   , out_params, __VA_ARGS__); \
    X(T, rlengths, out_params, __VA_ARGS__); \
    X(T, ris_null, out_params, __VA_ARGS__);

  RRDBStmt *rs = NULL;
  RR_ARENA_ALLOC_INIT(RRDBStmt, RRDB_FIELDS, rs);
  if (!rs) {
    LOG_ERROR("out of memory");
    mysql_stmt_close(stmt);
    return NULL;
  }

  rs->con        = con;
  rs->stmt       = stmt;
  rs->in_params  = in_params;
  rs->out_params = out_params;

  va_start(ap, sql);
  for (size_t i = 0; i < in_params; i++)
  {
    RRDBParam *param = va_arg(ap, RRDBParam *);

    rs->types[i]              = param->type;
    rs->bind[i].buffer_type   = rr_db_type_to_mysql_type(param->type);
    rs->bind[i].buffer        = param->bind;
    rs->bind[i].buffer_length = rr_db_type_length(param->type);
    rs->bind[i].is_null       = param->is_null;
    rs->is_null[i]            = param->bind == NULL;

    if (param->bind == NULL)
    {
      rs->bind[i].buffer_length = 0;
      rs->bind[i].length = NULL;
      continue;
    }

    if (rr_db_type_is_stringish(param->type))
    {
      rs->lengths[i] = 0;
      rs->bind[i].buffer_length = rs->lengths[i];
      rs->bind[i].length        = &rs->lengths[i];

      if (param->type == RRDB_TYPE_BINARY)
      {
        rs->bind[i].flags |= BINARY_FLAG;
        rs->lengths[i]     = param->size;
      }
    }
    else
    {
      rs->bind[i].buffer_length = 0;
      rs->bind[i].length = NULL;
      rs->bind[i].is_unsigned = rr_db_type_is_unsigned(param->type);
    }
  }    

  // NULL or RRDB_PARAM_OUT
  (void)va_arg(ap, void *);

  for(size_t i = 0; i < out_params; ++i)
  {
    RRDBParam *param = va_arg(ap, RRDBParam *);

    if (rr_db_type_is_stringish(param->type) && param->size == 0)
    {
      LOG_ERROR("out parameter %ld is a variable length buffer and size == 0", i);
      mysql_stmt_close(stmt);
      free(rs);
      return NULL;
    }

    rs->rtypes[i]              = param->type;
    rs->rbind[i].buffer_type   = rr_db_type_to_mysql_type(param->type);
    rs->rbind[i].buffer        = param->bind;
    rs->rbind[i].buffer_length = rr_db_type_is_stringish(param->type) ? param->size : rr_db_type_length(param->type);
    rs->rbind[i].is_null       = &rs->ris_null[i];
    rs->rbind[i].length        = &rs->rlengths[i];
    rs->rbind[i].is_unsigned   = rr_db_type_is_unsigned(param->type);
    rs->ris_null[i]            = param->bind == NULL;

    // make room for a null terminator
    if (param->type == RRDB_TYPE_STRING)
      --rs->rbind[i].buffer_length;

    if (param->bind == NULL)
      rs->rbind[i].buffer_length = 0;
  }

  // NULL
  if (out_params > 0)
    (void)va_arg(ap, void *);

  va_end(ap);

  if (rs->in_params > 0 && mysql_stmt_bind_param(stmt, rs->bind) != 0)
  {
    LOG_ERROR("mysql_stmt_bind_param failed: %s", mysql_stmt_error(stmt));
    mysql_stmt_close(stmt);
    free(rs);
    return NULL;
  }

  if (rs->out_params > 0 && mysql_stmt_bind_result(stmt, rs->rbind) != 0)
  {
    LOG_ERROR("mysql_stmt_bind_result failed: %s", mysql_stmt_error(stmt));
    mysql_stmt_close(stmt);
    free(rs);
    return false;
  }

  return rs;
}

bool rr_db_execute_stmt(RRDBStmt *stmt, unsigned long long *affectedRows)
{
  for(int i = 0; i < stmt->in_params; ++i)
    if (stmt->types[i] == RRDB_TYPE_STRING)
    {
      const char * str = stmt->bind[i].buffer;      
      stmt->lengths[i] = (unsigned long)strlen(str);
      stmt->bind[i].buffer_length = stmt->lengths[i];
    }

  if (mysql_stmt_execute(stmt->stmt) != 0)
  {
    LOG_ERROR("execute failed: %s", mysql_stmt_error(stmt->stmt));
    return false;
  }

  if (affectedRows)
    *affectedRows = mysql_affected_rows(&stmt->con->con);

  return true;
}

bool rr_db_stmt_fetch_one(RRDBStmt *stmt)
{
  bool ret = false;
  if(!rr_db_execute_stmt(stmt, NULL))
    goto err;

  if (mysql_stmt_fetch(stmt->stmt) == MYSQL_NO_DATA)
    goto err;

  mysql_stmt_free_result(stmt->stmt);

  // null terminate strings
  for(int i = 0; i < stmt->out_params; ++i)
    if (stmt->rtypes[i] == RRDB_TYPE_STRING)
    {
      char *str = stmt->rbind[i].buffer;
      str[stmt->rlengths[i]] = '\0';
    }

  ret = true;
  err:
    return ret;
}

void rr_db_stmt_free(RRDBStmt **rs)
{
  if (!rs || !*rs)
    return;

  if ((*rs)->stmt)
    mysql_stmt_close((*rs)->stmt);

  free(*rs);
  *rs = NULL;
}

unsigned rr_db_get_registrar_id(RRDBCon *con, const char *name, bool create, unsigned *serial, unsigned *last_import)
{
  RRDBStmt *st = NULL;
  unsigned ret = 0;

  if (create)
  {
    if (!(st = rr_db_query_stmt(con,
      "INSERT IGNORE INTO registrar (name) VALUES (?)",
      &(RRDBParam){ .type = RRDB_TYPE_STRING, .bind = (char *)name },
      NULL)))
    {
      LOG_ERROR("prepare failed");
      goto err;
    }

    if (!rr_db_execute_stmt(st, NULL))
      goto err;

    rr_db_stmt_free(&st);      
  }

  if (!(st = rr_db_query_stmt(con,
    "SELECT id, serial, last_import FROM registrar WHERE name = ?",
    &(RRDBParam){ .type = RRDB_TYPE_STRING, .bind = (char *)name },
    RRDB_PARAM_OUT,
    &(RRDBParam){ .type = RRDB_TYPE_UINT, .bind = &ret        },
    &(RRDBParam){ .type = RRDB_TYPE_UINT, .bind = serial      },
    &(RRDBParam){ .type = RRDB_TYPE_UINT, .bind = last_import },

    NULL)))
  {
    LOG_ERROR("prepare failed");
    goto err;
  }

  if (!rr_db_stmt_fetch_one(st))
  {
    LOG_ERROR("failed to fetch");
    goto err;
  }

err:
  rr_db_stmt_free(&st);
  return ret;
}

bool rr_db_prepare_org_insert(RRDBCon *con, RRDBStmt **stmt, RRDBOrg *in)
{
  RRDBStmt *st = rr_db_query_stmt(
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

bool rr_db_prepare_netblockv4_insert(RRDBCon *con, RRDBStmt **stmt, RRDBNetBlock *in)
{
  RRDBStmt *st = rr_db_query_stmt(
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

bool rr_db_prepare_netblockv6_insert(RRDBCon *con, RRDBStmt **stmt, RRDBNetBlock *in)
{
  RRDBStmt *st = rr_db_query_stmt(
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

bool rr_db_finalize_registrar(RRDBCon *con, unsigned registrar_id, unsigned serial, RRDBStatistics *stats)
{
  RRDBStmt *st;

  // update the registrar serial & import timestamp
  st = rr_db_query_stmt(con,
    "UPDATE registrar SET serial = ?, last_import = UNIX_TIMESTAMP() WHERE id = ?",
    &(RRDBParam){ .type = RRDB_TYPE_UINT, .bind = &serial       },
    &(RRDBParam){ .type = RRDB_TYPE_UINT, .bind = &registrar_id },
    NULL);

  if (!st || !rr_db_execute_stmt(st, NULL))
  {
    rr_db_stmt_free(&st);
    return false;
  }
  rr_db_stmt_free(&st);

  // delete all old records
  st = rr_db_query_stmt(con,
    "DELETE FROM netblock_v4 WHERE registrar_id = ? AND serial != ?",
    &(RRDBParam){ .type = RRDB_TYPE_UINT, .bind = &registrar_id },
    &(RRDBParam){ .type = RRDB_TYPE_UINT, .bind = &serial       },
    NULL);

  if (!st || !rr_db_execute_stmt(st, &stats->deletedIPv4))
  {
    rr_db_stmt_free(&st);
    return false;
  }
  rr_db_stmt_free(&st);

  st = rr_db_query_stmt(con,
    "DELETE FROM netblock_v6 WHERE registrar_id = ? AND serial != ?",
    &(RRDBParam){ .type = RRDB_TYPE_UINT, .bind = &registrar_id },
    &(RRDBParam){ .type = RRDB_TYPE_UINT, .bind = &serial       },
    NULL);

  if (!st || !rr_db_execute_stmt(st, &stats->deletedIPv6))
  {
    rr_db_stmt_free(&st);
    return false;
  }
  rr_db_stmt_free(&st);

  st = rr_db_query_stmt(con,
    "DELETE FROM org WHERE registrar_id = ? AND serial != ?",
    &(RRDBParam){ .type = RRDB_TYPE_UINT, .bind = &registrar_id },
    &(RRDBParam){ .type = RRDB_TYPE_UINT, .bind = &serial       },
    NULL);    

  if (!st || !rr_db_execute_stmt(st, &stats->deletedOrgs))
  {
    rr_db_stmt_free(&st);
    return false;
  }  
  rr_db_stmt_free(&st);

  // link orgs to netblocks
  st = rr_db_query_stmt(con,
    "UPDATE netblock_v4 nb "
    "LEFT JOIN org o "
    "ON o.registrar_id = nb.registrar_id "
    "AND o.name = nb.org_id_str "
    "SET nb.org_id = o.id",
    NULL);

  if (!st || !rr_db_execute_stmt(st, NULL))
  {
    rr_db_stmt_free(&st);
    return false;
  }
  rr_db_stmt_free(&st);

  st = rr_db_query_stmt(con,
    "UPDATE netblock_v6 nb "
    "LEFT JOIN org o "
    "ON o.registrar_id = nb.registrar_id "
    "AND o.name = nb.org_id_str "
    "SET nb.org_id = o.id",
    NULL);

  if (!st || !rr_db_execute_stmt(st, NULL))
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

bool rr_db_prepare_lookup_ipv4(RRDBCon *con, RRDBStmt **stmt, uint32_t *in, RRDBIPInfo *out)
{
  RRDBStmt *st;

  st = rr_db_query_stmt(con,
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

bool rr_db_prepare_lookup_ipv6(RRDBCon *con, RRDBStmt **stmt, unsigned __int128 *in, RRDBIPInfo *out)
{
  RRDBStmt *st;

  st = rr_db_query_stmt(con,
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