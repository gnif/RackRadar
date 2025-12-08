#include "db.h"
#include "config.h"
#include "log.h"
#include "util.h"

#include <mysql.h>
#include <errmsg.h>
#include <pthread.h>
#include <assert.h>
#include <string.h>

struct RRDBCon
{
  unsigned id;
  bool     in_use;
  bool     is_faulty;
  bool     is_reserved;
  MYSQL    con;

  // global user data
  void      *gudata;

  // per connection user data
  DBUdataFn  udataInitFn;
  DBUdataFn  udataDeInitFn;
  void      *ludata;
};

static struct
{
  bool  initialized;

  DBUdataFn udataInitFn;
  DBUdataFn udataDeInitFn;

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

static inline int rr_mysql_needs_reconnect(unsigned err)
{
  switch (err)
  {
    case CR_SERVER_GONE_ERROR:  // 2006
    case CR_SERVER_LOST:        // 2013
#ifdef CR_SERVER_LOST_EXTENDED
    case CR_SERVER_LOST_EXTENDED: // 2055 (MariaDB Connector/C)
#endif
      return 1;
    default:
      return 0;
  }
}

static bool rr_db_init_con(RRDBCon *con)
{
  if (!mysql_init(&con->con))
  {
    con->is_faulty = true;
    LOG_ERROR("mysql_init failed for connection %u", con->id);
    return false;
  }

  my_bool reconnect = false;
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
    con->is_faulty = true;
    LOG_ERROR("%s", mysql_error(&con->con));
    return false;
  };

  mysql_query     (&con->con, "SET collation_connection = 'utf8mb4_unicode_ci'");
  mysql_autocommit(&con->con, 1);

  if (db.udataInitFn && !db.udataInitFn(con, &con->gudata))
  {
    con->is_faulty = true;
    LOG_ERROR("global udataInitFn returned false");
    return false;
  }

  if (con->udataInitFn && !con->udataInitFn(con, &con->ludata))
  {
    con->is_faulty = true;
    LOG_ERROR("connection udataInitFn returned false");
    return false;
  }

  con->in_use    = false;
  con->is_faulty = false;
  return true;
}

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

        if (con->is_faulty || mysql_ping(&con->con) != 0)
        {
          LOG_WARN("connection %u failed, reconnecting", con->id);
          if (con->udataDeInitFn)
            con->udataDeInitFn(con, &con->ludata);

          if (db.udataDeInitFn)
            db.udataDeInitFn(con, &con->gudata);

          mysql_close(&con->con);
          if (!rr_db_init_con(con))
          {
            LOG_ERROR("failed to reconnect %u", con->id);
            continue;
          }
          LOG_INFO("reconnected %d", con->id);
        }
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
    if (!rr_db_init_con(con))
    {
      rr_db_deinit();
      return false;
    }
    ++db.sz_pool;
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

    if (con->udataDeInitFn && !con->udataDeInitFn(con, &con->ludata))
      LOG_ERROR("connection udataDeInitFn returned false");

    if (db.udataDeInitFn && !db.udataDeInitFn(con, &con->gudata))
      LOG_ERROR("global udataDeInitFn returned false");

    mysql_close(&con->con);
  }
  free(db.pool);
  memset(&db, 0, sizeof(db));
}

bool rr_db_reserve(RRDBCon **out, DBUdataFn udataInitFn, DBUdataFn udataDeInitFn)
{
  if (!db.initialized || !out)
    return false;

  pthread_mutex_lock(&db.pool_lock);
  for(size_t i = 0; i < db.sz_pool; ++i)
  {
    RRDBCon *con = db.pool + i;
    if (con->in_use || con->is_faulty || con->is_reserved)
      continue;

    con->is_reserved   = true;
    con->udataInitFn   = udataInitFn;
    con->udataDeInitFn = udataDeInitFn;
    con->ludata        = NULL;
    pthread_mutex_unlock(&db.pool_lock);

    if (udataInitFn && !udataInitFn(con, &con->ludata))
    {
      LOG_ERROR("udataInitFn returned false");
      rr_db_release(&con);
      return false;
    }

    *out = con;
    return true;
  }
  pthread_mutex_unlock(&db.pool_lock);
  return false;
}

void rr_db_release(RRDBCon **con)
{
  if (!con || !*con)
    return;

  pthread_mutex_lock(&db.pool_lock);
  (*con)->is_reserved   = false;
  (*con)->udataInitFn   = NULL;
  (*con)->udataDeInitFn = NULL;
  (*con)->ludata        = NULL;
  pthread_mutex_unlock(&db.pool_lock);
  *con = NULL;
}

bool rr_db_get(RRDBCon **out)
{
  if (!db.initialized || !out)
    return false;

  pthread_mutex_lock(&db.pool_lock);

  if (*out && (*out)->is_reserved)
  {
    (*out)->in_use = true;
    pthread_mutex_unlock(&db.pool_lock);
    return true;
  }

  for(size_t i = 0; i < db.sz_pool; ++i)
  {
    RRDBCon *con = db.pool + i;
    if (con->in_use || con->is_faulty || con->is_reserved)
      continue;

    con->in_use = true;
    pthread_mutex_unlock(&db.pool_lock);

    *out = con;
    return true;
  }
  pthread_mutex_unlock(&db.pool_lock);
  return false;
}

void rr_db_put(RRDBCon **con)
{
  if (!con || !*con)
    return;

  pthread_mutex_lock(&db.pool_lock);
  (*con)->in_use = false;
  pthread_mutex_unlock(&db.pool_lock);
  *con = NULL;
}

void *rr_db_get_con_gudata(RRDBCon *con)
{
  return con->gudata;
}

void *rr_db_get_con_ludata(RRDBCon *con)
{
  return con->ludata;
}

bool rr_db_start(RRDBCon *con)
{
  if (mysql_query(&con->con, "START TRANSACTION") != 0)
  {
    con->is_faulty = rr_mysql_needs_reconnect(mysql_errno(&con->con));
    LOG_ERROR("failed to start the transaction (%u): %s", con->id, mysql_error(&con->con));
    return false;
  }
  return true;
}

bool rr_db_commit(RRDBCon *con)
{
  if (mysql_query(&con->con, "COMMIT") != 0)
  {
    con->is_faulty = rr_mysql_needs_reconnect(mysql_errno(&con->con));
    LOG_ERROR("failed to commit the transaction (%u): %s", con->id, mysql_error(&con->con));
    return false;
  }
  return true;
}

bool rr_db_rollback(RRDBCon *con)
{
  if (mysql_query(&con->con, "ROLLBACK") != 0)
  {
    con->is_faulty = rr_mysql_needs_reconnect(mysql_errno(&con->con));
    LOG_ERROR("failed to rollback the transaction (%u): %s", con->id, mysql_error(&con->con));
    return false;
  }
  return true;
}

RRDBStmt *rr_db_stmt_prepare(RRDBCon *con, const char *sql, ...)
{
  MYSQL_STMT *stmt = mysql_stmt_init(&con->con);
  if (!stmt)
  {
    con->is_faulty = rr_mysql_needs_reconnect(mysql_errno(&con->con));
    return NULL;
  }

  if (mysql_stmt_prepare(stmt, sql, (unsigned long)strlen(sql)) != 0)
  {
    con->is_faulty = rr_mysql_needs_reconnect(mysql_errno(&con->con));
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

bool rr_db_stmt_execute(RRDBStmt *stmt, unsigned long long *affectedRows)
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
    stmt->con->is_faulty = rr_mysql_needs_reconnect(mysql_stmt_errno(stmt->stmt));
    LOG_ERROR("execute failed (%u): %s", stmt->con->id, mysql_stmt_error(stmt->stmt));
    return false;
  }

  if (affectedRows)
    *affectedRows = mysql_affected_rows(&stmt->con->con);

  return true;
}

unsigned long long rr_db_stmt_insert_id(RRDBStmt *stmt)
{
  return mysql_stmt_insert_id(stmt->stmt);
}

int rr_db_stmt_fetch_one(RRDBStmt *stmt)
{
  int ret = -1;
  if(!rr_db_stmt_execute(stmt, NULL))
    goto err;

  int rc;
  if ((rc = mysql_stmt_fetch(stmt->stmt)) == 1)
  {
    stmt->con->is_faulty = rr_mysql_needs_reconnect(mysql_stmt_errno(stmt->stmt));
    goto err;
  }

  if (!mysql_stmt_free_result(stmt->stmt) && mysql_stmt_errno(stmt->stmt) != 0)
  {
    LOG_ERROR("mysql_stmt_free_result: %s", mysql_stmt_error(stmt->stmt));
    stmt->con->is_faulty = rr_mysql_needs_reconnect(mysql_stmt_errno(stmt->stmt));
    goto err;
  }

  if (rc == MYSQL_NO_DATA)
  {
    ret = 0;
    goto err;
  }

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

  if ((*rs)->stmt && !mysql_stmt_close((*rs)->stmt))
    (*rs)->con->is_faulty = rr_mysql_needs_reconnect(mysql_errno(&(*rs)->con->con));

  free(*rs);
  *rs = NULL;
}
