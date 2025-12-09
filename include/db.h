#ifndef _H_RR_DB_
#define _H_RR_DB_

#include <stdbool.h>
#include <stdint.h>
#include <unistd.h>

typedef struct RRDBCon  RRDBCon;
typedef struct RRDBStmt RRDBStmt;

typedef enum RRDBType
{
  RRDB_TYPE_INT8,
  RRDB_TYPE_UINT8,
  RRDB_TYPE_INT,
  RRDB_TYPE_UINT,
  RRDB_TYPE_BIGINT,
  RRDB_TYPE_UBIGINT,
  RRDB_TYPE_FLOAT,
  RRDB_TYPE_DOUBLE,
  RRDB_TYPE_STRING,
  RRDB_TYPE_BINARY
}
RRDBType;

typedef struct RRDBParam
{
  RRDBType type;
  void    *bind;
  char    *is_null;
  size_t   size;
}
RRDBParam;

#define RRDB_PARAM_OUT ((void *)((uintptr_t)-1))

typedef bool (*DBUdataFn)(RRDBCon *con, void **udata);

bool rr_db_init(DBUdataFn udataInitFn, DBUdataFn udataDeInitFn);
void rr_db_deinit(void);

bool rr_db_reserve(RRDBCon **out, DBUdataFn udataInitFn, DBUdataFn udataDeInitFn);
void rr_db_release(RRDBCon **con);

bool rr_db_get(RRDBCon **out);
void rr_db_put(RRDBCon **con);

void *rr_db_get_con_gudata(RRDBCon *con);
void *rr_db_get_con_ludata(RRDBCon *con);

bool rr_db_start   (RRDBCon *con);
bool rr_db_commit  (RRDBCon *con);
bool rr_db_rollback(RRDBCon *con);

RRDBStmt          *rr_db_stmt_prepare  (RRDBCon *con, const char *sql, ...);
bool               rr_db_stmt_execute  (RRDBStmt *stmt, unsigned long long *affectedRows);
unsigned long long rr_db_stmt_insert_id(RRDBStmt *stmt);
int                rr_db_stmt_fetch_one(RRDBStmt *stmt);
void               rr_db_stmt_free     (RRDBStmt **rs);

bool rr_db_stmt_query(RRDBStmt *stmt);
bool rr_db_stmt_store(RRDBStmt *stmt);
int  rr_db_stmt_fetch(RRDBStmt *stmt);
void rr_db_stmt_close(RRDBStmt *stmt);

#endif
