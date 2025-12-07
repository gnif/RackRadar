#define DEFAULT_STMT_PREPARE(x, y) \
  static bool rr_query_prepare_ ##x(RRDBCon *con, DBQueryData *qd) \
  { \
    typeof(qd->x) *this = &qd->x; (void)this; \
    y \
    if (!st) \
      return false; \
    qd->x.stmt = st; \
    return true; \
  }

#define DEFAULT_STMT_FREE(x) \
  static void rr_query_free_ ##x (RRDBCon *con, DBQueryData *qd) \
  { \
    rr_db_stmt_free(&qd->x.stmt); \
  }

#define DEFAULT_STMT(x, y) \
  DEFAULT_STMT_PREPARE(x, y) \
  DEFAULT_STMT_FREE(x)  

#define STMT_STRUCT(x, y) \
  struct \
  { \
    RRDBStmt *stmt; \
    y \
  } x
