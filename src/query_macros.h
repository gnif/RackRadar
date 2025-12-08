#define DEFAULT_STMT_PREPARE(type, x, ...) \
  static bool _stmt_prepare_ ##x(RRDBCon *con, type *qd) \
  { \
    typeof(qd->x) *this = &qd->x; (void)this; \
    RRDBStmt *st = rr_db_stmt_prepare(con, __VA_ARGS__, NULL); \
    if (!st) \
      return false; \
    qd->x.stmt = st; \
    return true; \
  }

#define DEFAULT_STMT_FREE(type, x) \
  static void _stmt_free_ ##x (RRDBCon *con, type *qd) \
  { \
    rr_db_stmt_free(&qd->x.stmt); \
  }

#define DEFAULT_STMT(type, x, ...) \
  DEFAULT_STMT_PREPARE(type, x, __VA_ARGS__) \
  DEFAULT_STMT_FREE(type, x)

#define STMT_STRUCT(x, y) \
  struct \
  { \
    RRDBStmt *stmt; \
    y \
  } x

#define STMT_PREPARE_ONE(name) \
  do { \
    if (!_stmt_prepare_##name(con, _rr_stmt_udata)) { \
      LOG_ERROR("_stmt_prepare_" #name " failed"); \
      return false; \
    } \
  } while (0);

#define STMT_FREE_ONE(name) \
  do { \
    _stmt_free_##name(con, _rr_stmt_udata); \
  } while (0);

#define STMT_PREPARE(LIST, udata) \
  do { \
    __auto_type _rr_stmt_udata = (udata); \
    LIST(STMT_PREPARE_ONE); \
  } while (0)

#define STMT_FREE(LIST, udata) \
  do { \
    __auto_type _rr_stmt_udata = (udata); \
    LIST(STMT_FREE_ONE); \
  } while (0)
