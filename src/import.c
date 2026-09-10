#include "import.h"
#include "config.h"
#include "util.h"
#include "log.h"
#include "download.h"
#include "db.h"
#include "query.h"
#include "query_macros.h"

#include <string.h>
#include <stdlib.h>
#include <assert.h>

#define RR_IMPORT_BATCH_ROWS 64

typedef struct RRImportBatch
{
  struct
  {
    RRDBStmt *stmt;
    RRDBOrg   rows[RR_IMPORT_BATCH_ROWS];
    size_t    count;
  }
  org;

  struct
  {
    RRDBStmt      *stmt;
    RRDBNetBlock   rows[RR_IMPORT_BATCH_ROWS];
    size_t         count;
  }
  ipv4;

  struct
  {
    RRDBStmt      *stmt;
    RRDBNetBlock   rows[RR_IMPORT_BATCH_ROWS];
    size_t         count;
  }
  ipv6;
}
RRImportBatch;

typedef struct RRImport
{
  RRDownload    *dl;
  RRDBCon       *con;
  RRDBStatistics stats;
  RRImportBatch  batch;

  STMT_STRUCT(registrar_insert,
    char in_name[32];
  );

  STMT_STRUCT(registrar_update_serial,
    unsigned in_registrar_id;
    unsigned in_serial;
  );

  STMT_STRUCT(registrar_lock,
    char     in_name[32];
    unsigned out_registrar_id;
    unsigned out_serial;
    unsigned out_last_import;
  );

  STMT_STRUCT(import_lock_acquire,
    uint8_t out_acquired;
  );

  STMT_STRUCT(import_lock_release,);

  STMT_STRUCT(org_insert,
    RRDBOrg in;
  );

  STMT_STRUCT(org_stage_truncate,);

  STMT_STRUCT(org_merge_insert,
    unsigned in_serial;
    unsigned in_registrar_id;
  );

  STMT_STRUCT(org_merge_update,
    unsigned in_serial;
    unsigned in_registrar_id;
  );

  STMT_STRUCT(org_delete_old,
    unsigned in_registrar_id;
  );

  STMT_STRUCT(netblockv4_insert,
    RRDBNetBlock in;
  );

  STMT_STRUCT(netblockv4_stage_truncate,);

  STMT_STRUCT(netblockv4_merge_insert,
    unsigned in_serial;
    unsigned in_registrar_id;
  );

  STMT_STRUCT(netblockv4_merge_update,
    unsigned in_serial;
    unsigned in_registrar_id;
  );

  STMT_STRUCT(netblockv4_delete_old,
    unsigned in_registrar_id;
  );

  STMT_STRUCT(netblockv4_link_org,
    unsigned in_registrar_id;
  );

  STMT_STRUCT(netblockv6_insert,
    RRDBNetBlock in;
  );

  STMT_STRUCT(netblockv6_stage_truncate,);

  STMT_STRUCT(netblockv6_merge_insert,
    unsigned in_serial;
    unsigned in_registrar_id;
  );

  STMT_STRUCT(netblockv6_merge_update,
    unsigned in_serial;
    unsigned in_registrar_id;
  );

  STMT_STRUCT(netblockv6_delete_old,
    unsigned in_registrar_id;
  );

  STMT_STRUCT(netblockv6_link_org,
    unsigned in_registrar_id;
  );

  STMT_STRUCT(unions_dirty_get,
    uint8_t out_dirty;
  );

  STMT_STRUCT(unions_mark_dirty,);
  STMT_STRUCT(unions_mark_clean,);

  STMT_STRUCT(netblockv4_union_next_truncate,);
  STMT_STRUCT(netblockv6_union_next_truncate,);
  STMT_STRUCT(netblockv4_union_next_populate,);
  STMT_STRUCT(netblockv6_union_next_populate,);
  STMT_STRUCT(netblockv4_union_delete,);
  STMT_STRUCT(netblockv6_union_delete,);
  STMT_STRUCT(netblockv4_union_publish,);
  STMT_STRUCT(netblockv6_union_publish,);

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
    ConfigList *cl;
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
  X(registrar_lock                ) \
  X(import_lock_acquire           ) \
  X(import_lock_release           ) \
  X(org_insert                    ) \
  X(org_stage_truncate            ) \
  X(org_merge_insert              ) \
  X(org_merge_update              ) \
  X(org_delete_old                ) \
  X(netblockv4_insert             ) \
  X(netblockv4_stage_truncate     ) \
  X(netblockv4_merge_insert       ) \
  X(netblockv4_merge_update       ) \
  X(netblockv4_delete_old         ) \
  X(netblockv4_link_org           ) \
  X(netblockv6_insert             ) \
  X(netblockv6_stage_truncate     ) \
  X(netblockv6_merge_insert       ) \
  X(netblockv6_merge_update       ) \
  X(netblockv6_delete_old         ) \
  X(netblockv6_link_org           ) \
  X(unions_dirty_get              ) \
  X(unions_mark_dirty             ) \
  X(unions_mark_clean             ) \
  X(netblockv4_union_next_truncate) \
  X(netblockv6_union_next_truncate) \
  X(netblockv4_union_next_populate) \
  X(netblockv6_union_next_populate) \
  X(netblockv4_union_delete       ) \
  X(netblockv6_union_delete       ) \
  X(netblockv4_union_publish      ) \
  X(netblockv6_union_publish      ) \
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

DEFAULT_STMT(RRImport, registrar_lock,
  "SELECT id, serial, last_import FROM registrar WHERE name = ? FOR UPDATE",
  &(RRDBParam){ .type = RRDB_TYPE_STRING, .bind = this->in_name },
  RRDB_PARAM_OUT,
  &(RRDBParam){ .type = RRDB_TYPE_UINT, .bind = &this->out_registrar_id },
  &(RRDBParam){ .type = RRDB_TYPE_UINT, .bind = &this->out_serial       },
  &(RRDBParam){ .type = RRDB_TYPE_UINT, .bind = &this->out_last_import  }
);

DEFAULT_STMT(RRImport, import_lock_acquire,
  "SELECT COALESCE(GET_LOCK(SHA2(CONCAT('RackRadar:import:', DATABASE()), 256), 0), 0)",
  RRDB_PARAM_OUT,
  &(RRDBParam){ .type = RRDB_TYPE_UINT8, .bind = &this->out_acquired }
);

DEFAULT_STMT(RRImport, import_lock_release,
  "DO RELEASE_LOCK(SHA2(CONCAT('RackRadar:import:', DATABASE()), 256))"
);

DEFAULT_STMT(RRImport, org_insert,
  "INSERT INTO org_stage ("
    "registrar_id, "
    "handle, "
    "name, "
    "descr"
  ") VALUES ("
    "?,"
    "?,"
    "?,"
    "?"
  ") ON DUPLICATE KEY UPDATE "
    "name   = VALUES(name), "
    "descr  = VALUES(descr)",

  &(RRDBParam){ .type = RRDB_TYPE_UINT  , .bind = &this->in.registrar_id },
  &(RRDBParam){ .type = RRDB_TYPE_STRING, .bind = &this->in.handle       },
  &(RRDBParam){ .type = RRDB_TYPE_STRING, .bind = &this->in.name         },
  &(RRDBParam){ .type = RRDB_TYPE_STRING, .bind = &this->in.descr        }
);

DEFAULT_STMT(RRImport, org_stage_truncate,
  "TRUNCATE TABLE org_stage"
);

DEFAULT_STMT(RRImport, org_merge_insert,
  "INSERT INTO org (registrar_id, serial, handle, name, descr) "
  "SELECT s.registrar_id, ?, s.handle, s.name, s.descr "
  "FROM org_stage s "
  "LEFT JOIN org o "
    "ON o.registrar_id = s.registrar_id "
    "AND o.handle = s.handle "
  "WHERE s.registrar_id = ? AND o.id IS NULL",
  &(RRDBParam){ .type = RRDB_TYPE_UINT, .bind = &this->in_serial       },
  &(RRDBParam){ .type = RRDB_TYPE_UINT, .bind = &this->in_registrar_id }
);

DEFAULT_STMT(RRImport, org_merge_update,
  "UPDATE org o "
  "JOIN org_stage s "
    "ON s.registrar_id = o.registrar_id "
    "AND s.handle = o.handle "
  "SET o.serial = ?, o.name = s.name, o.descr = s.descr "
  "WHERE o.registrar_id = ? "
  "AND ("
    "NOT (CAST(o.name AS BINARY) <=> CAST(s.name AS BINARY)) OR "
    "NOT (CAST(o.descr AS BINARY) <=> CAST(s.descr AS BINARY))"
  ")",
  &(RRDBParam){ .type = RRDB_TYPE_UINT, .bind = &this->in_serial       },
  &(RRDBParam){ .type = RRDB_TYPE_UINT, .bind = &this->in_registrar_id }
);

DEFAULT_STMT(RRImport, org_delete_old,
  "DELETE o FROM org o "
  "LEFT JOIN org_stage s "
    "ON s.registrar_id = o.registrar_id "
    "AND s.handle = o.handle "
  "WHERE o.registrar_id = ? AND s.registrar_id IS NULL",
  &(RRDBParam){ .type = RRDB_TYPE_UINT, .bind = &this->in_registrar_id }
);

DEFAULT_STMT(RRImport, netblockv4_insert,
  "INSERT INTO netblock_v4_stage ("
    "registrar_id, "
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
    "?"
  ") ON DUPLICATE KEY UPDATE "
    "prefix_len = VALUES(prefix_len), "
    "netname = VALUES(netname), "
    "descr   = VALUES(descr)",

  &(RRDBParam){ .type = RRDB_TYPE_UINT  , .bind = &this->in.registrar_id },
  &(RRDBParam){ .type = RRDB_TYPE_STRING, .bind = &this->in.org_handle   },
  &(RRDBParam){ .type = RRDB_TYPE_UINT  , .bind = &this->in.startAddr.v4 },
  &(RRDBParam){ .type = RRDB_TYPE_UINT  , .bind = &this->in.endAddr  .v4 },
  &(RRDBParam){ .type = RRDB_TYPE_UINT8 , .bind = &this->in.prefixLen    },
  &(RRDBParam){ .type = RRDB_TYPE_STRING, .bind = &this->in.netname      },
  &(RRDBParam){ .type = RRDB_TYPE_STRING, .bind = &this->in.descr        }
);

DEFAULT_STMT(RRImport, netblockv4_stage_truncate,
  "TRUNCATE TABLE netblock_v4_stage"
);

DEFAULT_STMT(RRImport, netblockv4_merge_insert,
  "INSERT INTO netblock_v4 ("
    "registrar_id, serial, org_handle, start_ip, end_ip, prefix_len, netname, descr"
  ") "
  "SELECT s.registrar_id, ?, s.org_handle, s.start_ip, s.end_ip, "
    "s.prefix_len, s.netname, s.descr "
  "FROM netblock_v4_stage s "
  "LEFT JOIN netblock_v4 nb "
    "ON nb.registrar_id = s.registrar_id "
    "AND nb.org_handle = s.org_handle "
    "AND nb.start_ip = s.start_ip "
    "AND nb.end_ip = s.end_ip "
  "WHERE s.registrar_id = ? AND nb.id IS NULL",
  &(RRDBParam){ .type = RRDB_TYPE_UINT, .bind = &this->in_serial       },
  &(RRDBParam){ .type = RRDB_TYPE_UINT, .bind = &this->in_registrar_id }
);

DEFAULT_STMT(RRImport, netblockv4_merge_update,
  "UPDATE netblock_v4 nb "
  "JOIN netblock_v4_stage s "
    "ON s.registrar_id = nb.registrar_id "
    "AND s.org_handle = nb.org_handle "
    "AND s.start_ip = nb.start_ip "
    "AND s.end_ip = nb.end_ip "
  "SET "
    "nb.serial = ?, "
    "nb.prefix_len = s.prefix_len, "
    "nb.netname = s.netname, "
    "nb.descr = s.descr "
  "WHERE nb.registrar_id = ? "
  "AND ("
    "nb.prefix_len != s.prefix_len OR "
    "NOT (CAST(nb.netname AS BINARY) <=> CAST(s.netname AS BINARY)) OR "
    "NOT (CAST(nb.descr AS BINARY) <=> CAST(s.descr AS BINARY))"
  ")",
  &(RRDBParam){ .type = RRDB_TYPE_UINT, .bind = &this->in_serial       },
  &(RRDBParam){ .type = RRDB_TYPE_UINT, .bind = &this->in_registrar_id }
);

DEFAULT_STMT(RRImport, netblockv4_delete_old,
  "DELETE nb FROM netblock_v4 nb "
  "LEFT JOIN netblock_v4_stage s "
    "ON s.registrar_id = nb.registrar_id "
    "AND s.org_handle = nb.org_handle "
    "AND s.start_ip = nb.start_ip "
    "AND s.end_ip = nb.end_ip "
  "WHERE nb.registrar_id = ? AND s.registrar_id IS NULL",
  &(RRDBParam){ .type = RRDB_TYPE_UINT, .bind = &this->in_registrar_id }
);

DEFAULT_STMT(RRImport, netblockv4_link_org,
  "UPDATE netblock_v4 nb "
    "LEFT JOIN org_stage os "
    "ON os.registrar_id = nb.registrar_id "
    "AND os.handle = nb.org_handle "
    "LEFT JOIN org o "
    "ON o.registrar_id = os.registrar_id "
    "AND o.handle = os.handle "
    "SET nb.org_id = o.id "
    "WHERE nb.registrar_id = ? "
    "AND NOT (nb.org_id <=> o.id)",
  &(RRDBParam){ .type = RRDB_TYPE_UINT, .bind = &this->in_registrar_id }
);

DEFAULT_STMT(RRImport, netblockv6_insert,
  "INSERT INTO netblock_v6_stage ("
    "registrar_id, "
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
    "?"
  ") ON DUPLICATE KEY UPDATE "
    "prefix_len = VALUES(prefix_len), "
    "netname = VALUES(netname), "
    "descr   = VALUES(descr)",

  &(RRDBParam){ .type = RRDB_TYPE_UINT  , .bind = &this->in.registrar_id },
  &(RRDBParam){ .type = RRDB_TYPE_STRING, .bind = &this->in.org_handle   },
  &(RRDBParam){ .type = RRDB_TYPE_BINARY, .bind = &this->in.startAddr.v6, .size = sizeof(this->in.startAddr) },
  &(RRDBParam){ .type = RRDB_TYPE_BINARY, .bind = &this->in.endAddr  .v6, .size = sizeof(this->in.endAddr  ) },
  &(RRDBParam){ .type = RRDB_TYPE_UINT8 , .bind = &this->in.prefixLen    },
  &(RRDBParam){ .type = RRDB_TYPE_STRING, .bind = &this->in.netname      },
  &(RRDBParam){ .type = RRDB_TYPE_STRING, .bind = &this->in.descr        }
);

DEFAULT_STMT(RRImport, netblockv6_stage_truncate,
  "TRUNCATE TABLE netblock_v6_stage"
);

DEFAULT_STMT(RRImport, netblockv6_merge_insert,
  "INSERT INTO netblock_v6 ("
    "registrar_id, serial, org_handle, start_ip, end_ip, prefix_len, netname, descr"
  ") "
  "SELECT s.registrar_id, ?, s.org_handle, s.start_ip, s.end_ip, "
    "s.prefix_len, s.netname, s.descr "
  "FROM netblock_v6_stage s "
  "LEFT JOIN netblock_v6 nb "
    "ON nb.registrar_id = s.registrar_id "
    "AND nb.org_handle = s.org_handle "
    "AND nb.start_ip = s.start_ip "
    "AND nb.end_ip = s.end_ip "
  "WHERE s.registrar_id = ? AND nb.id IS NULL",
  &(RRDBParam){ .type = RRDB_TYPE_UINT, .bind = &this->in_serial       },
  &(RRDBParam){ .type = RRDB_TYPE_UINT, .bind = &this->in_registrar_id }
);

DEFAULT_STMT(RRImport, netblockv6_merge_update,
  "UPDATE netblock_v6 nb "
  "JOIN netblock_v6_stage s "
    "ON s.registrar_id = nb.registrar_id "
    "AND s.org_handle = nb.org_handle "
    "AND s.start_ip = nb.start_ip "
    "AND s.end_ip = nb.end_ip "
  "SET "
    "nb.serial = ?, "
    "nb.prefix_len = s.prefix_len, "
    "nb.netname = s.netname, "
    "nb.descr = s.descr "
  "WHERE nb.registrar_id = ? "
  "AND ("
    "nb.prefix_len != s.prefix_len OR "
    "NOT (CAST(nb.netname AS BINARY) <=> CAST(s.netname AS BINARY)) OR "
    "NOT (CAST(nb.descr AS BINARY) <=> CAST(s.descr AS BINARY))"
  ")",
  &(RRDBParam){ .type = RRDB_TYPE_UINT, .bind = &this->in_serial       },
  &(RRDBParam){ .type = RRDB_TYPE_UINT, .bind = &this->in_registrar_id }
);

DEFAULT_STMT(RRImport, netblockv6_delete_old,
  "DELETE nb FROM netblock_v6 nb "
  "LEFT JOIN netblock_v6_stage s "
    "ON s.registrar_id = nb.registrar_id "
    "AND s.org_handle = nb.org_handle "
    "AND s.start_ip = nb.start_ip "
    "AND s.end_ip = nb.end_ip "
  "WHERE nb.registrar_id = ? AND s.registrar_id IS NULL",
  &(RRDBParam){ .type = RRDB_TYPE_UINT, .bind = &this->in_registrar_id }
);

DEFAULT_STMT(RRImport, netblockv6_link_org,
  "UPDATE netblock_v6 nb "
    "LEFT JOIN org_stage os "
    "ON os.registrar_id = nb.registrar_id "
    "AND os.handle = nb.org_handle "
    "LEFT JOIN org o "
    "ON o.registrar_id = os.registrar_id "
    "AND o.handle = os.handle "
    "SET nb.org_id = o.id "
    "WHERE nb.registrar_id = ? "
    "AND NOT (nb.org_id <=> o.id)",
  &(RRDBParam){ .type = RRDB_TYPE_UINT, .bind = &this->in_registrar_id }
);

DEFAULT_STMT(RRImport, unions_dirty_get,
  "SELECT unions_dirty FROM import_state WHERE id = 1",
  RRDB_PARAM_OUT,
  &(RRDBParam){ .type = RRDB_TYPE_UINT8, .bind = &this->out_dirty }
);

DEFAULT_STMT(RRImport, unions_mark_dirty,
  "UPDATE import_state SET unions_dirty = 1 WHERE id = 1"
);

DEFAULT_STMT(RRImport, unions_mark_clean,
  "UPDATE import_state SET unions_dirty = 0 WHERE id = 1"
);

DEFAULT_STMT(RRImport, netblockv4_union_next_truncate,
  "TRUNCATE TABLE netblock_v4_union_next"
);

DEFAULT_STMT(RRImport, netblockv6_union_next_truncate,
  "TRUNCATE TABLE netblock_v6_union_next"
);

DEFAULT_STMT(RRImport, netblockv4_union_next_populate,
  "INSERT INTO netblock_v4_union_next (start_ip, end_ip) "
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
    "ORDER BY t.start_ip, t.end_ip "
  ") x "
  "GROUP BY grp"
);

DEFAULT_STMT(RRImport, netblockv6_union_next_populate,
  "INSERT INTO netblock_v6_union_next (start_ip, end_ip) "
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
    "ORDER BY t.start_ip, t.end_ip "
  ") x "
  "GROUP BY grp"
);

DEFAULT_STMT(RRImport, netblockv4_union_delete,
  "DELETE FROM netblock_v4_union"
);

DEFAULT_STMT(RRImport, netblockv6_union_delete,
  "DELETE FROM netblock_v6_union"
);

DEFAULT_STMT(RRImport, netblockv4_union_publish,
  "INSERT INTO netblock_v4_union (start_ip, end_ip) "
  "SELECT start_ip, end_ip FROM netblock_v4_union_next"
);

DEFAULT_STMT(RRImport, netblockv6_union_publish,
  "INSERT INTO netblock_v6_union (start_ip, end_ip) "
  "SELECT start_ip, end_ip FROM netblock_v6_union_next"
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

static int rr_import_registrar_lock(
  const char *in_name,
  unsigned   *out_registrar_id,
  unsigned   *out_serial,
  unsigned   *out_last_import)
{
  strcpy(s_import.registrar_lock.in_name, in_name);
  int rc = rr_db_stmt_fetch_one(s_import.registrar_lock.stmt);
  if (rc != 1)
    return rc;

  *out_registrar_id = s_import.registrar_lock.out_registrar_id;
  *out_serial       = s_import.registrar_lock.out_serial;
  *out_last_import  = s_import.registrar_lock.out_last_import;
  return 1;
}

static void rr_import_batches_reset(void)
{
  s_import.batch.org .count = 0;
  s_import.batch.ipv4.count = 0;
  s_import.batch.ipv6.count = 0;
}

static void rr_import_stats_rollback(void)
{
  s_import.stats.newOrgs     = 0;
  s_import.stats.updatedOrgs = 0;
  s_import.stats.deletedOrgs = 0;
  s_import.stats.newIPv4     = 0;
  s_import.stats.updatedIPv4 = 0;
  s_import.stats.deletedIPv4 = 0;
  s_import.stats.newIPv6     = 0;
  s_import.stats.updatedIPv6 = 0;
  s_import.stats.deletedIPv6 = 0;
}

static bool rr_import_org_batch_flush(void)
{
  const size_t count = s_import.batch.org.count;
  if (count == 0)
    return true;

  if (count == RR_IMPORT_BATCH_ROWS)
  {
    if (!rr_db_stmt_execute(s_import.batch.org.stmt, NULL))
    {
      LOG_ERROR("failed to write organization stage batch (%zu rows, %s through %s)",
        count,
        s_import.batch.org.rows[0        ].handle,
        s_import.batch.org.rows[count - 1].handle);
      return false;
    }
  }
  else
    for(size_t i = 0; i < count; ++i)
    {
      memcpy(&s_import.org_insert.in, &s_import.batch.org.rows[i],
        sizeof(s_import.org_insert.in));
      if (!rr_db_stmt_execute(s_import.org_insert.stmt, NULL))
      {
        LOG_ERROR("failed to write organization %s to the staging table",
          s_import.batch.org.rows[i].handle);
        return false;
      }
    }

  s_import.batch.org.count = 0;
  return true;
}

static bool rr_import_netblockv4_batch_flush(void)
{
  const size_t count = s_import.batch.ipv4.count;
  if (count == 0)
    return true;

  if (count == RR_IMPORT_BATCH_ROWS)
  {
    if (!rr_db_stmt_execute(s_import.batch.ipv4.stmt, NULL))
    {
      LOG_ERROR("failed to write IPv4 stage batch (%zu rows)", count);
      return false;
    }
  }
  else
    for(size_t i = 0; i < count; ++i)
    {
      memcpy(&s_import.netblockv4_insert.in, &s_import.batch.ipv4.rows[i],
        sizeof(s_import.netblockv4_insert.in));
      if (!rr_db_stmt_execute(s_import.netblockv4_insert.stmt, NULL))
      {
        LOG_ERROR("failed to write IPv4 row %zu to the staging table", i);
        return false;
      }
    }

  s_import.batch.ipv4.count = 0;
  return true;
}

static bool rr_import_netblockv6_batch_flush(void)
{
  const size_t count = s_import.batch.ipv6.count;
  if (count == 0)
    return true;

  if (count == RR_IMPORT_BATCH_ROWS)
  {
    if (!rr_db_stmt_execute(s_import.batch.ipv6.stmt, NULL))
    {
      LOG_ERROR("failed to write IPv6 stage batch (%zu rows)", count);
      return false;
    }
  }
  else
    for(size_t i = 0; i < count; ++i)
    {
      memcpy(&s_import.netblockv6_insert.in, &s_import.batch.ipv6.rows[i],
        sizeof(s_import.netblockv6_insert.in));
      if (!rr_db_stmt_execute(s_import.netblockv6_insert.stmt, NULL))
      {
        LOG_ERROR("failed to write IPv6 row %zu to the staging table", i);
        return false;
      }
    }

  s_import.batch.ipv6.count = 0;
  return true;
}

static bool rr_import_batches_flush(void)
{
  return
    rr_import_org_batch_flush       () &&
    rr_import_netblockv4_batch_flush() &&
    rr_import_netblockv6_batch_flush();
}

bool rr_import_org_insert(RRDBOrg *in_org)
{
  if (s_import.batch.org.count >= RR_IMPORT_BATCH_ROWS)
  {
    LOG_ERROR("organization staging batch overflow");
    return false;
  }

  const size_t index = s_import.batch.org.count++;
  memcpy(&s_import.batch.org.rows[index], in_org, sizeof(*in_org));
  ++s_import.stats.processedOrgs;

  if (s_import.batch.org.count == RR_IMPORT_BATCH_ROWS)
    return rr_import_org_batch_flush();

  return true;
}

static bool rr_import_org_stage_truncate(void)
{
  return rr_db_stmt_execute(s_import.org_stage_truncate.stmt, NULL);
}

static bool rr_import_org_merge_insert(unsigned in_registrar_id, unsigned in_serial)
{
  s_import.org_merge_insert.in_registrar_id = in_registrar_id;
  s_import.org_merge_insert.in_serial       = in_serial;
  return rr_db_stmt_execute(s_import.org_merge_insert.stmt, &s_import.stats.newOrgs);
}

static bool rr_import_org_merge_update(unsigned in_registrar_id, unsigned in_serial)
{
  s_import.org_merge_update.in_registrar_id = in_registrar_id;
  s_import.org_merge_update.in_serial       = in_serial;
  return rr_db_stmt_execute(s_import.org_merge_update.stmt, &s_import.stats.updatedOrgs);
}

static bool rr_import_org_delete_old(unsigned in_registrar_id)
{
  s_import.org_delete_old.in_registrar_id = in_registrar_id;
  return rr_db_stmt_execute(s_import.org_delete_old.stmt, &s_import.stats.deletedOrgs);
}

bool rr_import_netblockv4_insert(RRDBNetBlock *in_netblock)
{
  if (s_import.batch.ipv4.count >= RR_IMPORT_BATCH_ROWS)
  {
    LOG_ERROR("IPv4 staging batch overflow");
    return false;
  }

  const size_t index = s_import.batch.ipv4.count++;
  memcpy(&s_import.batch.ipv4.rows[index], in_netblock, sizeof(*in_netblock));
  ++s_import.stats.processedIPv4;

  if (s_import.batch.ipv4.count == RR_IMPORT_BATCH_ROWS)
    return rr_import_netblockv4_batch_flush();

  return true;
}

static bool rr_import_netblockv4_stage_truncate(void)
{
  return rr_db_stmt_execute(s_import.netblockv4_stage_truncate.stmt, NULL);
}

static bool rr_import_netblockv4_merge_insert(unsigned in_registrar_id, unsigned in_serial)
{
  s_import.netblockv4_merge_insert.in_registrar_id = in_registrar_id;
  s_import.netblockv4_merge_insert.in_serial       = in_serial;
  return rr_db_stmt_execute(s_import.netblockv4_merge_insert.stmt, &s_import.stats.newIPv4);
}

static bool rr_import_netblockv4_merge_update(unsigned in_registrar_id, unsigned in_serial)
{
  s_import.netblockv4_merge_update.in_registrar_id = in_registrar_id;
  s_import.netblockv4_merge_update.in_serial       = in_serial;
  return rr_db_stmt_execute(s_import.netblockv4_merge_update.stmt, &s_import.stats.updatedIPv4);
}

static bool rr_import_netblockv4_delete_old(unsigned in_registrar_id)
{
  s_import.netblockv4_delete_old.in_registrar_id = in_registrar_id;
  return rr_db_stmt_execute(s_import.netblockv4_delete_old.stmt, &s_import.stats.deletedIPv4);
}

static bool rr_import_netblockv4_link_org(unsigned in_registrar_id)
{
  s_import.netblockv4_link_org.in_registrar_id = in_registrar_id;
  return rr_db_stmt_execute(s_import.netblockv4_link_org.stmt, NULL);
}

bool rr_import_netblockv6_insert(RRDBNetBlock *in_netblock)
{
  if (s_import.batch.ipv6.count >= RR_IMPORT_BATCH_ROWS)
  {
    LOG_ERROR("IPv6 staging batch overflow");
    return false;
  }

  const size_t index = s_import.batch.ipv6.count++;
  memcpy(&s_import.batch.ipv6.rows[index], in_netblock, sizeof(*in_netblock));
  ++s_import.stats.processedIPv6;

  if (s_import.batch.ipv6.count == RR_IMPORT_BATCH_ROWS)
    return rr_import_netblockv6_batch_flush();

  return true;
}

static bool rr_import_netblockv6_stage_truncate(void)
{
  return rr_db_stmt_execute(s_import.netblockv6_stage_truncate.stmt, NULL);
}

static bool rr_import_netblockv6_merge_insert(unsigned in_registrar_id, unsigned in_serial)
{
  s_import.netblockv6_merge_insert.in_registrar_id = in_registrar_id;
  s_import.netblockv6_merge_insert.in_serial       = in_serial;
  return rr_db_stmt_execute(s_import.netblockv6_merge_insert.stmt, &s_import.stats.newIPv6);
}

static bool rr_import_netblockv6_merge_update(unsigned in_registrar_id, unsigned in_serial)
{
  s_import.netblockv6_merge_update.in_registrar_id = in_registrar_id;
  s_import.netblockv6_merge_update.in_serial       = in_serial;
  return rr_db_stmt_execute(s_import.netblockv6_merge_update.stmt, &s_import.stats.updatedIPv6);
}

static bool rr_import_netblockv6_delete_old(unsigned in_registrar_id)
{
  s_import.netblockv6_delete_old.in_registrar_id = in_registrar_id;
  return rr_db_stmt_execute(s_import.netblockv6_delete_old.stmt, &s_import.stats.deletedIPv6);
}

static bool rr_import_netblockv6_link_org(unsigned in_registrar_id)
{
  s_import.netblockv6_link_org.in_registrar_id = in_registrar_id;
  return rr_db_stmt_execute(s_import.netblockv6_link_org.stmt, NULL);
}

static bool rr_import_stages_truncate(void)
{
  return
    rr_import_org_stage_truncate       () &&
    rr_import_netblockv4_stage_truncate() &&
    rr_import_netblockv6_stage_truncate();
}

static int rr_import_unions_dirty(bool *out_dirty)
{
  int rc = rr_db_stmt_fetch_one(s_import.unions_dirty_get.stmt);
  if (rc == 1)
    *out_dirty = s_import.unions_dirty_get.out_dirty != 0;
  return rc;
}

static bool rr_import_unions_mark_dirty(void)
{
  return rr_db_stmt_execute(s_import.unions_mark_dirty.stmt, NULL);
}

static bool rr_import_unions_mark_clean(void)
{
  return rr_db_stmt_execute(s_import.unions_mark_clean.stmt, NULL);
}

static bool rr_import_netblockv4_union_next_truncate(void)
{
  return rr_db_stmt_execute(s_import.netblockv4_union_next_truncate.stmt, NULL);
}

static bool rr_import_netblockv6_union_next_truncate(void)
{
  return rr_db_stmt_execute(s_import.netblockv6_union_next_truncate.stmt, NULL);
}

static bool rr_import_netblockv4_union_next_populate(void)
{
  return rr_db_stmt_execute(s_import.netblockv4_union_next_populate.stmt, NULL);
}

static bool rr_import_netblockv6_union_next_populate(void)
{
  return rr_db_stmt_execute(s_import.netblockv6_union_next_populate.stmt, NULL);
}

static bool rr_import_netblockv4_union_delete(void)
{
  return rr_db_stmt_execute(s_import.netblockv4_union_delete.stmt, NULL);
}

static bool rr_import_netblockv6_union_delete(void)
{
  return rr_db_stmt_execute(s_import.netblockv6_union_delete.stmt, NULL);
}

static bool rr_import_netblockv4_union_publish(void)
{
  return rr_db_stmt_execute(s_import.netblockv4_union_publish.stmt, NULL);
}

static bool rr_import_netblockv6_union_publish(void)
{
  return rr_db_stmt_execute(s_import.netblockv6_union_publish.stmt, NULL);
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

static bool db_build_list_query_where(ConfigList *cl, RRBuffer *qb)
{
  #define APPEND_OR_FAIL(qb, str) \
    do { \
      if (!rr_buffer_append_str(qb, str)) \
      { \
        LOG_ERROR("out of memory"); \
        return false; \
      } \
    } while (0)

  #define ADD_CONDITION(x, y, z) \
    if (cl->x ##_ ##y.z) \
      for(const char **str = cl->x ##_ ##y.z; *str; ++str, ++conditions) \
        if (!rr_buffer_appendf(qb, "%s" #x "." #y " LIKE '%s'", \
          conditions > 0 ? " OR " : "", \
          *str)) \
        { \
          LOG_ERROR("out of memory"); \
          return false; \
        }\

  bool started = false;
  if (cl->registrar)
  {
    started = true;
    if (!rr_buffer_appendf(qb,
      "(ip.registrar_id = (SELECT id FROM registrar WHERE name = '%s'))",
      cl->registrar))
    {
      LOG_ERROR("out of memory");
      return false;
    }
  }

  if (cl->has_matches)
  {
    if (started)
      APPEND_OR_FAIL(qb, " AND (");
    else
      APPEND_OR_FAIL(qb, "(");

    started = true;
    int conditions = 0;
    APPEND_OR_FAIL(qb, "(");
    #define X(x, y) ADD_CONDITION(x, y, match)
    CONFIG_LIST_FIELDS
    #undef X
    APPEND_OR_FAIL(qb, ")");

    if (cl->has_ignores)
    {
      APPEND_OR_FAIL(qb, " AND NOT (");

      conditions = 0;
      #define X(x, y) ADD_CONDITION(x, y, ignore)
      CONFIG_LIST_FIELDS
      #undef X
      APPEND_OR_FAIL(qb, ")");
    }

    APPEND_OR_FAIL(qb, ")");
  }

  if (cl->sources)
  {
    for(const char **source = cl->sources; *source; ++source)
      for(typeof(g_config.sources) s = g_config.sources; s->name; ++s)
      {
        if (strcmp(s->name, *source) == 0)
        {
          if (started)
            APPEND_OR_FAIL(qb, " OR ");
          started = true;

          rr_alloc_sprintf(qb,
            "(ip.registrar_id = (SELECT id FROM registrar WHERE name = '%s'))",
            s->name);
          break;
        }
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

          if (started)
            APPEND_OR_FAIL(qb, " OR ");
          if (!db_build_list_query_where(l, qb))
            return false;
          started = true;
          break;
        }
      }
  }

  #undef ADD_CONDITION
  #undef APPEND_OR_FAIL
  return true;
}

static RRDBStmt *rr_import_prepare_org_batch(RRDBCon *con)
{
  RRBuffer  sql = { 0 };
  RRDBParam params[RR_IMPORT_BATCH_ROWS * 4];
  size_t    param = 0;

  if (rr_buffer_append_str(&sql,
    "INSERT INTO org_stage (registrar_id, handle, name, descr) VALUES ") < 0)
    goto fail;

  for(size_t i = 0; i < RR_IMPORT_BATCH_ROWS; ++i)
  {
    if (!rr_buffer_appendf(&sql, "%s(?, ?, ?, ?)", i == 0 ? "" : ","))
      goto fail;

    RRDBOrg *row = &s_import.batch.org.rows[i];
    params[param++] = (RRDBParam){ .type = RRDB_TYPE_UINT  , .bind = &row->registrar_id };
    params[param++] = (RRDBParam){ .type = RRDB_TYPE_STRING, .bind = &row->handle       };
    params[param++] = (RRDBParam){ .type = RRDB_TYPE_STRING, .bind = &row->name         };
    params[param++] = (RRDBParam){ .type = RRDB_TYPE_STRING, .bind = &row->descr        };
  }

  if (rr_buffer_append_str(&sql,
    " ON DUPLICATE KEY UPDATE name = VALUES(name), descr = VALUES(descr)") < 0)
    goto fail;

  RRDBStmt *stmt = rr_db_stmt_preparev(con, sql.buffer,
    params, ARRAY_SIZE(params), NULL, 0);
  rr_buffer_free(&sql);
  return stmt;

fail:
  LOG_ERROR("failed to construct the organization batch statement");
  rr_buffer_free(&sql);
  return NULL;
}

static RRDBStmt *rr_import_prepare_netblock_batch(RRDBCon *con, bool ipv6)
{
  RRBuffer  sql = { 0 };
  RRDBParam params[RR_IMPORT_BATCH_ROWS * 7];
  size_t    param = 0;

  if (!rr_buffer_appendf(&sql,
    "INSERT INTO netblock_%s_stage ("
      "registrar_id, org_handle, start_ip, end_ip, prefix_len, netname, descr"
    ") VALUES ",
    ipv6 ? "v6" : "v4"))
    goto fail;

  for(size_t i = 0; i < RR_IMPORT_BATCH_ROWS; ++i)
  {
    if (!rr_buffer_appendf(&sql, "%s(?, ?, ?, ?, ?, ?, ?)", i == 0 ? "" : ","))
      goto fail;

    RRDBNetBlock *row = ipv6
      ? &s_import.batch.ipv6.rows[i]
      : &s_import.batch.ipv4.rows[i];

    params[param++] = (RRDBParam){ .type = RRDB_TYPE_UINT  , .bind = &row->registrar_id };
    params[param++] = (RRDBParam){ .type = RRDB_TYPE_STRING, .bind = &row->org_handle   };
    if (ipv6)
    {
      params[param++] = (RRDBParam)
      {
        .type = RRDB_TYPE_BINARY,
        .bind = &row->startAddr.v6,
        .size = sizeof(row->startAddr.v6)
      };
      params[param++] = (RRDBParam)
      {
        .type = RRDB_TYPE_BINARY,
        .bind = &row->endAddr.v6,
        .size = sizeof(row->endAddr.v6)
      };
    }
    else
    {
      params[param++] = (RRDBParam){ .type = RRDB_TYPE_UINT, .bind = &row->startAddr.v4 };
      params[param++] = (RRDBParam){ .type = RRDB_TYPE_UINT, .bind = &row->endAddr  .v4 };
    }
    params[param++] = (RRDBParam){ .type = RRDB_TYPE_UINT8 , .bind = &row->prefixLen };
    params[param++] = (RRDBParam){ .type = RRDB_TYPE_STRING, .bind = &row->netname   };
    params[param++] = (RRDBParam){ .type = RRDB_TYPE_STRING, .bind = &row->descr     };
  }

  if (rr_buffer_append_str(&sql,
    " ON DUPLICATE KEY UPDATE "
      "prefix_len = VALUES(prefix_len), "
      "netname = VALUES(netname), "
      "descr = VALUES(descr)") < 0)
    goto fail;

  RRDBStmt *stmt = rr_db_stmt_preparev(con, sql.buffer,
    params, ARRAY_SIZE(params), NULL, 0);
  rr_buffer_free(&sql);
  return stmt;

fail:
  LOG_ERROR("failed to construct the IPv%s batch statement", ipv6 ? "6" : "4");
  rr_buffer_free(&sql);
  return NULL;
}

static bool rr_import_batches_prepare(RRDBCon *con)
{
  s_import.batch.org .stmt = rr_import_prepare_org_batch     (con);
  s_import.batch.ipv4.stmt = rr_import_prepare_netblock_batch(con, false);
  s_import.batch.ipv6.stmt = rr_import_prepare_netblock_batch(con, true );
  return
    s_import.batch.org .stmt &&
    s_import.batch.ipv4.stmt &&
    s_import.batch.ipv6.stmt;
}

static void rr_import_batches_free(void)
{
  rr_db_stmt_free(&s_import.batch.org .stmt);
  rr_db_stmt_free(&s_import.batch.ipv4.stmt);
  rr_db_stmt_free(&s_import.batch.ipv6.stmt);
  rr_import_batches_reset();
}

static bool rr_import_lock_acquire(void)
{
  s_import.import_lock_acquire.out_acquired = 0;
  int rc = rr_db_stmt_fetch_one(s_import.import_lock_acquire.stmt);
  if (rc == 1 && s_import.import_lock_acquire.out_acquired != 0)
    return true;

  LOG_ERROR("another RackRadar importer owns the database import lock");
  return false;
}

static void rr_import_lock_release(void)
{
  if (s_import.import_lock_release.stmt &&
      !rr_db_stmt_execute(s_import.import_lock_release.stmt, NULL))
    LOG_ERROR("failed to release the database import lock");
}

static bool db_init_fn(RRDBCon *con, void **udata)
{
  *udata = &s_import;
  STMT_PREPARE(STATEMENTS, *udata);

  if (!rr_import_lock_acquire() || !rr_import_batches_prepare(con))
    return false;

  if (!g_config.lists)
    return true;

  s_import.lists_prepare = calloc(g_config.nbListsActive + 1, sizeof(*s_import.lists_prepare));
  if (!s_import.lists_prepare)
  {
    LOG_ERROR("out of memory");
    return false;
  }

  RRBuffer qb = { .bufferSz = 8192 };

  typeof(s_import.lists_prepare) list = s_import.lists_prepare;
  for(ConfigList *cl = g_config.lists; cl->name; ++cl)
  {
    bool skip = false;
    if (!cl->build_list)
      continue;

    list->cl = cl;
    for(int n = 0; n < 2; ++n)
    {
      const char *ver = n == 0 ? "v4" : "v6";
      rr_buffer_reset(&qb);
      if (!rr_buffer_appendf(&qb,
        "INSERT INTO netblock_%s_list "
        "SELECT "
          "list.id, "
          "ip.id, "
          "ip.start_ip, "
          "ip.end_ip, "
          "ip.prefix_len "
        "FROM "
          "netblock_%s          AS ip "
          "RIGHT JOIN list      AS list      ON list.name    = ? "
          "LEFT  JOIN org       AS org       ON org.id       = ip.org_id "
        "WHERE ",
        ver,
        ver))
      {
        LOG_ERROR("out of memory");
        rr_buffer_free(&qb);
        return false;
      }
      size_t start = qb.pos;

      // reset the seen state
      for(ConfigList *l = g_config.lists; l->name; ++l)
        l->include_seen = false;

      // build the where part of the query
      if (!db_build_list_query_where(cl, &qb))
      {
        LOG_ERROR("out of memory");
        rr_buffer_free(&qb);
        return false;
      }

      // If there was no query built
      if (start == qb.pos)
      {
        LOG_WARN("Skipping invalid list: %s", cl->name);
        skip = true;
        break;
      }

      if (!rr_buffer_append_str(&qb, " ORDER BY ip.start_ip ASC"))
      {
        LOG_ERROR("out of memory");
        rr_buffer_free(&qb);
        return false;
      }

      strncpy(list->in_list_name, cl->name, sizeof(list->in_list_name));
      list->stmt[n] = rr_db_stmt_prepare(con, qb.buffer,
        &(RRDBParam){ .type = RRDB_TYPE_STRING, .bind = &list->in_list_name },
        NULL
      );

      if (!list->stmt[n])
      {
        LOG_ERROR("failed to prepare %s statement for list %s", ver, cl->name);
        continue;
      }
    }

    if (skip)
    {
      rr_db_stmt_free(&list->stmt[0]);
      continue;
    }

    ++list;
  }

  #undef CONFIG_LIST_FIELDS
  rr_buffer_free(&qb);
  return true;
}

static bool db_deinit_fn(RRDBCon *con, void **udata)
{
  rr_import_lock_release();
  rr_import_batches_free();
  STMT_FREE(STATEMENTS, *udata);

  for(typeof(s_import.lists_prepare) list = s_import.lists_prepare; list && list->cl; ++list)
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

typedef struct RRExcludeRangeV4
{
  uint32_t start;
  uint32_t end;
}
RRExcludeRangeV4;

typedef struct RRExcludeRangeV6
{
  unsigned __int128 start;
  unsigned __int128 end;
}
RRExcludeRangeV6;

static int rr_exclude_range_v4_cmp(const void *a, const void *b)
{
  const RRExcludeRangeV4 *left = a;
  const RRExcludeRangeV4 *right = b;
  if (left->start < right->start)
    return -1;
  if (left->start > right->start)
    return 1;
  if (left->end < right->end)
    return -1;
  if (left->end > right->end)
    return 1;
  return 0;
}

static int rr_exclude_range_v6_cmp(const void *a, const void *b)
{
  const RRExcludeRangeV6 *left = a;
  const RRExcludeRangeV6 *right = b;
  if (left->start < right->start)
    return -1;
  if (left->start > right->start)
    return 1;
  if (left->end < right->end)
    return -1;
  if (left->end > right->end)
    return 1;
  return 0;
}

static bool rr_append_exclude_v4_range(RRExcludeRangeV4 **ranges, size_t *count, size_t *capacity,
  uint32_t start, uint32_t end)
{
  if (*count >= *capacity)
  {
    size_t next_capacity = (*capacity == 0) ? 64 : (*capacity * 2);
    RRExcludeRangeV4 *next = realloc(*ranges, next_capacity * sizeof(*next));
    if (!next)
    {
      LOG_ERROR("out of memory");
      return false;
    }
    *ranges = next;
    *capacity = next_capacity;
  }

  (*ranges)[*count] = (RRExcludeRangeV4){ .start = start, .end = end };
  ++(*count);
  return true;
}

static bool rr_append_exclude_v6_range(RRExcludeRangeV6 **ranges, size_t *count, size_t *capacity,
  unsigned __int128 start, unsigned __int128 end)
{
  if (*count >= *capacity)
  {
    size_t next_capacity = (*capacity == 0) ? 64 : (*capacity * 2);
    RRExcludeRangeV6 *next = realloc(*ranges, next_capacity * sizeof(*next));
    if (!next)
    {
      LOG_ERROR("out of memory");
      return false;
    }
    *ranges = next;
    *capacity = next_capacity;
  }

  (*ranges)[*count] = (RRExcludeRangeV6){ .start = start, .end = end };
  ++(*count);
  return true;
}

static size_t rr_merge_exclude_v4_ranges(RRExcludeRangeV4 *ranges, size_t count)
{
  if (count == 0)
    return 0;

  size_t out = 0;
  for (size_t i = 0; i < count; ++i)
  {
    if (out == 0)
    {
      ranges[out++] = ranges[i];
      continue;
    }

    RRExcludeRangeV4 *cur = &ranges[out - 1];
    if ((uint64_t)ranges[i].start <= (uint64_t)cur->end + 1ULL)
    {
      if (ranges[i].end > cur->end)
        cur->end = ranges[i].end;
      continue;
    }

    ranges[out++] = ranges[i];
  }

  return out;
}

static size_t rr_merge_exclude_v6_ranges(RRExcludeRangeV6 *ranges, size_t count)
{
  if (count == 0)
    return 0;

  const unsigned __int128 U128_MAX = (unsigned __int128)-1;
  size_t out = 0;
  for (size_t i = 0; i < count; ++i)
  {
    if (out == 0)
    {
      ranges[out++] = ranges[i];
      continue;
    }

    RRExcludeRangeV6 *cur = &ranges[out - 1];
    bool overlaps = (ranges[i].start <= cur->end);
    bool adjacent = (cur->end != U128_MAX) && (ranges[i].start == cur->end + 1);
    if (overlaps || adjacent)
    {
      if (ranges[i].end > cur->end)
        cur->end = ranges[i].end;
      continue;
    }

    ranges[out++] = ranges[i];
  }

  return out;
}

static bool rr_collect_exclude_v4_ranges(RRDBCon *con, const ConfigList *cl, RRExcludeRangeV4 **out_ranges, size_t *out_count)
{
  *out_ranges = NULL;
  *out_count = 0;
  if (!cl || !cl->exclude)
    return true;

  size_t capacity = 0;
  for (const char **exclude = cl->exclude; *exclude; ++exclude)
  {
    unsigned exclude_list_id;
    int rc = rr_query_list_by_name(con, *exclude, &exclude_list_id);
    if (rc == 0)
    {
      LOG_WARN("Exclude list not found: %s", *exclude);
      continue;
    }
    if (rc < 0)
      return false;

    if (!rr_query_netblockv4_list_union_start(con, exclude_list_id, true))
      return false;

    uint32_t ip;
    uint8_t prefix_len;
    while ((rc = rr_query_netblockv4_list_union_fetch(con, &ip, &prefix_len)) == 1)
    {
      uint32_t end_ip = ip;
      if (prefix_len == 0)
      {
        ip = 0;
        end_ip = UINT32_MAX;
      }
      else if (prefix_len < 32)
      {
        uint32_t mask = UINT32_MAX << (32u - prefix_len);
        uint32_t network = ip & mask;
        ip = network;
        end_ip = network | ~mask;
      }

      if (!rr_append_exclude_v4_range(out_ranges, out_count, &capacity, ip, end_ip))
      {
        rr_query_netblockv4_list_union_end(con);
        return false;
      }
    }

    rr_query_netblockv4_list_union_end(con);
    if (rc < 0)
      return false;
  }

  if (*out_count == 0)
    return true;

  qsort(*out_ranges, *out_count, sizeof(**out_ranges), rr_exclude_range_v4_cmp);
  *out_count = rr_merge_exclude_v4_ranges(*out_ranges, *out_count);
  return true;
}

static bool rr_collect_exclude_v6_ranges(RRDBCon *con, const ConfigList *cl, RRExcludeRangeV6 **out_ranges, size_t *out_count)
{
  *out_ranges = NULL;
  *out_count = 0;
  if (!cl || !cl->exclude)
    return true;

  size_t capacity = 0;
  for (const char **exclude = cl->exclude; *exclude; ++exclude)
  {
    unsigned exclude_list_id;
    int rc = rr_query_list_by_name(con, *exclude, &exclude_list_id);
    if (rc == 0)
    {
      LOG_WARN("Exclude list not found: %s", *exclude);
      continue;
    }
    if (rc < 0)
      return false;

    if (!rr_query_netblockv6_list_union_start(con, exclude_list_id, true))
      return false;

    unsigned __int128 ip;
    uint8_t prefix_len;
    while ((rc = rr_query_netblockv6_list_union_fetch(con, &ip, &prefix_len)) == 1)
    {
      unsigned __int128 end_ip = ip;
      if (prefix_len == 0)
      {
        ip = 0;
        end_ip = (unsigned __int128)-1;
      }
      else if (prefix_len < 128)
      {
        unsigned __int128 ip_be = rr_raw_to_be(ip);
        unsigned __int128 mask = ((unsigned __int128)-1) << (128u - prefix_len);
        unsigned __int128 network = ip_be & mask;
        ip = rr_be_to_raw(network);
        end_ip = rr_be_to_raw(network | ~mask);
      }

      if (!rr_append_exclude_v6_range(out_ranges, out_count, &capacity, rr_raw_to_be(ip), rr_raw_to_be(end_ip)))
      {
        rr_query_netblockv6_list_union_end(con);
        return false;
      }
    }

    rr_query_netblockv6_list_union_end(con);
    if (rc < 0)
      return false;
  }

  if (*out_count == 0)
    return true;

  qsort(*out_ranges, *out_count, sizeof(**out_ranges), rr_exclude_range_v6_cmp);
  *out_count = rr_merge_exclude_v6_ranges(*out_ranges, *out_count);
  return true;
}

static bool rr_emit_ipv4_range_with_excludes(unsigned list_id, uint32_t start, uint32_t end,
  const RRExcludeRangeV4 *excludes, size_t exclude_count, size_t *exclude_index)
{
  uint32_t cur = start;

  while (cur <= end)
  {
    while (*exclude_index < exclude_count && excludes[*exclude_index].end < cur)
      ++(*exclude_index);

    if (*exclude_index >= exclude_count || excludes[*exclude_index].start > end)
      return rr_emit_ipv4_range_as_cidrs(list_id, cur, end);

    if (excludes[*exclude_index].start > cur)
    {
      uint32_t chunk_end = excludes[*exclude_index].start - 1;
      if (!rr_emit_ipv4_range_as_cidrs(list_id, cur, chunk_end))
        return false;
    }

    if (excludes[*exclude_index].end >= end)
      break;

    if (excludes[*exclude_index].end == UINT32_MAX)
      break;

    cur = excludes[*exclude_index].end + 1;
    if (cur == 0)
      break;
  }

  return true;
}

static bool rr_import_netblockv4_list_union_populate(RRDBCon *con, const ConfigList *cl, unsigned list_id)
{
  uint32_t start_ip, end_ip;
  uint8_t  prefix_len;
  uint32_t run_start = 0, run_end = 0;
  bool     have_run = false;

  RRExcludeRangeV4 *exclude_ranges = NULL;
  size_t exclude_count = 0;
  size_t exclude_index = 0;
  if (!rr_collect_exclude_v4_ranges(con, cl, &exclude_ranges, &exclude_count))
    return false;

  if (!rr_query_netblockv4_list_start(con, list_id, true))
  {
    free(exclude_ranges);
    return false;
  }

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

    bool emitted = exclude_count == 0
      ? rr_emit_ipv4_range_as_cidrs(list_id, run_start, run_end)
      : rr_emit_ipv4_range_with_excludes(list_id, run_start, run_end, exclude_ranges, exclude_count, &exclude_index);

    if (!emitted)
    {
      rr_query_netblockv4_list_end(con);
      free(exclude_ranges);
      return false;
    }

    run_start = start_ip;
    run_end   = end_ip;
  }

  if (have_run)
  {
    bool emitted = exclude_count == 0
      ? rr_emit_ipv4_range_as_cidrs(list_id, run_start, run_end)
      : rr_emit_ipv4_range_with_excludes(list_id, run_start, run_end, exclude_ranges, exclude_count, &exclude_index);

    if (!emitted)
    {
      rr_query_netblockv4_list_end(con);
      free(exclude_ranges);
      return false;
    }
  }

  if (rc < 0)
  {
    rr_query_netblockv4_list_end(con);
    free(exclude_ranges);
    return false;
  }

  rr_query_netblockv4_list_end(con);
  free(exclude_ranges);
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

static bool rr_emit_ipv6_range_with_excludes(unsigned list_id, unsigned __int128 start_be, unsigned __int128 end_be,
  const RRExcludeRangeV6 *excludes, size_t exclude_count, size_t *exclude_index)
{
  const unsigned __int128 U128_MAX = (unsigned __int128)-1;
  unsigned __int128 cur = start_be;

  while (cur <= end_be)
  {
    while (*exclude_index < exclude_count && excludes[*exclude_index].end < cur)
      ++(*exclude_index);

    if (*exclude_index >= exclude_count || excludes[*exclude_index].start > end_be)
      return rr_emit_ipv6_range_as_cidrs(list_id, rr_be_to_raw(cur), rr_be_to_raw(end_be));

    if (excludes[*exclude_index].start > cur)
    {
      unsigned __int128 chunk_end = excludes[*exclude_index].start - 1;
      if (!rr_emit_ipv6_range_as_cidrs(list_id, rr_be_to_raw(cur), rr_be_to_raw(chunk_end)))
        return false;
    }

    if (excludes[*exclude_index].end >= end_be)
      break;

    if (excludes[*exclude_index].end == U128_MAX)
      break;

    cur = excludes[*exclude_index].end + 1;
    if (cur == 0)
      break;
  }

  return true;
}

static bool rr_import_netblockv6_list_union_populate(RRDBCon *con, const ConfigList *cl, unsigned list_id)
{
  unsigned __int128 start_raw, end_raw;
  uint8_t  prefix_len;

  const unsigned __int128 U128_MAX = (unsigned __int128)-1;

  unsigned __int128 run_start = 0, run_end = 0; // BE-numeric domain
  bool have_run = false;

  RRExcludeRangeV6 *exclude_ranges = NULL;
  size_t exclude_count = 0;
  size_t exclude_index = 0;
  if (!rr_collect_exclude_v6_ranges(con, cl, &exclude_ranges, &exclude_count))
    return false;

  if (!rr_query_netblockv6_list_start(con, list_id, true))
  {
    free(exclude_ranges);
    return false;
  }

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

    bool emitted = exclude_count == 0
      ? rr_emit_ipv6_range_as_cidrs(list_id, rr_be_to_raw(run_start), rr_be_to_raw(run_end))
      : rr_emit_ipv6_range_with_excludes(list_id, run_start, run_end, exclude_ranges, exclude_count, &exclude_index);

    if (!emitted)
    {
      rr_query_netblockv6_list_end(con);
      free(exclude_ranges);
      return false;
    }

    run_start = start;
    run_end   = end;
  }

  if (have_run)
  {
    bool emitted = exclude_count == 0
      ? rr_emit_ipv6_range_as_cidrs(list_id, rr_be_to_raw(run_start), rr_be_to_raw(run_end))
      : rr_emit_ipv6_range_with_excludes(list_id, run_start, run_end, exclude_ranges, exclude_count, &exclude_index);

    if (!emitted)
    {
      rr_query_netblockv6_list_end(con);
      free(exclude_ranges);
      return false;
    }
  }

  rr_query_netblockv6_list_end(con);
  free(exclude_ranges);
  return (rc >= 0);
}

static bool rr_import_build_lists_internal(RRDBCon *con)
{
  if (!g_config.lists)
    return true;

  LOG_INFO("rebuilding lists");
  for(typeof(s_import.lists_prepare) list = s_import.lists_prepare; list->stmt[0] && list->stmt[1]; ++list)
  {
    LOG_INFO("  Building: %s", list->cl->name);
    unsigned list_id;
    if (
      !rr_db_start(con) ||
      !rr_import_list_insert(list->cl->name) ||
      rr_query_list_by_name(con, list->cl->name, &list_id) != 1 ||
      !rr_import_netblockv4_list_delete(list_id) ||
      !rr_import_netblockv6_list_delete(list_id) ||
      !rr_db_stmt_execute(list->stmt[0], NULL) ||
      !rr_db_stmt_execute(list->stmt[1], NULL) ||
      !rr_import_netblockv4_list_union_delete  (list_id) ||
      !rr_import_netblockv6_list_union_delete  (list_id) ||
      !rr_import_netblockv4_list_union_populate(con, list->cl, list_id) ||
      !rr_import_netblockv6_list_union_populate(con, list->cl, list_id) ||
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
  bool check_unions    = true;
  while(true)
  {
    RRDBCon *con = s_import.con;
    if (!rr_db_get(&con))
    {
      LOG_ERROR("failed to get the reserved connection");
      goto fail;
    }

    if (check_unions)
    {
      rc = rr_import_unions_dirty(&rebuild_unions);
      if (rc != 1)
      {
        LOG_ERROR("failed to read the import state");
        goto fail_con;
      }
      check_unions = false;
    }

    for(unsigned i = 0; i < g_config.nbSources; ++i)
    {
      typeof(*g_config.sources) *src = &g_config.sources[i];
      if (src->type == SOURCE_TYPE_INVALID)
        continue;

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
          continue;
        }
        LOG_INFO("New registrar inserted");
      }

      if (last_import > 0 && time(NULL) - last_import < src->frequency)
        continue;

      LOG_INFO("Fetching source: %s", src->name);
      if (src->user && src->pass)
        rr_download_set_auth(s_import.dl, src->user, src->pass);
      else
        rr_download_clear_auth(s_import.dl);

      FILE *fp;
      if (!rr_download_to_tmpfile(s_import.dl, src->url, &fp))
      {
        LOG_ERROR("failed fetch for %s", src->name);
        continue;
      }

      if (fseek(fp, 0, SEEK_SET) != 0)
      {
        LOG_ERROR("fseek 0 failed");
        fclose(fp);
        continue;
      }

      rr_import_batches_reset();
      if (!rr_import_stages_truncate())
      {
        LOG_ERROR("failed to clear the import staging tables");
        fclose(fp);
        goto fail_con;
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

        case SOURCE_TYPE_JSON:
          success = rr_json_import_FILE(src->name, fp, registrar_id, serial,
            src->extra_v4, src->extra_v6);
          break;

        case SOURCE_TYPE_REGEX:
          success = rr_regex_import_FILE(src->name, fp, registrar_id, serial,
            src->extra_v4, src->extra_v6);
          break;

        default:
          assert(false);
      }

      if (success)
        success = rr_import_batches_flush();
      fclose(fp);

      const char *resultStr;
      if (success)
      {
        if (!rr_db_start(con))
          goto fail_con;

        unsigned locked_registrar_id;
        unsigned locked_serial;
        unsigned locked_last_import;
        rc = rr_import_registrar_lock(src->name,
          &locked_registrar_id,
          &locked_serial,
          &locked_last_import);

        if (rc != 1)
        {
          LOG_ERROR("failed to lock registrar %s", src->name);
          if (!rr_db_rollback(con))
            goto fail_con;
          rr_import_stats_rollback();
          resultStr = "failed";
          goto log_result;
        }

        if (locked_registrar_id != registrar_id ||
            locked_serial       != serial - 1 ||
            locked_last_import  != last_import)
        {
          LOG_WARN("registrar %s changed while its source was being staged", src->name);
          if (!rr_db_rollback(con))
            goto fail_con;
          rr_import_stats_rollback();
          resultStr = "superseded";
          goto log_result;
        }

        //finalize the registrar
        LOG_INFO("merging staged import");
        if (
          !rr_import_org_merge_insert       (registrar_id, serial) ||
          !rr_import_org_merge_update       (registrar_id, serial) ||
          !rr_import_netblockv4_merge_insert(registrar_id, serial) ||
          !rr_import_netblockv4_merge_update(registrar_id, serial) ||
          !rr_import_netblockv6_merge_insert(registrar_id, serial) ||
          !rr_import_netblockv6_merge_update(registrar_id, serial) ||
          !rr_import_netblockv4_delete_old  (registrar_id) ||
          !rr_import_netblockv6_delete_old  (registrar_id) ||
          !rr_import_netblockv4_link_org    (registrar_id) ||
          !rr_import_netblockv6_link_org    (registrar_id) ||
          !rr_import_org_delete_old         (registrar_id) ||
          !rr_import_unions_mark_dirty      () ||
          !rr_import_registrar_update_serial(registrar_id, serial) ||
          !rr_db_commit                     (con))
        {
          LOG_ERROR("failed to finalize");
          if (!rr_db_rollback(con))
            goto fail_con;
          rr_import_stats_rollback();
          resultStr = "failed";
          goto log_result;
        }

        resultStr = "succeeded";
        rebuild_unions = true;
        rebuild_lists  = true;
      }
      else
      {
        rr_import_batches_reset();
        resultStr = "failed";
      }

log_result:
      ;
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
      LOG_INFO("  Parsed   : %llu", s_import.stats.processedOrgs);
      LOG_INFO("  New      : %llu", s_import.stats.newOrgs      );
      LOG_INFO("  Updated  : %llu", s_import.stats.updatedOrgs  );
      LOG_INFO("  Deleted  : %llu", s_import.stats.deletedOrgs  );
      LOG_INFO("IPv4:");
      LOG_INFO("  Parsed   : %llu", s_import.stats.processedIPv4);
      LOG_INFO("  New      : %llu", s_import.stats.newIPv4      );
      LOG_INFO("  Updated  : %llu", s_import.stats.updatedIPv4  );
      LOG_INFO("  Deleted  : %llu", s_import.stats.deletedIPv4  );
      LOG_INFO("IPv6:");
      LOG_INFO("  Parsed   : %llu", s_import.stats.processedIPv6);
      LOG_INFO("  New      : %llu", s_import.stats.newIPv6      );
      LOG_INFO("  Updated  : %llu", s_import.stats.updatedIPv6  );
      LOG_INFO("  Deleted  : %llu", s_import.stats.deletedIPv6  );
    }

    if (rebuild_unions)
    {
      LOG_INFO("building union snapshot");
      if (
        !rr_import_netblockv4_union_next_truncate() ||
        !rr_import_netblockv6_union_next_truncate() ||
        !rr_import_netblockv4_union_next_populate() ||
        !rr_import_netblockv6_union_next_populate())
      {
        LOG_ERROR("failed");
        goto fail_con;
      }

      LOG_INFO("publishing union snapshot");
      if (
        !rr_db_start                         (con) ||
        !rr_import_netblockv4_union_delete  () ||
        !rr_import_netblockv6_union_delete  () ||
        !rr_import_netblockv4_union_publish () ||
        !rr_import_netblockv6_union_publish () ||
        !rr_import_unions_mark_clean         () ||
        !rr_db_commit                        (con))
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
