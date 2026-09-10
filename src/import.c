#include "import.h"
#include "config.h"
#include "util.h"
#include "log.h"
#include "download.h"
#include "db.h"
#include "query.h"
#include "query_macros.h"
#include "sha256.h"

#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include <signal.h>

#define RR_IMPORT_BATCH_ROWS            64
#define RR_IMPORT_LIST_UNION_BATCH_ROWS 256
#define RR_IMPORT_LIST_VERSION          "RackRadar-list-builder-v2"
#define RR_IMPORT_PARSER_VERSION        "RackRadar-source-parser-v3"

typedef struct RRImportSourceState
{
  char               configHash [RR_SHA256_HEX_SIZE];
  char               contentHash[RR_SHA256_HEX_SIZE];
  RRDownloadMetadata download;
}
RRImportSourceState;

typedef struct RRImportRegistrar
{
  unsigned            id;
  unsigned            serial;
  unsigned            lastImport;
  unsigned            lastCheck;
  unsigned            databaseTime;
  char                checkConfigHash[RR_SHA256_HEX_SIZE];
  RRImportSourceState source;
}
RRImportRegistrar;

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

typedef struct RRImportListUnionV4
{
  unsigned listId;
  unsigned ip;
  uint8_t  prefixLen;
}
RRImportListUnionV4;

typedef struct RRImportListUnionV6
{
  unsigned          listId;
  unsigned __int128 ip;
  uint8_t           prefixLen;
}
RRImportListUnionV6;

typedef struct RRImportListUnionBatch
{
  struct
  {
    RRDBStmt           *stmt;
    RRImportListUnionV4 rows[RR_IMPORT_LIST_UNION_BATCH_ROWS];
    size_t              count;
  }
  ipv4;

  struct
  {
    RRDBStmt           *stmt;
    RRImportListUnionV6 rows[RR_IMPORT_LIST_UNION_BATCH_ROWS];
    size_t              count;
  }
  ipv6;
}
RRImportListUnionBatch;

typedef enum RRImportListState
{
  RR_IMPORT_LIST_PENDING,
  RR_IMPORT_LIST_BUILDING,
  RR_IMPORT_LIST_BUILT
}
RRImportListState;

typedef struct RRImportList
{
  ConfigList        *cl;
  char               in_list_name[32];
  RRDBStmt          *stmt[2];
  RRImportListState  state;
}
RRImportList;

typedef struct RRImport
{
  RRDownload             *dl;
  RRDBCon                *con;
  RRDBStatistics          stats;
  RRImportBatch           batch;
  RRImportListUnionBatch  listUnionBatch;
  bool                    lockHeld;

  STMT_STRUCT(registrar_insert,
    char in_name[32];
  );

  STMT_STRUCT(registrar_update_serial,
    unsigned            in_registrar_id;
    unsigned            in_serial;
    RRImportSourceState in_source;
  );

  STMT_STRUCT(registrar_get,
    char              in_name[32];
    RRImportRegistrar out;
  );

  STMT_STRUCT(registrar_update_check,
    unsigned            in_registrar_id;
    RRImportSourceState in_source;
  );

  STMT_STRUCT(registrar_update_attempt,
    unsigned in_registrar_id;
    char     in_config_hash[RR_SHA256_HEX_SIZE];
  );

  STMT_STRUCT(registrar_lock,
    char     in_name[32];
    unsigned out_registrar_id;
    unsigned out_serial;
    unsigned out_last_import;
    unsigned out_last_check;
  );

  STMT_STRUCT(import_lock_acquire,
    uint8_t out_acquired;
  );

  STMT_STRUCT(import_lock_release,
    uint8_t out_released;
  );

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

  STMT_STRUCT(import_state_mark_changed,
    uint8_t in_unions_dirty;
  );

  STMT_STRUCT(list_state_get,
    unsigned long long out_data_generation;
    unsigned long long out_list_generation;
    char               out_config_hash[RR_SHA256_HEX_SIZE];
  );

  STMT_STRUCT(list_state_mark_current,
    char in_config_hash[RR_SHA256_HEX_SIZE];
  );

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

  RRImportList *lists_prepare;
}
RRImport;
RRImport s_import = { 0 };
static volatile sig_atomic_t s_import_stop_requested;

static void rr_import_list_config_hash(char out_hash[RR_SHA256_HEX_SIZE]);

static void rr_import_log_timing(
  const char *phase,
  const char *subject,
  uint64_t    started)
{
  const uint64_t elapsed = rr_microtime() - started;
  const uint64_t sec     = elapsed / 1000000UL;
  const uint64_t us      = elapsed % 1000000UL;

  LOG_INFO("timing: %s %s took %02u:%02u:%02u.%03u",
    phase,
    subject,
    (unsigned)(sec / 60 / 60),
    (unsigned)(sec / 60 % 60),
    (unsigned)(sec % 60),
    (unsigned)(us / 1000));
}

#define STATEMENTS(X) \
  X(registrar_insert              ) \
  X(registrar_update_serial       ) \
  X(registrar_get                 ) \
  X(registrar_update_check        ) \
  X(registrar_update_attempt      ) \
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
  X(import_state_mark_changed     ) \
  X(list_state_get                ) \
  X(list_state_mark_current       ) \
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
  "UPDATE registrar SET "
    "serial = ?, "
    "last_import = UNIX_TIMESTAMP(), "
    "last_check = UNIX_TIMESTAMP(), "
    "source_config_hash = ?, "
    "source_check_config_hash = ?, "
    "source_content_hash = ?, "
    "source_etag = ?, "
    "source_last_modified = ? "
  "WHERE id = ?",
  &(RRDBParam){ .type = RRDB_TYPE_UINT  , .bind = &this->in_serial                      },
  &(RRDBParam){ .type = RRDB_TYPE_STRING, .bind =  this->in_source.configHash           },
  &(RRDBParam){ .type = RRDB_TYPE_STRING, .bind =  this->in_source.configHash           },
  &(RRDBParam){ .type = RRDB_TYPE_STRING, .bind =  this->in_source.contentHash          },
  &(RRDBParam){ .type = RRDB_TYPE_STRING, .bind =  this->in_source.download.etag         },
  &(RRDBParam){ .type = RRDB_TYPE_STRING, .bind =  this->in_source.download.lastModified },
  &(RRDBParam){ .type = RRDB_TYPE_UINT  , .bind = &this->in_registrar_id                }
);

DEFAULT_STMT(RRImport, registrar_get,
  "SELECT "
    "id, serial, last_import, last_check, "
    "source_check_config_hash, "
    "source_config_hash, source_content_hash, source_etag, "
    "source_last_modified, UNIX_TIMESTAMP() "
  "FROM registrar WHERE name = ?",
  &(RRDBParam){ .type = RRDB_TYPE_STRING, .bind = this->in_name },
  RRDB_PARAM_OUT,
  &(RRDBParam){ .type = RRDB_TYPE_UINT  , .bind = &this->out.id                                           },
  &(RRDBParam){ .type = RRDB_TYPE_UINT  , .bind = &this->out.serial                                       },
  &(RRDBParam){ .type = RRDB_TYPE_UINT  , .bind = &this->out.lastImport                                   },
  &(RRDBParam){ .type = RRDB_TYPE_UINT  , .bind = &this->out.lastCheck                                    },
  &(RRDBParam)
  {
    .type = RRDB_TYPE_STRING,
    .bind = this->out.checkConfigHash,
    .size = sizeof(this->out.checkConfigHash)
  },
  &(RRDBParam)
  {
    .type = RRDB_TYPE_STRING,
    .bind = this->out.source.configHash,
    .size = sizeof(this->out.source.configHash)
  },
  &(RRDBParam)
  {
    .type = RRDB_TYPE_STRING,
    .bind = this->out.source.contentHash,
    .size = sizeof(this->out.source.contentHash)
  },
  &(RRDBParam)
  {
    .type = RRDB_TYPE_STRING,
    .bind = this->out.source.download.etag,
    .size = sizeof(this->out.source.download.etag)
  },
  &(RRDBParam)
  {
    .type = RRDB_TYPE_STRING,
    .bind = this->out.source.download.lastModified,
    .size = sizeof(this->out.source.download.lastModified)
  },
  &(RRDBParam){ .type = RRDB_TYPE_UINT, .bind = &this->out.databaseTime }
);

DEFAULT_STMT(RRImport, registrar_update_check,
  "UPDATE registrar SET "
    "last_check = UNIX_TIMESTAMP(), "
    "source_config_hash = ?, "
    "source_check_config_hash = ?, "
    "source_content_hash = ?, "
    "source_etag = ?, "
    "source_last_modified = ? "
  "WHERE id = ?",
  &(RRDBParam){ .type = RRDB_TYPE_STRING, .bind =  this->in_source.configHash           },
  &(RRDBParam){ .type = RRDB_TYPE_STRING, .bind =  this->in_source.configHash           },
  &(RRDBParam){ .type = RRDB_TYPE_STRING, .bind =  this->in_source.contentHash          },
  &(RRDBParam){ .type = RRDB_TYPE_STRING, .bind =  this->in_source.download.etag         },
  &(RRDBParam){ .type = RRDB_TYPE_STRING, .bind =  this->in_source.download.lastModified },
  &(RRDBParam){ .type = RRDB_TYPE_UINT  , .bind = &this->in_registrar_id                }
);

DEFAULT_STMT(RRImport, registrar_update_attempt,
  "UPDATE registrar SET "
    "last_check = UNIX_TIMESTAMP(), "
    "source_check_config_hash = ? "
  "WHERE id = ?",
  &(RRDBParam){ .type = RRDB_TYPE_STRING, .bind =  this->in_config_hash   },
  &(RRDBParam){ .type = RRDB_TYPE_UINT  , .bind = &this->in_registrar_id }
);

DEFAULT_STMT(RRImport, registrar_lock,
  "SELECT id, serial, last_import, last_check "
  "FROM registrar WHERE name = ? FOR UPDATE",
  &(RRDBParam){ .type = RRDB_TYPE_STRING, .bind = this->in_name },
  RRDB_PARAM_OUT,
  &(RRDBParam){ .type = RRDB_TYPE_UINT, .bind = &this->out_registrar_id },
  &(RRDBParam){ .type = RRDB_TYPE_UINT, .bind = &this->out_serial       },
  &(RRDBParam){ .type = RRDB_TYPE_UINT, .bind = &this->out_last_import  },
  &(RRDBParam){ .type = RRDB_TYPE_UINT, .bind = &this->out_last_check   }
);

DEFAULT_STMT(RRImport, import_lock_acquire,
  "SELECT COALESCE(GET_LOCK(SHA2(CONCAT('RackRadar:import:', DATABASE()), 256), 0), 0)",
  RRDB_PARAM_OUT,
  &(RRDBParam){ .type = RRDB_TYPE_UINT8, .bind = &this->out_acquired }
);

DEFAULT_STMT(RRImport, import_lock_release,
  "SELECT COALESCE(RELEASE_LOCK(SHA2(CONCAT('RackRadar:import:', DATABASE()), 256)), 0)",
  RRDB_PARAM_OUT,
  &(RRDBParam){ .type = RRDB_TYPE_UINT8, .bind = &this->out_released }
);

DEFAULT_STMT(RRImport, org_insert,
  "INSERT INTO org_stage ("
    "registrar_id, "
    "handle, "
    "name, "
    "descr, "
    "email"
  ") VALUES ("
    "?,"
    "?,"
    "?,"
    "?,"
    "?"
  ") ON DUPLICATE KEY UPDATE "
    "name   = VALUES(name), "
    "descr  = VALUES(descr), "
    "email  = VALUES(email)",

  &(RRDBParam){ .type = RRDB_TYPE_UINT  , .bind = &this->in.registrar_id },
  &(RRDBParam){ .type = RRDB_TYPE_STRING, .bind = &this->in.handle       },
  &(RRDBParam){ .type = RRDB_TYPE_STRING, .bind = &this->in.name         },
  &(RRDBParam){ .type = RRDB_TYPE_STRING, .bind = &this->in.descr        },
  &(RRDBParam){ .type = RRDB_TYPE_STRING, .bind = &this->in.email        }
);

DEFAULT_STMT(RRImport, org_stage_truncate,
  "TRUNCATE TABLE org_stage"
);

DEFAULT_STMT(RRImport, org_merge_insert,
  "INSERT INTO org (registrar_id, serial, handle, name, descr, email) "
  "SELECT s.registrar_id, ?, s.handle, s.name, s.descr, s.email "
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
  "SET o.serial = ?, o.name = s.name, o.descr = s.descr, o.email = s.email "
  "WHERE o.registrar_id = ? "
  "AND ("
    "NOT (CAST(o.name AS BINARY) <=> CAST(s.name AS BINARY)) OR "
    "NOT (CAST(o.descr AS BINARY) <=> CAST(s.descr AS BINARY)) OR "
    "NOT (CAST(o.email AS BINARY) <=> CAST(s.email AS BINARY))"
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
    "descr, "
    "email"
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
    "prefix_len = VALUES(prefix_len), "
    "netname = VALUES(netname), "
    "descr   = VALUES(descr), "
    "email   = VALUES(email)",

  &(RRDBParam){ .type = RRDB_TYPE_UINT  , .bind = &this->in.registrar_id },
  &(RRDBParam){ .type = RRDB_TYPE_STRING, .bind = &this->in.org_handle   },
  &(RRDBParam){ .type = RRDB_TYPE_UINT  , .bind = &this->in.startAddr.v4 },
  &(RRDBParam){ .type = RRDB_TYPE_UINT  , .bind = &this->in.endAddr  .v4 },
  &(RRDBParam){ .type = RRDB_TYPE_UINT8 , .bind = &this->in.prefixLen    },
  &(RRDBParam){ .type = RRDB_TYPE_STRING, .bind = &this->in.netname      },
  &(RRDBParam){ .type = RRDB_TYPE_STRING, .bind = &this->in.descr        },
  &(RRDBParam){ .type = RRDB_TYPE_STRING, .bind = &this->in.email        }
);

DEFAULT_STMT(RRImport, netblockv4_stage_truncate,
  "TRUNCATE TABLE netblock_v4_stage"
);

DEFAULT_STMT(RRImport, netblockv4_merge_insert,
  "INSERT INTO netblock_v4 ("
    "registrar_id, serial, org_handle, start_ip, end_ip, prefix_len, netname, descr, email"
  ") "
  "SELECT s.registrar_id, ?, s.org_handle, s.start_ip, s.end_ip, "
    "s.prefix_len, s.netname, s.descr, s.email "
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
    "nb.descr = s.descr, "
    "nb.email = s.email "
  "WHERE nb.registrar_id = ? "
  "AND ("
    "nb.prefix_len != s.prefix_len OR "
    "NOT (CAST(nb.netname AS BINARY) <=> CAST(s.netname AS BINARY)) OR "
    "NOT (CAST(nb.descr AS BINARY) <=> CAST(s.descr AS BINARY)) OR "
    "NOT (CAST(nb.email AS BINARY) <=> CAST(s.email AS BINARY))"
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
    "descr, "
    "email"
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
    "prefix_len = VALUES(prefix_len), "
    "netname = VALUES(netname), "
    "descr   = VALUES(descr), "
    "email   = VALUES(email)",

  &(RRDBParam){ .type = RRDB_TYPE_UINT  , .bind = &this->in.registrar_id },
  &(RRDBParam){ .type = RRDB_TYPE_STRING, .bind = &this->in.org_handle   },
  &(RRDBParam){ .type = RRDB_TYPE_BINARY, .bind = &this->in.startAddr.v6, .size = sizeof(this->in.startAddr) },
  &(RRDBParam){ .type = RRDB_TYPE_BINARY, .bind = &this->in.endAddr  .v6, .size = sizeof(this->in.endAddr  ) },
  &(RRDBParam){ .type = RRDB_TYPE_UINT8 , .bind = &this->in.prefixLen    },
  &(RRDBParam){ .type = RRDB_TYPE_STRING, .bind = &this->in.netname      },
  &(RRDBParam){ .type = RRDB_TYPE_STRING, .bind = &this->in.descr        },
  &(RRDBParam){ .type = RRDB_TYPE_STRING, .bind = &this->in.email        }
);

DEFAULT_STMT(RRImport, netblockv6_stage_truncate,
  "TRUNCATE TABLE netblock_v6_stage"
);

DEFAULT_STMT(RRImport, netblockv6_merge_insert,
  "INSERT INTO netblock_v6 ("
    "registrar_id, serial, org_handle, start_ip, end_ip, prefix_len, netname, descr, email"
  ") "
  "SELECT s.registrar_id, ?, s.org_handle, s.start_ip, s.end_ip, "
    "s.prefix_len, s.netname, s.descr, s.email "
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
    "nb.descr = s.descr, "
    "nb.email = s.email "
  "WHERE nb.registrar_id = ? "
  "AND ("
    "nb.prefix_len != s.prefix_len OR "
    "NOT (CAST(nb.netname AS BINARY) <=> CAST(s.netname AS BINARY)) OR "
    "NOT (CAST(nb.descr AS BINARY) <=> CAST(s.descr AS BINARY)) OR "
    "NOT (CAST(nb.email AS BINARY) <=> CAST(s.email AS BINARY))"
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

DEFAULT_STMT(RRImport, import_state_mark_changed,
  "UPDATE import_state SET "
    "unions_dirty = IF(? != 0, 1, unions_dirty), "
    "data_generation = data_generation + 1 "
  "WHERE id = 1",
  &(RRDBParam){ .type = RRDB_TYPE_UINT8, .bind = &this->in_unions_dirty }
);

DEFAULT_STMT(RRImport, list_state_get,
  "SELECT data_generation, list_generation, list_config_hash "
  "FROM import_state WHERE id = 1",
  RRDB_PARAM_OUT,
  &(RRDBParam){ .type = RRDB_TYPE_UBIGINT, .bind = &this->out_data_generation },
  &(RRDBParam){ .type = RRDB_TYPE_UBIGINT, .bind = &this->out_list_generation },
  &(RRDBParam)
  {
    .type = RRDB_TYPE_STRING,
    .bind = this->out_config_hash,
    .size = sizeof(this->out_config_hash)
  }
);

DEFAULT_STMT(RRImport, list_state_mark_current,
  "UPDATE import_state SET "
    "list_generation = data_generation, "
    "list_config_hash = ? "
  "WHERE id = 1",
  &(RRDBParam){ .type = RRDB_TYPE_STRING, .bind = this->in_config_hash }
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
  snprintf(s_import.registrar_insert.in_name,
    sizeof(s_import.registrar_insert.in_name), "%s", in_name);
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

static bool rr_import_registrar_update_serial(
  unsigned                   in_registrar_id,
  unsigned                   in_serial,
  const RRImportSourceState *in_source)
{
  s_import.registrar_update_serial.in_registrar_id = in_registrar_id;
  s_import.registrar_update_serial.in_serial       = in_serial;
  s_import.registrar_update_serial.in_source       = *in_source;
  return rr_db_stmt_execute(s_import.registrar_update_serial.stmt, NULL);
}

static int rr_import_registrar_get(
  const char        *in_name,
  RRImportRegistrar *out_registrar)
{
  snprintf(s_import.registrar_get.in_name,
    sizeof(s_import.registrar_get.in_name), "%s", in_name);

  int rc = rr_db_stmt_fetch_one(s_import.registrar_get.stmt);
  if (rc == 1)
    *out_registrar = s_import.registrar_get.out;
  return rc;
}

static bool rr_import_registrar_update_check(
  unsigned                   in_registrar_id,
  const RRImportSourceState *in_source)
{
  s_import.registrar_update_check.in_registrar_id = in_registrar_id;
  s_import.registrar_update_check.in_source       = *in_source;
  return rr_db_stmt_execute(s_import.registrar_update_check.stmt, NULL);
}

static bool rr_import_registrar_update_attempt(
  unsigned in_registrar_id, const char *in_config_hash)
{
  s_import.registrar_update_attempt.in_registrar_id = in_registrar_id;
  snprintf(s_import.registrar_update_attempt.in_config_hash,
    sizeof(s_import.registrar_update_attempt.in_config_hash),
    "%s", in_config_hash);
  return rr_db_stmt_execute(s_import.registrar_update_attempt.stmt, NULL);
}

static int rr_import_registrar_lock(
  const char *in_name,
  unsigned   *out_registrar_id,
  unsigned   *out_serial,
  unsigned   *out_last_import,
  unsigned   *out_last_check)
{
  snprintf(s_import.registrar_lock.in_name,
    sizeof(s_import.registrar_lock.in_name), "%s", in_name);
  int rc = rr_db_stmt_fetch_one(s_import.registrar_lock.stmt);
  if (rc != 1)
    return rc;

  *out_registrar_id = s_import.registrar_lock.out_registrar_id;
  *out_serial       = s_import.registrar_lock.out_serial;
  *out_last_import  = s_import.registrar_lock.out_last_import;
  *out_last_check   = s_import.registrar_lock.out_last_check;
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
  if (s_import_stop_requested)
    return false;

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
  if (s_import_stop_requested)
    return false;

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

static bool rr_import_netblockv4_link_org(
  unsigned in_registrar_id, unsigned long long *out_affected)
{
  s_import.netblockv4_link_org.in_registrar_id = in_registrar_id;
  return rr_db_stmt_execute(s_import.netblockv4_link_org.stmt, out_affected);
}

bool rr_import_netblockv6_insert(RRDBNetBlock *in_netblock)
{
  if (s_import_stop_requested)
    return false;

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

static bool rr_import_netblockv6_link_org(
  unsigned in_registrar_id, unsigned long long *out_affected)
{
  s_import.netblockv6_link_org.in_registrar_id = in_registrar_id;
  return rr_db_stmt_execute(s_import.netblockv6_link_org.stmt, out_affected);
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

static bool rr_import_state_mark_changed(bool unions_dirty)
{
  s_import.import_state_mark_changed.in_unions_dirty = unions_dirty ? 1 : 0;
  return rr_db_stmt_execute(s_import.import_state_mark_changed.stmt, NULL);
}

static int rr_import_list_state_get(
  unsigned long long *out_data_generation,
  unsigned long long *out_list_generation,
  char                out_config_hash[RR_SHA256_HEX_SIZE])
{
  int rc = rr_db_stmt_fetch_one(s_import.list_state_get.stmt);
  if (rc == 1)
  {
    *out_data_generation = s_import.list_state_get.out_data_generation;
    *out_list_generation = s_import.list_state_get.out_list_generation;
    snprintf(out_config_hash, RR_SHA256_HEX_SIZE, "%s",
      s_import.list_state_get.out_config_hash);
  }
  return rc;
}

static bool rr_import_list_state_mark_current(const char *config_hash)
{
  snprintf(s_import.list_state_mark_current.in_config_hash,
    sizeof(s_import.list_state_mark_current.in_config_hash),
    "%s", config_hash);
  return rr_db_stmt_execute(s_import.list_state_mark_current.stmt, NULL);
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

static void rr_import_list_union_batches_reset(void)
{
  s_import.listUnionBatch.ipv4.count = 0;
  s_import.listUnionBatch.ipv6.count = 0;
}

static bool rr_import_netblockv4_list_union_batch_flush(void)
{
  const size_t count = s_import.listUnionBatch.ipv4.count;
  if (count == 0)
    return true;

  if (count == RR_IMPORT_LIST_UNION_BATCH_ROWS)
  {
    if (!rr_db_stmt_execute(s_import.listUnionBatch.ipv4.stmt, NULL))
    {
      LOG_ERROR("failed to write IPv4 list union batch (%zu rows)", count);
      return false;
    }
  }
  else
    for(size_t i = 0; i < count; ++i)
    {
      const RRImportListUnionV4 *row = &s_import.listUnionBatch.ipv4.rows[i];
      s_import.netblockv4_list_union_insert.in_list_id    = row->listId;
      s_import.netblockv4_list_union_insert.in_ip         = row->ip;
      s_import.netblockv4_list_union_insert.in_prefix_len = row->prefixLen;
      if (!rr_db_stmt_execute(s_import.netblockv4_list_union_insert.stmt, NULL))
      {
        LOG_ERROR("failed to write IPv4 list union row %zu", i);
        return false;
      }
    }

  s_import.listUnionBatch.ipv4.count = 0;
  return true;
}

static bool rr_import_netblockv6_list_union_batch_flush(void)
{
  const size_t count = s_import.listUnionBatch.ipv6.count;
  if (count == 0)
    return true;

  if (count == RR_IMPORT_LIST_UNION_BATCH_ROWS)
  {
    if (!rr_db_stmt_execute(s_import.listUnionBatch.ipv6.stmt, NULL))
    {
      LOG_ERROR("failed to write IPv6 list union batch (%zu rows)", count);
      return false;
    }
  }
  else
    for(size_t i = 0; i < count; ++i)
    {
      const RRImportListUnionV6 *row = &s_import.listUnionBatch.ipv6.rows[i];
      s_import.netblockv6_list_union_insert.in_list_id    = row->listId;
      s_import.netblockv6_list_union_insert.in_ip         = row->ip;
      s_import.netblockv6_list_union_insert.in_prefix_len = row->prefixLen;
      if (!rr_db_stmt_execute(s_import.netblockv6_list_union_insert.stmt, NULL))
      {
        LOG_ERROR("failed to write IPv6 list union row %zu", i);
        return false;
      }
    }

  s_import.listUnionBatch.ipv6.count = 0;
  return true;
}

static bool rr_import_list_union_batches_flush(void)
{
  return
    rr_import_netblockv4_list_union_batch_flush() &&
    rr_import_netblockv6_list_union_batch_flush();
}

static bool rr_import_netblockv4_list_union_insert(unsigned in_list_id, unsigned in_ip, uint8_t in_prefix_len)
{
  if (s_import_stop_requested)
    return false;

  if (s_import.listUnionBatch.ipv4.count >= RR_IMPORT_LIST_UNION_BATCH_ROWS)
  {
    LOG_ERROR("IPv4 list union batch overflow");
    return false;
  }

  const size_t         index = s_import.listUnionBatch.ipv4.count++;
  RRImportListUnionV4 *row   = &s_import.listUnionBatch.ipv4.rows[index];
  row->listId    = in_list_id;
  row->ip        = in_ip;
  row->prefixLen = in_prefix_len;

  if (s_import.listUnionBatch.ipv4.count == RR_IMPORT_LIST_UNION_BATCH_ROWS)
    return rr_import_netblockv4_list_union_batch_flush();

  return true;
}

static bool rr_import_netblockv6_list_union_insert(unsigned in_list_id, unsigned __int128 in_ip, uint8_t in_prefix_len)
{
  if (s_import_stop_requested)
    return false;

  if (s_import.listUnionBatch.ipv6.count >= RR_IMPORT_LIST_UNION_BATCH_ROWS)
  {
    LOG_ERROR("IPv6 list union batch overflow");
    return false;
  }

  const size_t         index = s_import.listUnionBatch.ipv6.count++;
  RRImportListUnionV6 *row   = &s_import.listUnionBatch.ipv6.rows[index];
  row->listId    = in_list_id;
  row->ip        = in_ip;
  row->prefixLen = in_prefix_len;

  if (s_import.listUnionBatch.ipv6.count == RR_IMPORT_LIST_UNION_BATCH_ROWS)
    return rr_import_netblockv6_list_union_batch_flush();

  return true;
}

#pragma endregion

typedef struct RRImportListQuery
{
  RRBuffer  *sql;
  RRDBParam *params;
  size_t     paramCount;
  size_t     paramCapacity;
}
RRImportListQuery;

static bool db_list_query_add_param(
  RRImportListQuery *query, const char *value)
{
  if (query->paramCount == query->paramCapacity)
  {
    if (query->paramCapacity > SIZE_MAX / 2)
    {
      LOG_ERROR("too many list query parameters");
      return false;
    }

    const size_t newCapacity = query->paramCapacity
      ? query->paramCapacity * 2
      : 16;
    if (newCapacity > SIZE_MAX / sizeof(*query->params))
    {
      LOG_ERROR("too many list query parameters");
      return false;
    }

    RRDBParam *params = realloc(query->params,
      sizeof(*query->params) * newCapacity);
    if (!params)
    {
      LOG_ERROR("out of memory");
      return false;
    }

    query->params        = params;
    query->paramCapacity = newCapacity;
  }

  query->params[query->paramCount++] = (RRDBParam)
  {
    .type = RRDB_TYPE_STRING,
    .bind = (void *)value
  };
  return true;
}

static bool db_build_list_query_where(
  ConfigList *cl, RRImportListQuery *query)
{
  RRBuffer *qb = query->sql;

  #define APPEND_OR_FAIL(qb, str) \
    do { \
      if (rr_buffer_append_str(qb, str) < 0) \
      { \
        LOG_ERROR("out of memory"); \
        return false; \
      } \
    } while (0)

  #define LIST_FIELD_org_handle  "org.handle"
  #define LIST_FIELD_org_name    "org.name"
  #define LIST_FIELD_org_descr   "org.descr"
  #define LIST_FIELD_org_email   "COALESCE(org.email, '')"
  #define LIST_FIELD_ip_netname  "ip.netname"
  #define LIST_FIELD_ip_descr    "ip.descr"
  #define LIST_FIELD_ip_email    "COALESCE(ip.email, '')"
  #define LIST_FIELD_IMPL(x, y)  LIST_FIELD_ ##x ##_ ##y
  #define LIST_FIELD(x, y)       LIST_FIELD_IMPL(x, y)

  #define ADD_CONDITION(x, y, z) \
    if (cl->x ##_ ##y.z) \
      for(const char **str = cl->x ##_ ##y.z; *str; ++str, ++conditions) \
      { \
        if (!rr_buffer_appendf(qb, "%s" LIST_FIELD(x, y) " LIKE ?", \
            conditions > 0 ? " OR " : "")) \
        { \
          LOG_ERROR("out of memory"); \
          return false; \
        } \
        if (!db_list_query_add_param(query, *str)) \
          return false; \
      } \

  bool started = false;
  if (cl->registrar)
  {
    started = true;
    if (rr_buffer_append_str(qb,
      "(ip.registrar_id = (SELECT id FROM registrar WHERE name = ?))") < 0)
    {
      LOG_ERROR("out of memory");
      return false;
    }
    if (!db_list_query_add_param(query, cl->registrar))
      return false;
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

          if (rr_buffer_append_str(qb,
            "(ip.registrar_id = "
            "(SELECT id FROM registrar WHERE name = ?))") < 0)
          {
            LOG_ERROR("out of memory");
            return false;
          }
          if (!db_list_query_add_param(query, s->name))
            return false;
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
          if (!db_build_list_query_where(l, query))
            return false;
          started = true;
          break;
        }
      }
  }

  #undef ADD_CONDITION
  #undef LIST_FIELD
  #undef LIST_FIELD_IMPL
  #undef LIST_FIELD_ip_email
  #undef LIST_FIELD_ip_descr
  #undef LIST_FIELD_ip_netname
  #undef LIST_FIELD_org_email
  #undef LIST_FIELD_org_descr
  #undef LIST_FIELD_org_name
  #undef LIST_FIELD_org_handle
  #undef APPEND_OR_FAIL
  return true;
}

static RRDBStmt *rr_import_prepare_org_batch(RRDBCon *con)
{
  RRBuffer  sql = { 0 };
  RRDBParam params[RR_IMPORT_BATCH_ROWS * 5];
  size_t    param = 0;

  if (rr_buffer_append_str(&sql,
    "INSERT INTO org_stage (registrar_id, handle, name, descr, email) VALUES ") < 0)
    goto fail;

  for(size_t i = 0; i < RR_IMPORT_BATCH_ROWS; ++i)
  {
    if (!rr_buffer_appendf(&sql, "%s(?, ?, ?, ?, ?)", i == 0 ? "" : ","))
      goto fail;

    RRDBOrg *row = &s_import.batch.org.rows[i];
    params[param++] = (RRDBParam){ .type = RRDB_TYPE_UINT  , .bind = &row->registrar_id };
    params[param++] = (RRDBParam){ .type = RRDB_TYPE_STRING, .bind = &row->handle       };
    params[param++] = (RRDBParam){ .type = RRDB_TYPE_STRING, .bind = &row->name         };
    params[param++] = (RRDBParam){ .type = RRDB_TYPE_STRING, .bind = &row->descr        };
    params[param++] = (RRDBParam){ .type = RRDB_TYPE_STRING, .bind = &row->email        };
  }

  if (rr_buffer_append_str(&sql,
    " ON DUPLICATE KEY UPDATE "
      "name = VALUES(name), descr = VALUES(descr), email = VALUES(email)") < 0)
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
  RRDBParam params[RR_IMPORT_BATCH_ROWS * 8];
  size_t    param = 0;

  if (!rr_buffer_appendf(&sql,
    "INSERT INTO netblock_%s_stage ("
      "registrar_id, org_handle, start_ip, end_ip, prefix_len, netname, descr, email"
    ") VALUES ",
    ipv6 ? "v6" : "v4"))
    goto fail;

  for(size_t i = 0; i < RR_IMPORT_BATCH_ROWS; ++i)
  {
    if (!rr_buffer_appendf(&sql, "%s(?, ?, ?, ?, ?, ?, ?, ?)", i == 0 ? "" : ","))
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
    params[param++] = (RRDBParam){ .type = RRDB_TYPE_STRING, .bind = &row->email     };
  }

  if (rr_buffer_append_str(&sql,
    " ON DUPLICATE KEY UPDATE "
      "prefix_len = VALUES(prefix_len), "
      "netname = VALUES(netname), "
      "descr = VALUES(descr), "
      "email = VALUES(email)") < 0)
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

static RRDBStmt *rr_import_prepare_list_union_batch(RRDBCon *con, bool ipv6)
{
  RRBuffer  sql = { 0 };
  RRDBParam params[RR_IMPORT_LIST_UNION_BATCH_ROWS * 3];
  size_t    param = 0;

  if (!rr_buffer_appendf(&sql,
    "INSERT INTO netblock_v%s_list_union (list_id, ip, prefix_len) VALUES ",
    ipv6 ? "6" : "4"))
    goto fail;

  for(size_t i = 0; i < RR_IMPORT_LIST_UNION_BATCH_ROWS; ++i)
  {
    if (!rr_buffer_appendf(&sql, "%s(?, ?, ?)", i == 0 ? "" : ","))
      goto fail;

    if (ipv6)
    {
      RRImportListUnionV6 *row = &s_import.listUnionBatch.ipv6.rows[i];
      params[param++] = (RRDBParam){ .type = RRDB_TYPE_UINT, .bind = &row->listId };
      params[param++] = (RRDBParam)
      {
        .type = RRDB_TYPE_BINARY,
        .bind = &row->ip,
        .size = sizeof(row->ip)
      };
      params[param++] = (RRDBParam){ .type = RRDB_TYPE_UINT8, .bind = &row->prefixLen };
    }
    else
    {
      RRImportListUnionV4 *row = &s_import.listUnionBatch.ipv4.rows[i];
      params[param++] = (RRDBParam){ .type = RRDB_TYPE_UINT , .bind = &row->listId    };
      params[param++] = (RRDBParam){ .type = RRDB_TYPE_UINT , .bind = &row->ip        };
      params[param++] = (RRDBParam){ .type = RRDB_TYPE_UINT8, .bind = &row->prefixLen };
    }
  }

  RRDBStmt *stmt = rr_db_stmt_preparev(con, sql.buffer,
    params, ARRAY_SIZE(params), NULL, 0);
  rr_buffer_free(&sql);
  return stmt;

fail:
  LOG_ERROR("failed to construct the IPv%s list union batch statement",
    ipv6 ? "6" : "4");
  rr_buffer_free(&sql);
  return NULL;
}

static bool rr_import_list_union_batches_prepare(RRDBCon *con)
{
  s_import.listUnionBatch.ipv4.stmt =
    rr_import_prepare_list_union_batch(con, false);
  if (!s_import.listUnionBatch.ipv4.stmt)
    return false;

  s_import.listUnionBatch.ipv6.stmt =
    rr_import_prepare_list_union_batch(con, true);
  if (!s_import.listUnionBatch.ipv6.stmt)
  {
    rr_db_stmt_free(&s_import.listUnionBatch.ipv4.stmt);
    return false;
  }

  rr_import_list_union_batches_reset();
  return true;
}

static void rr_import_list_union_batches_free(void)
{
  rr_db_stmt_free(&s_import.listUnionBatch.ipv4.stmt);
  rr_db_stmt_free(&s_import.listUnionBatch.ipv6.stmt);
  rr_import_list_union_batches_reset();
}

static bool rr_import_lock_acquire(void)
{
  s_import.lockHeld                         = false;
  s_import.import_lock_acquire.out_acquired = 0;
  const int rc = rr_db_stmt_fetch_one(s_import.import_lock_acquire.stmt);
  if (rc == 1 && s_import.import_lock_acquire.out_acquired != 0)
  {
    s_import.lockHeld = true;
    return true;
  }

  LOG_ERROR("another RackRadar importer owns the database import lock");
  return false;
}

static void rr_import_lock_release(void)
{
  if (!s_import.lockHeld)
    return;

  s_import.import_lock_release.out_released = 0;
  const int rc = rr_db_stmt_fetch_one(s_import.import_lock_release.stmt);
  if (rc != 1 || s_import.import_lock_release.out_released == 0)
  {
    LOG_ERROR("failed to release the database import lock");
    return;
  }

  s_import.lockHeld = false;
}

static bool db_init_fn(RRDBCon *con, void **udata)
{
  *udata = &s_import;
  STMT_PREPARE(STATEMENTS, *udata);

  if (!rr_import_lock_acquire() || !rr_import_batches_prepare(con))
    return false;

  if (!g_config.lists)
    return true;

  if (!rr_import_list_union_batches_prepare(con))
    return false;

  s_import.lists_prepare = calloc(g_config.nbListsActive + 1, sizeof(*s_import.lists_prepare));
  if (!s_import.lists_prepare)
  {
    LOG_ERROR("out of memory");
    return false;
  }

  RRBuffer          qb    = { .bufferSz = 8192 };
  RRImportListQuery query = { .sql = &qb };

  typeof(s_import.lists_prepare) list = s_import.lists_prepare;
  for(ConfigList *cl = g_config.lists; cl->name; ++cl)
  {
    if (!cl->build_list)
      continue;

    list->cl = cl;
    snprintf(list->in_list_name, sizeof(list->in_list_name), "%s", cl->name);
    for(int n = 0; n < 2; ++n)
    {
      const char *ver = n == 0 ? "v4" : "v6";
      rr_buffer_reset(&qb);
      query.paramCount = 0;
      if (!db_list_query_add_param(&query, list->in_list_name))
        goto fail_list_query;

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
        goto fail_list_query;
      }
      size_t start = qb.pos;

      // reset the seen state
      for(ConfigList *l = g_config.lists; l->name; ++l)
        l->include_seen = false;

      // build the where part of the query
      if (!db_build_list_query_where(cl, &query))
      {
        LOG_ERROR("out of memory");
        goto fail_list_query;
      }

      // If there was no query built
      if (start == qb.pos)
      {
        LOG_ERROR("active list has no selection criteria: %s", cl->name);
        goto fail_list_query;
      }

      if (rr_buffer_append_str(&qb, " ORDER BY ip.start_ip ASC") < 0)
      {
        LOG_ERROR("out of memory");
        goto fail_list_query;
      }

      list->stmt[n] = rr_db_stmt_preparev(con, qb.buffer,
        query.params, query.paramCount, NULL, 0);

      if (!list->stmt[n])
      {
        LOG_ERROR("failed to prepare %s statement for list %s", ver, cl->name);
        goto fail_list_query;
      }
    }

    ++list;
  }

  #undef CONFIG_LIST_FIELDS
  free(query.params);
  rr_buffer_free(&qb);
  return true;

fail_list_query:
  free(query.params);
  rr_buffer_free(&qb);
  return false;
}

static bool db_deinit_fn(RRDBCon *con, void **udata)
{
  rr_import_lock_release();
  rr_import_list_union_batches_free();
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

static bool rr_import_download_cancel(void *opaque)
{
  (void)opaque;
  return s_import_stop_requested;
}

bool rr_import_init(void)
{
  if (s_import_stop_requested)
    return false;

  s_import.lockHeld = false;

  if (!rr_download_init(&s_import.dl))
  {
    LOG_ERROR("rr_download_init failed");
    return false;
  }

  if (s_import_stop_requested)
  {
    rr_download_deinit(&s_import.dl);
    return false;
  }

  rr_download_set_cancel(s_import.dl, rr_import_download_cancel, NULL);

  // reserve a connection for imports only
  if (!rr_db_reserve(&s_import.con, db_init_fn, db_deinit_fn))
  {
    LOG_ERROR("rr_db_reserve failed");
    rr_download_deinit(&s_import.dl);
    return false;
  }

  if (s_import_stop_requested)
  {
    rr_db_release(&s_import.con);
    rr_download_deinit(&s_import.dl);
    return false;
  }

  return true;
}

void rr_import_deinit(void)
{
  s_import_stop_requested = 1;
  rr_db_release(&s_import.con);
  rr_download_deinit(&s_import.dl);
}

void rr_import_stop(void)
{
  s_import_stop_requested = 1;
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

static RRImportList *rr_import_list_find(const char *name)
{
  for(RRImportList *list = s_import.lists_prepare; list->cl; ++list)
    if (strcmp(list->cl->name, name) == 0)
      return list;

  return NULL;
}

static bool rr_import_build_list(RRDBCon *con, RRImportList *list)
{
  if (s_import_stop_requested)
    return false;

  if (list->state == RR_IMPORT_LIST_BUILT)
    return true;

  if (list->state == RR_IMPORT_LIST_BUILDING)
  {
    LOG_ERROR("list exclusion cycle detected at %s", list->cl->name);
    return false;
  }

  list->state = RR_IMPORT_LIST_BUILDING;
  if (list->cl->exclude)
    for(const char **name = list->cl->exclude; *name; ++name)
    {
      RRImportList *dependency = rr_import_list_find(*name);
      if (!dependency)
      {
        LOG_ERROR("exclude list %s required by %s is not buildable",
          *name, list->cl->name);
        return false;
      }

      if (!rr_import_build_list(con, dependency))
        return false;
    }

  unsigned       list_id;
  const uint64_t started = rr_microtime();

  LOG_INFO("  Building: %s", list->cl->name);
  rr_import_list_union_batches_reset();
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
    !rr_import_list_union_batches_flush      () ||
    !rr_db_commit(con))
  {
    rr_db_rollback(con);
    rr_import_list_union_batches_reset();
    rr_import_log_timing("list build", list->cl->name, started);
    LOG_ERROR("failed");
    return false;
  }

  rr_import_log_timing("list build", list->cl->name, started);
  list->state = RR_IMPORT_LIST_BUILT;
  return true;
}

static bool rr_import_build_lists_internal(RRDBCon *con)
{
  if (!g_config.lists)
    return true;

  unsigned long long dataGeneration;
  unsigned long long listGeneration;
  char               configHash[RR_SHA256_HEX_SIZE];
  char               storedHash[RR_SHA256_HEX_SIZE];

  rr_import_list_config_hash(configHash);
  if (rr_import_list_state_get(
    &dataGeneration, &listGeneration, storedHash) != 1)
  {
    LOG_ERROR("failed to read the list state");
    return false;
  }

  if (dataGeneration == listGeneration &&
      strcmp(configHash, storedHash) == 0)
  {
    LOG_INFO("list snapshot is current");
    return true;
  }

  const uint64_t started = rr_microtime();
  LOG_INFO("rebuilding lists");
  for(RRImportList *list = s_import.lists_prepare; list->cl; ++list)
    list->state = RR_IMPORT_LIST_PENDING;

  for(RRImportList *list = s_import.lists_prepare; list->cl; ++list)
    if (!rr_import_build_list(con, list))
    {
      rr_import_log_timing("list snapshot", "all lists", started);
      return false;
    }

  if (!rr_import_list_state_mark_current(configHash))
  {
    rr_import_log_timing("list snapshot", "all lists", started);
    LOG_ERROR("failed to mark the list snapshot current");
    return false;
  }

  rr_import_log_timing("list snapshot", "all lists", started);
  LOG_INFO("done");
  return true;
}

bool rr_import_build_lists(void)
{
  if (s_import_stop_requested)
    return false;

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

static void rr_import_hash_config_value(RRSHA256 *ctx, const char *value)
{
  const uint8_t present = value ? 1 : 0;

  rr_sha256_update(ctx, &present, sizeof(present));
  if (value)
    rr_sha256_update(ctx, value, strlen(value) + 1);
}

static void rr_import_hash_list_value(RRSHA256 *ctx, const char *value)
{
  const uint8_t present = value ? 1 : 0;

  rr_sha256_update(ctx, &present, sizeof(present));
  if (value)
    rr_sha256_update(ctx, value, strlen(value) + 1);
}

static void rr_import_hash_list_array(RRSHA256 *ctx, const char **values)
{
  static const uint8_t end     = 0xff;
  const uint8_t        present = values ? 1 : 0;

  rr_sha256_update(ctx, &present, sizeof(present));
  if (values)
    for (const char **value = values; *value; ++value)
      rr_import_hash_list_value(ctx, *value);

  rr_sha256_update(ctx, &end, sizeof(end));
}

static void rr_import_hash_list_filter(
  RRSHA256 *ctx, const ConfigFilter *filter)
{
  rr_import_hash_list_array(ctx, filter->match );
  rr_import_hash_list_array(ctx, filter->ignore);
}

static void rr_import_list_config_hash(char out_hash[RR_SHA256_HEX_SIZE])
{
  RRSHA256 ctx;
  uint8_t  digest[RR_SHA256_DIGEST_SIZE];

  rr_sha256_init(&ctx);
  rr_sha256_update(&ctx, RR_IMPORT_LIST_VERSION,
    sizeof(RR_IMPORT_LIST_VERSION));

  for (const ConfigList *list = g_config.lists;
       list && list->name;
       ++list)
  {
    const uint8_t build = list->build_list ? 1 : 0;

    rr_sha256_update(&ctx, &build, sizeof(build));
    rr_import_hash_list_value (&ctx, list->name     );
    rr_import_hash_list_value (&ctx, list->registrar);
    rr_import_hash_list_array (&ctx, list->sources  );
    rr_import_hash_list_array (&ctx, list->include  );
    rr_import_hash_list_array (&ctx, list->exclude  );
    rr_import_hash_list_filter(&ctx, &list->org_handle);
    rr_import_hash_list_filter(&ctx, &list->org_name  );
    rr_import_hash_list_filter(&ctx, &list->org_descr );
    rr_import_hash_list_filter(&ctx, &list->org_email );
    rr_import_hash_list_filter(&ctx, &list->ip_netname);
    rr_import_hash_list_filter(&ctx, &list->ip_descr  );
    rr_import_hash_list_filter(&ctx, &list->ip_email  );
  }

  rr_sha256_final(&ctx, digest);
  rr_sha256_hex(digest, out_hash);
}

static void rr_import_source_config_hash(
  const typeof(*g_config.sources) *source,
  char out_hash[RR_SHA256_HEX_SIZE])
{
  RRSHA256 ctx;
  uint8_t  digest[RR_SHA256_DIGEST_SIZE];
  uint8_t  type = (uint8_t)source->type;

  rr_sha256_init(&ctx);
  rr_sha256_update(&ctx, RR_IMPORT_PARSER_VERSION,
    sizeof(RR_IMPORT_PARSER_VERSION));
  rr_sha256_update(&ctx, &type, sizeof(type));
  rr_import_hash_config_value(&ctx, source->name    );
  rr_import_hash_config_value(&ctx, source->url     );
  rr_import_hash_config_value(&ctx, source->user    );
  rr_import_hash_config_value(&ctx, source->pass    );
  rr_import_hash_config_value(&ctx, source->extra_v4);
  rr_import_hash_config_value(&ctx, source->extra_v6);
  rr_sha256_final(&ctx, digest);
  rr_sha256_hex(digest, out_hash);
}

static bool rr_import_source_content_hash(
  FILE *fp,
  char  out_hash[RR_SHA256_HEX_SIZE])
{
  uint8_t digest[RR_SHA256_DIGEST_SIZE];
  int     error;

  if (!rr_sha256_file(fp, true, digest, &error))
  {
    LOG_ERROR("failed to hash downloaded source: %s", strerror(error));
    return false;
  }

  rr_sha256_hex(digest, out_hash);
  return true;
}

static bool rr_import_source_due(
  unsigned last_check,
  unsigned database_time,
  int      frequency)
{
  if (last_check == 0 || frequency <= 0)
    return true;

  if (database_time < last_check)
    return true;

  return database_time - last_check >= (unsigned)frequency;
}

static void rr_import_source_state_set(
  RRImportSourceState *state,
  const char *config_hash,
  const char *content_hash,
  const RRDownloadMetadata *download)
{
  snprintf(state->configHash , sizeof(state->configHash ), "%s", config_hash );
  snprintf(state->contentHash, sizeof(state->contentHash), "%s", content_hash);
  state->download = *download;
}

static void rr_import_source_state_update_validators(
  RRImportSourceState *state, const RRDownloadMetadata *download)
{
  if (download->etag[0])
    snprintf(state->download.etag, sizeof(state->download.etag),
      "%s", download->etag);

  if (download->lastModified[0])
    snprintf(state->download.lastModified,
      sizeof(state->download.lastModified), "%s", download->lastModified);
}

static bool rr_import_registrar_update_unchanged(
  RRDBCon                   *con,
  const char                *name,
  const RRImportRegistrar   *expected,
  const RRImportSourceState *source)
{
  if (!rr_db_start(con))
    return false;

  unsigned  registrar_id;
  unsigned  serial;
  unsigned  last_import;
  unsigned  last_check;
  const int rc = rr_import_registrar_lock(name,
    &registrar_id, &serial, &last_import, &last_check);

  if (rc != 1)
  {
    LOG_ERROR("failed to lock registrar %s", name);
    rr_db_rollback(con);
    return false;
  }

  if (registrar_id != expected->id ||
      serial       != expected->serial ||
      last_import  != expected->lastImport ||
      last_check   != expected->lastCheck)
  {
    LOG_WARN("registrar %s changed while its source was fetched", name);
    rr_db_rollback(con);
    return false;
  }

  if (rr_import_registrar_update_check(registrar_id, source) &&
      rr_db_commit(con))
    return true;

  rr_db_rollback(con);
  return false;
}

static bool rr_import_registrar_mark_attempt(
  RRDBCon                   *con,
  const char                *name,
  const RRImportRegistrar   *expected,
  const char                *config_hash,
  RRImportRegistrar         *updated)
{
  if (!rr_db_start(con))
    return false;

  unsigned  registrar_id;
  unsigned  serial;
  unsigned  last_import;
  unsigned  last_check;
  const int rc = rr_import_registrar_lock(name,
    &registrar_id, &serial, &last_import, &last_check);

  if (rc != 1)
  {
    LOG_ERROR("failed to lock registrar %s", name);
    rr_db_rollback(con);
    return false;
  }

  if (registrar_id != expected->id ||
      serial       != expected->serial ||
      last_import  != expected->lastImport ||
      last_check   != expected->lastCheck)
  {
    LOG_WARN("registrar %s changed before its source fetch", name);
    rr_db_rollback(con);
    return false;
  }

  if (!rr_import_registrar_update_attempt(registrar_id, config_hash) ||
      !rr_db_commit(con))
  {
    rr_db_rollback(con);
    return false;
  }

  if (rr_import_registrar_get(name, updated) != 1)
  {
    LOG_ERROR("failed to refresh registrar %s after marking its fetch", name);
    return false;
  }

  return true;
}

bool rr_import_run(void)
{
  int rc;
  bool rebuild_unions = false;
  bool rebuild_lists  = true;
  bool check_unions    = true;
  while(!s_import_stop_requested)
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

    for(unsigned i = 0; !s_import_stop_requested && i < g_config.nbSources; ++i)
    {
      typeof(*g_config.sources) *src = &g_config.sources[i];
      if (src->type == SOURCE_TYPE_INVALID)
        continue;

      memset(&s_import.stats, 0, sizeof(s_import.stats));
      RRImportRegistrar registrar = { 0 };
      char              configHash[RR_SHA256_HEX_SIZE];
      rr_import_source_config_hash(src, configHash);

      rc = rr_import_registrar_get(src->name, &registrar);

      if (rc < 0)
        goto fail_con;

      if (rc == 0)
      {
        LOG_INFO("Registrar not found, inserting new record...");
        rc = rr_import_registrar_insert(src->name, &registrar.id);
        if (rc < 0)
          goto fail_con;

        if (rc == 0)
        {
          LOG_ERROR("Failed to insert a new registrar");
          continue;
        }
        LOG_INFO("New registrar inserted");
      }

      const bool configMatches =
        strcmp(configHash, registrar.source.configHash) == 0;
      const bool checkConfigMatches =
        strcmp(configHash, registrar.checkConfigHash) == 0;
      if (checkConfigMatches &&
          !rr_import_source_due(registrar.lastCheck,
            registrar.databaseTime, src->frequency))
        continue;

      RRImportRegistrar checkedRegistrar;
      if (!rr_import_registrar_mark_attempt(con, src->name, &registrar,
        configHash, &checkedRegistrar))
        goto fail_con;
      registrar = checkedRegistrar;

      const bool sentValidators = configMatches &&
        registrar.source.contentHash[0] &&
        (registrar.source.download.etag[0] ||
         registrar.source.download.lastModified[0]);

      LOG_INFO("Fetching source: %s", src->name);
      if (src->user && src->pass)
        rr_download_set_auth(s_import.dl, src->user, src->pass);
      else
        rr_download_clear_auth(s_import.dl);

      FILE                *fp             = NULL;
      RRDownloadMetadata   response       = { 0 };
      const uint64_t       fetchStarted   = rr_microtime();
      RRDownloadResult     downloadResult =
        rr_download_to_tmpfile_conditional(
          s_import.dl,
          src->url,
          sentValidators ? &registrar.source.download : NULL,
          &fp,
          &response);
      rr_import_log_timing("fetch", src->name, fetchStarted);

      if (downloadResult == RR_DOWNLOAD_RESULT_CANCELLED ||
          s_import_stop_requested)
      {
        if (fp)
          fclose(fp);
        break;
      }

      if (downloadResult == RR_DOWNLOAD_RESULT_ERROR)
      {
        LOG_ERROR("failed fetch for %s", src->name);
        continue;
      }

      if (downloadResult == RR_DOWNLOAD_RESULT_NOT_MODIFIED)
      {
        if (!sentValidators)
        {
          LOG_ERROR("source %s returned 304 without a conditional request",
            src->name);
          continue;
        }

        RRImportSourceState checked = registrar.source;
        rr_import_source_state_update_validators(&checked, &response);
        if (!rr_import_registrar_update_unchanged(
          con, src->name, &registrar, &checked))
          goto fail_con;

        LOG_INFO("source %s is unchanged (HTTP 304)", src->name);
        continue;
      }

      char           contentHash[RR_SHA256_HEX_SIZE];
      const uint64_t hashStarted   = rr_microtime();
      const bool     hashSucceeded =
        rr_import_source_content_hash(fp, contentHash);
      rr_import_log_timing("hash", src->name, hashStarted);
      if (s_import_stop_requested)
      {
        fclose(fp);
        break;
      }

      if (!hashSucceeded)
      {
        fclose(fp);
        continue;
      }

      if (configMatches &&
          strcmp(contentHash, registrar.source.contentHash) == 0)
      {
        RRImportSourceState checked;
        rr_import_source_state_set(&checked, configHash, contentHash, &response);
        fclose(fp);

        if (!rr_import_registrar_update_unchanged(
          con, src->name, &registrar, &checked))
          goto fail_con;

        LOG_INFO("source %s content is unchanged", src->name);
        continue;
      }

      RRImportSourceState sourceState;
      rr_import_source_state_set(&sourceState,
        configHash, contentHash, &response);

      if (fseeko(fp, 0, SEEK_SET) != 0)
      {
        LOG_ERROR("fseek 0 failed");
        fclose(fp);
        continue;
      }

      LOG_INFO("start import %s", src->name);
      const uint64_t startTime = rr_microtime();

      rr_import_batches_reset();
      if (!rr_import_stages_truncate())
      {
        rr_import_log_timing("parse/stage", src->name, startTime);
        LOG_ERROR("failed to clear the import staging tables");
        fclose(fp);
        goto fail_con;
      }

      unsigned registrar_id = registrar.id;
      unsigned serial       = registrar.serial + 1;
      bool     success      = false;
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
        success = !s_import_stop_requested &&
          rr_import_batches_flush() &&
          !s_import_stop_requested;
      fclose(fp);
      rr_import_log_timing("parse/stage", src->name, startTime);

      const char *resultStr;
      if (success && !s_import_stop_requested)
      {
        const uint64_t lockStarted = rr_microtime();
        if (!rr_db_start(con))
        {
          rr_import_log_timing("registrar lock", src->name, lockStarted);
          goto fail_con;
        }

        unsigned locked_registrar_id;
        unsigned locked_serial;
        unsigned locked_last_import;
        unsigned locked_last_check;
        rc = rr_import_registrar_lock(src->name,
          &locked_registrar_id,
          &locked_serial,
          &locked_last_import,
          &locked_last_check);

        if (rc != 1)
        {
          rr_import_log_timing("registrar lock", src->name, lockStarted);
          LOG_ERROR("failed to lock registrar %s", src->name);
          if (!rr_db_rollback(con))
            goto fail_con;
          rr_import_stats_rollback();
          resultStr = "failed";
          goto log_result;
        }

        if (locked_registrar_id != registrar_id ||
            locked_serial       != serial - 1 ||
            locked_last_import  != registrar.lastImport ||
            locked_last_check   != registrar.lastCheck)
        {
          rr_import_log_timing("registrar lock", src->name, lockStarted);
          LOG_WARN("registrar %s changed while its source was being staged", src->name);
          if (!rr_db_rollback(con))
            goto fail_con;
          rr_import_stats_rollback();
          resultStr = "superseded";
          goto log_result;
        }

        rr_import_log_timing("registrar lock", src->name, lockStarted);

        //finalize the registrar
        const uint64_t mergeStarted = rr_microtime();
        LOG_INFO("merging staged import");
        unsigned long long linkedIPv4      = 0;
        unsigned long long linkedIPv6      = 0;
        const bool         merged          = !s_import_stop_requested &&
          rr_import_org_merge_insert       (registrar_id, serial) &&
          rr_import_org_merge_update       (registrar_id, serial) &&
          rr_import_netblockv4_merge_insert(registrar_id, serial) &&
          rr_import_netblockv4_merge_update(registrar_id, serial) &&
          rr_import_netblockv6_merge_insert(registrar_id, serial) &&
          rr_import_netblockv6_merge_update(registrar_id, serial) &&
          rr_import_netblockv4_delete_old  (registrar_id) &&
          rr_import_netblockv6_delete_old  (registrar_id) &&
          rr_import_netblockv4_link_org    (registrar_id, &linkedIPv4) &&
          rr_import_netblockv6_link_org    (registrar_id, &linkedIPv6) &&
          rr_import_org_delete_old         (registrar_id);
        const bool         coverageChanged = merged &&
          (s_import.stats.newIPv4     || s_import.stats.deletedIPv4 ||
           s_import.stats.newIPv6     || s_import.stats.deletedIPv6);
        const bool         dataChanged     = coverageChanged || (merged &&
          (s_import.stats.newOrgs     || s_import.stats.updatedOrgs ||
           s_import.stats.deletedOrgs || s_import.stats.updatedIPv4 ||
           s_import.stats.updatedIPv6 || linkedIPv4 || linkedIPv6));
        const bool         finalized       = merged &&
          !s_import_stop_requested &&
          (!dataChanged || rr_import_state_mark_changed(coverageChanged)) &&
          rr_import_registrar_update_serial(registrar_id, serial,
            &sourceState) &&
          !s_import_stop_requested &&
          rr_db_commit(con);
        rr_import_log_timing("merge", src->name, mergeStarted);

        if (!finalized)
        {
          LOG_ERROR("failed to finalize");
          if (!rr_db_rollback(con))
            goto fail_con;
          rr_import_stats_rollback();
          resultStr = "failed";
          goto log_result;
        }

        resultStr = "succeeded";
        rebuild_unions |= coverageChanged;
        rebuild_lists  |= dataChanged;
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

    if (s_import_stop_requested)
    {
      rr_db_put(&con);
      break;
    }

    if (rebuild_unions)
    {
      const uint64_t buildStarted = rr_microtime();
      LOG_INFO("building union snapshot");
      const bool built =
        rr_import_netblockv4_union_next_truncate() &&
        rr_import_netblockv6_union_next_truncate() &&
        rr_import_netblockv4_union_next_populate() &&
        rr_import_netblockv6_union_next_populate();
      rr_import_log_timing("union build", "snapshot", buildStarted);
      if (!built)
      {
        LOG_ERROR("failed");
        goto fail_con;
      }

      const uint64_t publishStarted = rr_microtime();
      LOG_INFO("publishing union snapshot");
      const bool published =
        rr_db_start                         (con) &&
        rr_import_netblockv4_union_delete  () &&
        rr_import_netblockv6_union_delete  () &&
        rr_import_netblockv4_union_publish () &&
        rr_import_netblockv6_union_publish () &&
        rr_import_unions_mark_clean         () &&
        rr_db_commit                        (con);
      rr_import_log_timing("union publish", "snapshot", publishStarted);
      if (!published)
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

    rr_db_put(&con);
    if (s_import_stop_requested)
      break;

    usleep(1000000);
    continue;

fail_con:
    rr_db_put(&con);
fail:
    check_unions   = true;
    rebuild_lists  = true;
    if (!s_import_stop_requested)
      usleep(1000000);
  }

  return true;
}
