#ifndef _H_RR_IMPORT_
#define _H_RR_IMPORT_

#include "db_structs.h"
#include <stdbool.h>
#include <stdio.h>

bool rr_import_init(void);
void rr_import_deinit(void);

// only used at startup, do not call after rr_import_run has started!
bool rr_import_build_lists(void);

bool rr_import_run(void);

// for use by the import code only when called from rr_import_run
bool rr_import_org_insert       (RRDBOrg      *in_org     );
bool rr_import_netblockv4_insert(RRDBNetBlock *in_netblock);
bool rr_import_netblockv6_insert(RRDBNetBlock *in_netblock);

bool rr_rpsl_import_gz_FILE(const char *registrar, FILE *fp,
  unsigned registrar_id, unsigned new_serial);
bool rr_rpsl_import_gz(const char *filename, const char *registrar,
  unsigned registar_id, unsigned new_serial);

bool rr_arin_import_zip_FILE(const char *registrar, FILE *fp,
  unsigned registrar_id, unsigned new_serial);
bool rr_arin_import_zip(const char *registrar, const char *filename,
  unsigned registrar_id, unsigned new_serial);

  bool rr_json_import_FILE(const char *registrar, FILE *fp,
  unsigned registrar_id, unsigned new_serial,
  const char *extra_v4, const char *extra_v6);

bool rr_regex_import_FILE(const char *registrar, FILE *fp,
  unsigned registrar_id, unsigned new_serial,
  const char *extra_v4, const char *extra_v6);

#endif
