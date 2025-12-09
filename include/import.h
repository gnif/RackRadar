#ifndef _H_RR_IMPORT_
#define _H_RR_IMPORT_

#include "db_structs.h"
#include <stdbool.h>

bool rr_import_init(void);
void rr_import_deinit(void);

// only used at startup, do not call after rr_import_run has started!
bool rr_import_build_lists(void);

bool rr_import_run(void);

// for use by the import code only when called from rr_import_run
bool rr_import_org_insert       (RRDBOrg      *in_org     );
bool rr_import_netblockv4_insert(RRDBNetBlock *in_netblock);
bool rr_import_netblockv6_insert(RRDBNetBlock *in_netblock);

#endif
