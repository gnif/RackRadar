#ifndef _H_RR_ARIN_
#define _H_RR_ARIN_

#include "db.h"

#include <stdbool.h>
#include <stdio.h>

bool rr_arin_import_zip_FILE(const char *registrar, FILE *fp,
  RRDBCon *con, unsigned registrar_id, unsigned new_serial);
bool rr_arin_import_zip(const char *registrar, const char *filename,
  RRDBCon *con, unsigned registrar_id, unsigned new_serial);


#endif
