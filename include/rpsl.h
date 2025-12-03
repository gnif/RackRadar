#ifndef _H_RR_RPSL_
#define _H_RR_RPSL_

#include "db.h"

#include <stdbool.h>
#include <stdio.h>


bool rr_rpsl_import_gz_FILE(const char *registrar, FILE *fp,
  RRDBCon *con, unsigned registrar_id, unsigned new_serial);
bool rr_rpsl_import_gz(const char *filename, const char *registrar,
  RRDBCon *con, unsigned registar_id, unsigned new_serial);

#endif
