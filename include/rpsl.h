#ifndef _H_RR_RPSL_
#define _H_RR_RPSL_

#include <stdbool.h>
#include <stdio.h>

bool rr_rpsl_import_gz_FILE(const char *registrar, FILE *fp,
  unsigned registrar_id, unsigned new_serial);
bool rr_rpsl_import_gz(const char *filename, const char *registrar,
  unsigned registar_id, unsigned new_serial);

#endif
