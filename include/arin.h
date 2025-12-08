#ifndef _H_RR_ARIN_
#define _H_RR_ARIN_

#include <stdbool.h>
#include <stdio.h>

bool rr_arin_import_zip_FILE(const char *registrar, FILE *fp,
  unsigned registrar_id, unsigned new_serial);
bool rr_arin_import_zip(const char *registrar, const char *filename,
  unsigned registrar_id, unsigned new_serial);


#endif
