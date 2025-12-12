#ifndef _H_RR_JSON_
#define _H_RR_JSON_

#include <stdio.h>
#include <stdint.h>

bool rr_json_import_FILE(const char *registrar, FILE *fp,
  unsigned registrar_id, unsigned new_serial,
  const char *extra_v4, const char *extra_v6);

#endif
