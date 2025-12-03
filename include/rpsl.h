#ifndef _H_RR_RPSL_
#define _H_RR_RPSL_

#include <stdbool.h>
#include <stdio.h>

bool rr_rpsl_import_gz_FILE(const char *registrar, FILE *fp);
bool rr_rpsl_import_gz(const char *registrar, const char *filename);

#endif
