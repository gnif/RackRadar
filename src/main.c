#include <stdlib.h>

#include "log.h"
#include "config.h"
#include "db.h"
#include "rpsl.h"

int main(int argc, char *argv)
{
  rr_log_init();
  if (!rr_config_init())
    LOG_WARN("Failed to load config, using defaults");

  if (!rr_db_init())
  {
    LOG_ERROR("DB init failed, can not continue");
    return EXIT_FAILURE;
  }

  //rr_rpsl_import_gz("APNIC", "/home/geoff/Projects/RPSLStrip/build/apnic.RPSL.db.gz");
  //rr_rpsl_import_gz("RIPE" , "/home/geoff/Projects/RPSLStrip/build/ripe.db.gz");
  //rr_rpsl_import_gz("ARIN" , "/home/geoff/Projects/ARINtoRPSL/arin.db.gz");

  rr_db_deinit();
  rr_config_deinit();
  return EXIT_SUCCESS;
}