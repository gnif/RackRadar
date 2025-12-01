#include <stdlib.h>

#include "log.h"
#include "config.h"
#include "db.h"

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

  rr_db_deinit();
  return EXIT_SUCCESS;
}