#include "log.h"
#include "config.h"
#include "query.h"
#include "import.h"
#include "http.h"

int main(int argc, char *argv[])
{
  rr_log_init();
  if (!rr_config_init())
    LOG_WARN("Failed to load config, using defaults");

  if (!rr_db_init(rr_query_init, rr_query_deinit))
  {
    LOG_ERROR("DB init failed, can not continue");
    return EXIT_FAILURE;
  }

  if (!rr_import_init())
  {
    LOG_ERROR("rr_import_init failed");
    return EXIT_FAILURE;
  }

  if (!rr_http_init())
  {
    LOG_ERROR("rr_http_init failed");
    return EXIT_FAILURE;
  }

  rr_import_run();

  rr_http_deinit();
  rr_import_deinit();
  rr_db_deinit();
  rr_config_deinit();
  return EXIT_SUCCESS;
}
