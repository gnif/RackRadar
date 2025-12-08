#include "log.h"
#include "config.h"
#include "util.h"
#include "download.h"
#include "query.h"
#include "import.h"
#include "http.h"

#include "rpsl.h"
#include "arin.h"

#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include <pthread.h>

bool running = true;
RRDownload *dl = NULL;

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

  if (!rr_download_init(&dl))
  {
    LOG_ERROR("rr_download_init failed");
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
  rr_download_deinit(&dl);
  rr_db_deinit();
  rr_config_deinit();
  return EXIT_SUCCESS;
}
