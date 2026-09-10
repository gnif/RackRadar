#include "log.h"
#include "config.h"
#include "query.h"
#include "import.h"
#include "http.h"

#include <signal.h>

static void rr_signal_handler(int signalNumber)
{
  (void)signalNumber;
  rr_import_stop();
}

static bool rr_signal_init(void)
{
  struct sigaction action = { 0 };
  action.sa_handler = rr_signal_handler;
  sigemptyset(&action.sa_mask);

  return
    sigaction(SIGINT , &action, NULL) == 0 &&
    sigaction(SIGTERM, &action, NULL) == 0;
}

int main(int argc, char *argv[])
{
  rr_log_init();
  const bool configLoaded = rr_config_init();
  int        result       = EXIT_FAILURE;
  if (!configLoaded)
    LOG_WARN("Failed to load config, using defaults");

  if (!rr_db_init(rr_query_init, rr_query_deinit))
  {
    LOG_ERROR("DB init failed, can not continue");
    goto config_deinit;
  }

  if (!rr_signal_init())
  {
    LOG_ERROR("failed to install signal handlers");
    goto db_deinit;
  }

  if (!rr_import_init())
  {
    LOG_ERROR("rr_import_init failed");
    goto db_deinit;
  }

  if (!rr_http_init())
  {
    LOG_ERROR("rr_http_init failed");
    goto import_deinit;
  }

  /* the configuration may have changed so we must
  rebuild the lists to correct the if they were changed */
  rr_import_build_lists();

  if (rr_import_run())
    result = EXIT_SUCCESS;

  rr_http_deinit();
import_deinit:
  rr_import_deinit();
db_deinit:
  rr_db_deinit();
config_deinit:
  if (configLoaded)
    rr_config_deinit();
  return result;
}
