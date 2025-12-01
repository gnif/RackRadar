#include <stdlib.h>

#include "log.h"
#include "config.h"

int main(int argc, char *argv)
{
  rr_log_init();
  if (!rr_config_init())
    LOG_WARN("Failed to load config, using defaults");

  return EXIT_SUCCESS;
}