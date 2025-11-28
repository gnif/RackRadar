#include <stdio.h>
#include <stdlib.h>
#include <libconfig.h>

#include "log.h"

int main(int argc, char *argv)
{
  log_init();

  config_t config;
  config_init(&config);
  if (config_read_file(&config, "settings.cfg") != CONFIG_TRUE)
  {
    LOG_ERROR("Failed to read settings.cfg");
    return EXIT_FAILURE;
  }

  config_destroy(&config);
  return EXIT_SUCCESS;
}