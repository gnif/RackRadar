#include <stdlib.h>

#include "log.h"
#include "config.h"

int main(int argc, char *argv)
{
  rr_log_init();
  rr_config_init();

  return EXIT_SUCCESS;
}