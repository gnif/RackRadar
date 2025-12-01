#include <stdbool.h>

#define SETTINGS \
  SETTING_STR(database.host, "127.0.0.1") \
  SETTING_INT(database.port, 3306       ) \
  SETTING_STR(database.user, "rackradar") \
  SETTING_STR(database.pass, "rackradar")

typedef struct Config
{
  struct
  {
    const char *host;
    int         port;
    const char *user;
    const char *pass;
  }
  database;
}
Config;

extern Config g_config;

bool rr_config_init(void);