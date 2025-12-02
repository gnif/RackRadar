#ifndef _H_RR_CONFIG_
#define _H_RR_CONFIG_

#include <stdbool.h>

#define SETTINGS \
  SETTING_STR(database.host, "127.0.0.1") \
  SETTING_INT(database.port, 3306       ) \
  SETTING_STR(database.user, "rackradar") \
  SETTING_STR(database.pass, "rackradar") \
  SETTING_STR(database.name, "rackradar")  

typedef struct Config
{
  struct
  {
    const char *host;
    int         port;
    const char *user;
    const char *pass;
    const char *name;
  }
  database;
}
Config;

extern Config g_config;

bool rr_config_init(void);
void rr_config_deinit(void);

#endif