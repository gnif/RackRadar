#ifndef _H_RR_CONFIG_
#define _H_RR_CONFIG_

#include <stdbool.h>

#define SETTINGS \
  SETTING_STR(database.host, "127.0.0.1") \
  SETTING_INT(database.port, 3306       ) \
  SETTING_STR(database.user, "rackradar") \
  SETTING_STR(database.pass, "rackradar") \
  SETTING_STR(database.name, "rackradar") \
  SETTING_STR(database.pool, 8          ) \
  \
  SETTING_INT(http.port    , 8888       )

typedef struct ConfigFilter
{
  const char **match;
  const char **ignore;
}
ConfigFilter;

typedef struct ConfigList
{
  const char *name;
  const char **include;
  const char **exclude;
  ConfigFilter netname;
  ConfigFilter descr;
  ConfigFilter org_name;
  ConfigFilter org;
}
ConfigList;

typedef struct Config
{
  struct
  {
    const char *host;
    int         port;
    const char *user;
    const char *pass;
    const char *name;
    int         pool;
  }
  database;

  struct
  {
    int port;
  }
  http;

  struct
  {
    const char *name;
    enum
    {
      SOURCE_TYPE_INVALID,
      SOURCE_TYPE_RPSL,
      SOURCE_TYPE_ARIN
    }
    type;
    int frequency;
    const char *url;
    const char *user;
    const char *pass;
  }
  *sources;
  unsigned nbSources;

  ConfigList *lists;
  unsigned    nbLists;
}
Config;

extern Config g_config;

bool rr_config_init(void);
void rr_config_deinit(void);

#endif
