#include "config.h"
#include "util.h"

#include <libconfig.h>

#define SETTING_STR(configField, defaultValue) .configField = defaultValue,
#define SETTING_INT SETTING_STR
Config g_config = { SETTINGS };
#undef SETTING_STR
#undef SETTING_INT

typedef struct ConfigDef
{
  enum
  {
    CFG_TYPE_STR,
    CFG_TYPE_INT
  }
  type;
  void       *ptr;
  const char *path;
}
ConfigDef;

#define SETTING_STR(configField, defaultValue) \
  { .type = CFG_TYPE_STR, .ptr = &g_config.configField, .path = #configField },

#define SETTING_INT(configField, defaultValue) \
  { .type = CFG_TYPE_INT, .ptr = &g_config.configField, .path = #configField },

static ConfigDef configDef[] = { SETTINGS };

#undef SETTING_STR
#undef SETTING_INT

bool rr_config_init(void)
{
  config_t config;
  config_init(&config);
  if (config_read_file(&config, "settings.cfg") != CONFIG_TRUE)
    return false;

  for(int i = 0; i < ARRAY_SIZE(configDef); ++i)
  {
    ConfigDef *def = &configDef[i];
    switch(def->type)
    {
      case CFG_TYPE_STR:
        config_lookup_string(&config, def->path, def->ptr);
        break;

      case CFG_TYPE_INT:
        config_lookup_int(&config, def->path, def->ptr);
        break;
    }
  }

  config_destroy(&config);
  return true;
}