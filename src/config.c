#include "config.h"
#include "util.h"
#include "log.h"

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
static config_t s_config = { 0 };

#undef SETTING_STR
#undef SETTING_INT

bool rr_config_init(void)
{
  config_init(&s_config);
  if (config_read_file(&s_config, "settings.cfg") != CONFIG_TRUE)
  {
    LOG_ERROR("%s %s:%d",
      config_error_text(&s_config),
      config_error_file(&s_config),
      config_error_line(&s_config));
    return false;
  }

  for(int i = 0; i < ARRAY_SIZE(configDef); ++i)
  {
    ConfigDef *def = &configDef[i];
    switch(def->type)
    {
      case CFG_TYPE_STR:
        config_lookup_string(&s_config, def->path, def->ptr);
        break;

      case CFG_TYPE_INT:
        config_lookup_int(&s_config, def->path, def->ptr);
        break;
    }
  }

  return true;
}

void rr_config_deinit(void)
{
  config_destroy(&s_config);
}