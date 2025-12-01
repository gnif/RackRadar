#include "config.h"
#include "log.h"
#include "util.h"

#include <libconfig.h>

Config g_config = {0};

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
  union
  {
    const char *s;
    int         i;
  }
  def;
  int (*readFn)(config_t *config, const char *path, void **value);
}
ConfigDef;

#define SETTING_STR(configField, defaultValue) \
  { .type = CFG_TYPE_STR, .ptr = &g_config.configField, .path = #configField, .def.s = defaultValue },

#define SETTING_INT(configField, defaultValue) \
  { .type = CFG_TYPE_INT, .ptr = &g_config.configField, .path = #configField, .def.i = defaultValue },

static ConfigDef configDef[] = { SETTINGS };

int rr_config_init(void)
{
  config_t config;
  config_init(&config);
  if (config_read_file(&config, "settings.cfg") != CONFIG_TRUE)
    LOG_ERROR("Failed to read settings.cfg");

  for(int i = 0; i < ARRAY_SIZE(configDef); ++i)
  {
    ConfigDef *def = &configDef[i];
    switch(def->type)
    {
      case CFG_TYPE_STR:
        if (config_lookup_string(&config, def->path, def->ptr) != CONFIG_TRUE)
          *(const char **)(def->ptr) = def->def.s;
        break;

      case CFG_TYPE_INT:
        if (config_lookup_int(&config, def->path, def->ptr) != CONFIG_TRUE)
          *(int *)(def->ptr) = def->def.i;
        break;
    }
  }

  config_destroy(&config);
  return EXIT_SUCCESS;
}