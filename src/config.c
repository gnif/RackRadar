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
  
  config_setting_t *sources = config_lookup(&s_config, "sources");
  if (!sources || config_setting_type(sources) != CONFIG_TYPE_GROUP)
  {
    LOG_ERROR("'sources' missing or is not a group");
    return false;    
  }

  g_config.nbSources = config_setting_length(sources);
  g_config.sources   = calloc(g_config.nbSources, sizeof(*g_config.sources));
  if (!g_config.sources)
  {
    LOG_ERROR("out of memory");
    return false;
  }

  for (unsigned i = 0; i < g_config.nbSources; ++i)
  {
    typeof(*g_config.sources) *dst = &g_config.sources[i];    
    const config_setting_t    *src = config_setting_get_elem(sources, i);

    dst->name = config_setting_name(src);
    const char *type;
    config_setting_lookup_string(src, "type"     , &type);
    config_setting_lookup_int   (src, "frequency", &dst->frequency);
    config_setting_lookup_string(src, "url"      , &dst->url      );
    config_setting_lookup_string(src, "user"     , &dst->user     );
    config_setting_lookup_string(src, "pass"     , &dst->pass     );

    if (strcmp(type, "RPSL") == 0)
      dst->type = SOURCE_TYPE_RPSL;
    else if (strcmp(type, "ARIN") == 0)
      dst->type = SOURCE_TYPE_ARIN;
    else
    {
      dst->type = SOURCE_TYPE_INVALID;
      LOG_ERROR("Unsupported source type %s", type);      
    }
  }

  return true;
}

void rr_config_deinit(void)
{
  config_destroy(&s_config);
  free(g_config.sources);
  memset(&g_config, 0, sizeof(g_config));
}