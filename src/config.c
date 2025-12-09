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

static bool rr_config_read_string_array(const config_setting_t *array, const char ***out)
{
  if (config_setting_type(array) != CONFIG_TYPE_ARRAY)
  {
    LOG_ERROR("not an array");
    return false;
  }

  unsigned count = config_setting_length(array);
  if (count == 0)
    return true;

  const char **values = calloc(count+1, sizeof(*values));
  if (!values)
  {
    LOG_ERROR("out of memory");
    return false;
  }

  for(unsigned i = 0; i < count; ++i)
  {
    const config_setting_t *elem = config_setting_get_elem(array, i);
    values[i] = config_setting_get_string(elem);
    if (!values[i])
    {
      LOG_ERROR("invalid value in array, %d is not a string", i);
      free(values);
      return false;
    }
  }

  *out = values;
  return true;
}

static bool rr_config_read_list_filter(const config_setting_t *filter, ConfigFilter *out)
{
  config_setting_t *match  = config_setting_lookup(filter, "match" );
  config_setting_t *ignore = config_setting_lookup(filter, "ignore");

  if (match && !rr_config_read_string_array(match, &out->match))
  {
    LOG_ERROR("failed to load filter 'match'");
    return false;
  }

  if (ignore && !rr_config_read_string_array(ignore, &out->ignore))
  {
    LOG_ERROR("failed to load filter 'ignore'");
    return false;
  }

  return true;
}

bool rr_config_init(void)
{
  config_init(&s_config);
  if (config_read_file(&s_config, "/etc/rackradar/main.cfg") != CONFIG_TRUE)
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
  g_config.sources   = calloc(g_config.nbSources + 1, sizeof(*g_config.sources));
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

  config_setting_t *lists = config_lookup(&s_config, "lists");
  if (!lists || config_setting_type(lists) != CONFIG_TYPE_GROUP)
  {
    LOG_ERROR("'lists' missing or is not a group");
    return false;
  }

  unsigned len   = config_setting_length(lists);
  g_config.lists = calloc(len + 1, sizeof(*g_config.lists));
  if (!g_config.lists)
  {
    LOG_ERROR("out of memory");
    return false;
  }

  ConfigList *list = g_config.lists;
  for (unsigned i = 0; i < len; ++i)
  {
    const config_setting_t *s = config_setting_get_elem(lists, i);

    list->name = config_setting_name(s);
    config_setting_t *include  = config_setting_lookup(s, "include" );
    config_setting_t *exclude  = config_setting_lookup(s, "exclude" );

    if (include && !rr_config_read_string_array(include, &list->include))
    {
      LOG_ERROR("lists.%s.include failed to load", list->name);
      continue;
    }

    if (exclude && !rr_config_read_string_array(exclude, &list->exclude))
    {
      LOG_ERROR("lists.%s.exclude failed to load", list->name);
      continue;
    }

    #define LOAD_FILTER(x) \
      config_setting_t *x = config_setting_lookup(s, #x); \
      if (x && !rr_config_read_list_filter(x, &list->x)) \
      { \
        LOG_ERROR("lists.%s." #x " failed to load", list->name); \
        continue; \
      } \
      list->has_matches = list->has_matches || (list->x.match  != NULL); \
      list->has_ignores = list->has_ignores || (list->x.ignore != NULL);

    #define X(x, y) LOAD_FILTER(x ##_ ##y)
    CONFIG_LIST_FIELDS
    #undef X
    #undef LOAD_FILTER

    ++list;
    ++g_config.nbLists;
  }

  return true;
}

void rr_config_deinit(void)
{
  for(ConfigList *list = g_config.lists; list->name; ++list)
  {
    free(list->include);
    free(list->exclude);
    #define X(x, y) \
      free(list->x ##_ ##y.match ); \
      free(list->x ##_ ##y.ignore);
    CONFIG_LIST_FIELDS
    #undef X
  }
  free(g_config.lists);

  free(g_config.sources);
  config_destroy(&s_config);
  memset(&g_config, 0, sizeof(g_config));
}
