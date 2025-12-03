#include <stdlib.h>
#include <assert.h>

#include "log.h"
#include "config.h"
#include "db.h"
#include "rpsl.h"
#include "download.h"


int main(int argc, char *argv)
{
  rr_log_init();
  if (!rr_config_init())
    LOG_WARN("Failed to load config, using defaults");

  if (!rr_db_init())
  {
    LOG_ERROR("DB init failed, can not continue");
    return EXIT_FAILURE;
  }

  RRDownload *dl = NULL;
  if (!rr_download_init(&dl))
  {
    LOG_ERROR("rr_download_init failed");
    return EXIT_FAILURE;
  }

  RRDBCon *con;
  if (!rr_db_getCon(&con))
  {
    LOG_ERROR("out of connections");
    return EXIT_FAILURE;
  }

  for(unsigned i = 0; i < g_config.nbSources; ++i)
  {
    typeof(*g_config.sources) *src = &g_config.sources[i];
    const char * filename;
    switch(src->type)
    {
      case SOURCE_TYPE_INVALID:
        continue;

      case SOURCE_TYPE_RPSL:
        filename = "/tmp/rr.db.gz";
        break;
      
      case SOURCE_TYPE_ARIN:
        continue; //FIXME: no ARIN support yet
        filename = "/tmp/arin.zip";
        break;
    };

    unsigned serial, last_import;
    unsigned registrar_id =
      rr_db_get_registrar_id(con, src->name, true, &serial, &last_import);
    if (last_import > 0 && time(NULL) - last_import >= src->frequency)
    {
      LOG_INFO("Skipping %s, not time yet", src->name);
      continue;
    }

    LOG_INFO("Fetching source: %s", src->name);
    if (src->user && src->pass)
      rr_download_set_auth(dl, src->user, src->pass);
    else
      rr_download_clear_auth(dl);

    FILE *fp;
    if (!rr_download_to_tmpfile(dl, src->url, &fp))
    {
      LOG_ERROR("failed fetch for %s", src->name);
      continue;
    }

    switch(src->type)
    {
      case SOURCE_TYPE_RPSL:
        rr_rpsl_import_gz_FILE(src->name, fp, con, registrar_id, serial + 1);
        break;

      case SOURCE_TYPE_ARIN:
        break;

      default:
        assert(false);
    }

    fclose(fp);
  }

  rr_db_putCon(&con);
  rr_download_deinit(&dl);
  rr_db_deinit();
  rr_config_deinit();
  return EXIT_SUCCESS;
}