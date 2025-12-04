#include <stdlib.h>
#include <assert.h>

#include "log.h"
#include "config.h"
#include "download.h"
#include "db.h"
#include "rpsl.h"
#include "arin.h"

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
  if (!rr_db_get(&con))
  {
    LOG_ERROR("out of connections");
    return EXIT_FAILURE;
  }

  for(unsigned i = 0; i < g_config.nbSources; ++i)
  {
    typeof(*g_config.sources) *src = &g_config.sources[i];
    if (src->type == SOURCE_TYPE_INVALID)
      continue;

    rr_db_start(con);
    unsigned serial, last_import;
    unsigned registrar_id =
      rr_db_get_registrar_id(con, src->name, true, &serial, &last_import);
    if (last_import > 0 && time(NULL) - last_import >= src->frequency)
    {
      LOG_INFO("Skipping %s, not time yet", src->name);
      rr_db_rollback(con);
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
      rr_db_rollback(con);
      continue;
    }

    ++serial;
    bool success = false;
    switch(src->type)
    {
      case SOURCE_TYPE_RPSL:
        success = rr_rpsl_import_gz_FILE(src->name, fp, con, registrar_id, serial);
        break;

      case SOURCE_TYPE_ARIN:
        success = rr_arin_import_zip_FILE(src->name, fp, con, registrar_id, serial);
        break;

      default:
        assert(false);
    }

    if (success)
      rr_db_commit(con);
    else
      rr_db_rollback(con);

    fclose(fp);
  }  

  rr_db_put(&con);
  rr_download_deinit(&dl);
  rr_db_deinit();
  rr_config_deinit();
  return EXIT_SUCCESS;
}