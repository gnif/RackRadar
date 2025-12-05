#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include <pthread.h>

#include "log.h"
#include "config.h"
#include "download.h"
#include "db.h"
#include "rpsl.h"
#include "arin.h"
#include "util.h"

bool running = true;
RRDownload *dl = NULL;
static void * import_thread(void *)
{
  while(running)
  {
    RRDBCon *con;
    if (!rr_db_get(&con))
    {
      LOG_ERROR("out of connections");
      goto next;
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

      if (registrar_id == 0)
      {
        LOG_ERROR("Failed to get the registrar id");
        rr_db_rollback(con);
        continue;
      }

      if (last_import > 0 && time(NULL) - last_import < src->frequency)
      {
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

      LOG_INFO("start import %s", src->name);
      uint64_t startTime = rr_microtime();

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

      const char *resultStr;
      if (success)
      {
        resultStr = "succeeded";
        rr_db_commit(con);
      }
      else
      {
        resultStr = "failed";
        rr_db_rollback(con);
      }
      rr_db_put(&con);
      fclose(fp);      

      uint64_t elapsed = rr_microtime() - startTime;
      uint64_t sec     = elapsed / 1000000UL;
      uint64_t us      = elapsed % 1000000UL;
      LOG_INFO("Import of %s %s in %02u:%02u:%02u.%03u",
        src->name,
        resultStr,
        (unsigned)(sec / 60 / 60),
        (unsigned)(sec / 60 % 60),
        (unsigned)(sec % 60),
        (unsigned)(us / 1000));
    }
    next:
    usleep(1000000);
  }
}

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

  if (!rr_download_init(&dl))
  {
    LOG_ERROR("rr_download_init failed");
    return EXIT_FAILURE;
  }

  import_thread(NULL);

  rr_download_deinit(&dl);
  rr_db_deinit();
  rr_config_deinit();
  return EXIT_SUCCESS;
}