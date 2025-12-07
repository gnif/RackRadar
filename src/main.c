#include "log.h"
#include "config.h"
#include "util.h"
#include "download.h"
#include "http.h"
#include "query.h"
#include "rpsl.h"
#include "arin.h"

#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include <pthread.h>
#include <microhttpd.h>

bool running = true;
RRDownload *dl = NULL;
static void * import_thread(void *)
{
  bool rebuild_unions = false;

  while(running)
  {
    RRDBCon *con;
    int rc;
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

      unsigned registrar_id = 0;
      unsigned serial       = 0;
      unsigned last_import  = 0;
      
      if (!rr_db_start(con))
        goto put_next;

      rc = rr_query_registrar_by_name(con, src->name,
        &registrar_id,
        &serial,
        &last_import);

      if (rc < 0)
        goto next;

      if (rc == 0)
      {
        LOG_INFO("Registrar not found, inserting new record...");
        rc = rr_query_registrar_insert(con, src->name, &registrar_id);
        if (rc < 0)
          goto put_next;

        if (rc == 0)
        {
          LOG_ERROR("Failed to insert a new registrar");
          if (!rr_db_rollback(con))
            goto put_next;          
          continue;
        }
        LOG_INFO("New registrar inserted");
      }

      if (last_import > 0 && time(NULL) - last_import < src->frequency)
      {
        if (!rr_db_rollback(con))
          goto put_next;
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
        if (!rr_db_rollback(con))
          goto put_next;
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
        if (!rr_db_commit(con))
          goto put_next;
        resultStr = "succeeded";
        rebuild_unions = true;
      }
      else
      {
        if (!rr_db_rollback(con))
          goto put_next;
        resultStr = "failed";
      }
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

    if (rebuild_unions)
    {
      if (!rr_db_start(con))
        goto put_next;

      if (rr_query_build_unions(con))
      {
        if (!rr_db_commit(con))
          goto put_next;
      }
      else
      {        
        LOG_ERROR("failed to rebuild the unions");
        if (!rr_db_rollback(con))
          goto put_next;
      }
      rebuild_unions = false;
    }

    put_next:
    rr_db_put(&con);
    next:
    usleep(1000000);
  }
  return NULL;
}

int main(int argc, char *argv[])
{
  rr_log_init();
  if (!rr_config_init())
    LOG_WARN("Failed to load config, using defaults");

  if (!rr_db_init(rr_query_init, rr_query_deinit))
  {
    LOG_ERROR("DB init failed, can not continue");
    return EXIT_FAILURE;
  }

  if (!rr_download_init(&dl))
  {
    LOG_ERROR("rr_download_init failed");
    return EXIT_FAILURE;
  }

  if (!rr_http_init())
  {
    LOG_ERROR("rr_http_init failed");
    return EXIT_FAILURE;
  }

  import_thread(NULL);

  rr_http_deinit();
  rr_download_deinit(&dl);
  rr_db_deinit();
  rr_config_deinit();
  return EXIT_SUCCESS;
}