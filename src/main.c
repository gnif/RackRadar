#include "log.h"
#include "config.h"
#include "download.h"
#include "rpsl.h"
#include "arin.h"
#include "util.h"
#include "query.h"

#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include <pthread.h>
#include <microhttpd.h>

bool running = true;
RRDownload *dl = NULL;
static void * import_thread(void *)
{
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
        resultStr = "succeeded";
        if (!rr_db_commit(con))
          goto put_next;
      }
      else
      {
        resultStr = "failed";
        if (!rr_db_rollback(con))
          goto put_next;
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

    put_next:
    rr_db_put(&con);
    next:
    usleep(1000000);
  }
  return NULL;
}

static enum MHD_Result httpd_handler(void * cls,
  struct MHD_Connection * connection,
  const char * url,
  const char * method,
  const char * version,
  const char * upload_data,
  size_t * upload_data_size,
  void ** ptr)
{
  struct MHD_Response *res;

  if (strcmp(method, "GET") != 0)
    return MHD_NO;

  if (strncmp(url, "/ip/", 4) == 0)
  {
    RRDBCon   *con;
    RRDBIPInfo info;
    char       ipstring[64];

    const char * ip = url + 4;
    if(strstr(ip, ":"))
    {
      unsigned __int128 ipv6;
      if (rr_parse_ipv6_decimal(ip, &ipv6) != 1)
      {
        res = MHD_create_response_empty(MHD_RF_NONE); 
        MHD_queue_response(connection, MHD_HTTP_NOT_FOUND, res);
        MHD_destroy_response(res);
        return MHD_YES;
      }

      if (!rr_db_get(&con))
        return MHD_NO;;        

      if (!rr_query_netblockv6_by_ip(con, ipv6, &info))
      {
        rr_db_put(&con);
        res = MHD_create_response_empty(MHD_RF_NONE); 
        MHD_queue_response(connection, MHD_HTTP_NOT_FOUND, res);
        MHD_destroy_response(res);
        return MHD_YES;      
      }

      inet_ntop(AF_INET6, &info.start_ip.v6, ipstring, sizeof(ipstring));
    }
    else
    {
      uint32_t ipv4;
      if (rr_parse_ipv4_decimal(ip, &ipv4) != 1)
      {
        res = MHD_create_response_empty(MHD_RF_NONE); 
        MHD_queue_response(connection, MHD_HTTP_NOT_FOUND, res);
        MHD_destroy_response(res);
        return MHD_YES;
      }

      if (!rr_db_get(&con))
        return MHD_NO;;

      if (!rr_query_netblockv4_by_ip(con, ipv4, &info))
      {
        rr_db_put(&con);
        res = MHD_create_response_empty(MHD_RF_NONE); 
        MHD_queue_response(connection, MHD_HTTP_NOT_FOUND, res);
        MHD_destroy_response(res);
        return MHD_YES;      
      }

      uint32_t netip = htonl(info.start_ip.v4);
      inet_ntop(AF_INET, &netip, ipstring, sizeof(ipstring));
    }

    char * buffer = malloc(16384);
    int n = snprintf(buffer, 16384,
      "netblock: %s/%d\n"
      "netname : %s\n"
      "org     : %s\n"
      "org_name: %s\n"
      "descr   : %s\n",
      ipstring,
      info.prefix_len,
      info.netname,
      info.org_id_str,
      info.org_name,
      info.descr
    );
    rr_db_put(&con);

    res = MHD_create_response_from_buffer_with_free_callback(n, buffer, &(free));
    MHD_add_response_header(res, "Content-Type", "text/plain");
    MHD_queue_response(connection, MHD_HTTP_OK, res);
    MHD_destroy_response(res);
    return MHD_YES;
  }

  return MHD_NO;
}

static void httpd_panic_handler(
  void *cls,
  const char *file,
  unsigned int line,
  const char *reason)
{
  LOG_ERROR("%s:%u - %s", file, line, reason);
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

  MHD_set_panic_func(httpd_panic_handler, NULL);
  struct MHD_Daemon *d = MHD_start_daemon(
    MHD_USE_THREAD_PER_CONNECTION,
    g_config.http.port,
    NULL,
    NULL,
    &httpd_handler, NULL,
    MHD_OPTION_END);
  if (!d)
  {

    LOG_ERROR("MHD_start_daemon failed");
    return EXIT_FAILURE;
  }

  import_thread(NULL);

  MHD_stop_daemon(d);
  rr_download_deinit(&dl);
  rr_db_deinit();
  rr_config_deinit();
  return EXIT_SUCCESS;
}