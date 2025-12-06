#include "log.h"
#include "config.h"
#include "download.h"
#include "db.h"
#include "rpsl.h"
#include "arin.h"
#include "util.h"

#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include <pthread.h>
#include <microhttpd.h>

typedef struct DBConnectionData
{
  struct
  {
    RRDBStmt  *stmt;
    uint32_t   in_ipv4;
    RRDBIPInfo out;
  }
  lookupIPv4;

  struct
  {
    RRDBStmt           *stmt;
    unsigned __int128   in_ipv6;
    RRDBIPInfo          out;
  }
  lookupIPv6;  
}
DBConnectionData;

bool running = true;
RRDownload *dl = NULL;
static void * import_thread(void *)
{
  while(running)
  {
    RRDBCon *con;
    DBConnectionData *condata;
    if (!rr_db_get(&con, (void **)&condata))
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
      unsigned serial = 0;
      unsigned last_import = 0;
      unsigned registrar_id =
        rr_db_get_registrar_id(con, src->name, true, &serial, &last_import);
      if (registrar_id == 0)
      {
        LOG_ERROR("Failed to get the registrar id");
        rr_db_rollback(con);
        goto next_con;
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
        goto next_con;
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

    next_con:
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
    RRDBCon          *con;
    DBConnectionData *condata;
    RRDBIPInfo       *info;
    char   ipstring[64];

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

      if (!rr_db_get(&con, (void **)&condata))
        return MHD_NO;;

      condata->lookupIPv6.in_ipv6 = ipv6;
      if (!rr_db_stmt_fetch_one(condata->lookupIPv6.stmt))
      {
        rr_db_put(&con);
        res = MHD_create_response_empty(MHD_RF_NONE); 
        MHD_queue_response(connection, MHD_HTTP_NOT_FOUND, res);
        MHD_destroy_response(res);
        return MHD_YES;
      }

      inet_ntop(AF_INET6, &condata->lookupIPv6.out.start_ip.v6,
        ipstring, sizeof(ipstring));
      info = &condata->lookupIPv6.out;        
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

      if (!rr_db_get(&con, (void **)&condata))
        return MHD_NO;;

      condata->lookupIPv4.in_ipv4 = ipv4;
      if (!rr_db_stmt_fetch_one(condata->lookupIPv4.stmt))
      {
        rr_db_put(&con);
        res = MHD_create_response_empty(MHD_RF_NONE); 
        MHD_queue_response(connection, MHD_HTTP_NOT_FOUND, res);
        MHD_destroy_response(res);
        return MHD_YES;
      }

      uint32_t netip = htonl(condata->lookupIPv4.out.start_ip.v4);
      inet_ntop(AF_INET, &netip, ipstring, sizeof(ipstring));
      info = &condata->lookupIPv4.out;
    }

    char * buffer = malloc(16384);
    int n = snprintf(buffer, 16384,
      "netblock: %s/%d\n"
      "netname : %s\n"
      "org     : %s\n"
      "org_name: %s\n"
      "descr   : %s\n",
      ipstring,
      info->prefix_len,
      info->netname,
      info->org_id_str,
      info->org_name,
      info->descr
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

static bool db_udata_init(RRDBCon *con, void **udata)
{
  DBConnectionData *condata = calloc(1, sizeof(*condata));
  if (!condata)
  {
    LOG_ERROR("out of memory");
    return false;
  }
  *udata = condata;

  if (!rr_db_prepare_lookup_ipv4(con,
    &condata->lookupIPv4.stmt,
    &condata->lookupIPv4.in_ipv4,
    &condata->lookupIPv4.out))
  {
    LOG_ERROR("rr_db_prepare_lookup_ipv4 failed");
    return false;
  }

  if (!rr_db_prepare_lookup_ipv6(con,
    &condata->lookupIPv6.stmt,
    &condata->lookupIPv6.in_ipv6,
    &condata->lookupIPv6.out))
  {
    LOG_ERROR("rr_db_prepare_lookup_ipv6 failed");
    return false;
  }

  return true;
}

static bool db_udata_deinit(RRDBCon *con, void **udata)
{
  if (!*udata)
    return true;

  DBConnectionData *condata = *udata;

  rr_db_stmt_free(&condata->lookupIPv4.stmt);
  rr_db_stmt_free(&condata->lookupIPv6.stmt);

  free(condata);
  *udata = NULL;  
  return true;
}

int main(int argc, char *argv[])
{
  rr_log_init();
  if (!rr_config_init())
    LOG_WARN("Failed to load config, using defaults");

  if (!rr_db_init(db_udata_init, db_udata_deinit))
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