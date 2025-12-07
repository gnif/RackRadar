#include "http.h"
#include "log.h"
#include "config.h"
#include "util.h"
#include "query.h"

#include <string.h>
#include <microhttpd.h>

typedef struct RRHTTPHandler
{
  const char *route;
  int (*handler)(struct MHD_Connection *con, const char *uri);
}
RRHTTPHander;

struct
{
  struct MHD_Daemon *daemon;
  struct
  {
    struct MHD_Response *r400;
    struct MHD_Response *r404;
    struct MHD_Response *r405;
    struct MHD_Response *r500;    
  }
  response;
}
s_http = {};

static int http_handler_ip(struct MHD_Connection *con, const char *uri)
{
  struct MHD_Response *res;
  RRDBCon   *dbcon;
  RRDBIPInfo info;
  char       ipstring[64];

  if(strstr(uri, ":"))
  {
    unsigned __int128 ipv6;
    if (rr_parse_ipv6_decimal(uri, &ipv6) != 1)
      return 400;

    if (!rr_db_get(&dbcon))
      return 500;

    if (!rr_query_netblockv6_by_ip(dbcon, ipv6, &info))
    {
      rr_db_put(&dbcon);
      return 404;
    }
    rr_db_put(&dbcon);

    inet_ntop(AF_INET6, &info.start_ip.v6, ipstring, sizeof(ipstring));
  }
  else
  {
    uint32_t ipv4;
    if (rr_parse_ipv4_decimal(uri, &ipv4) != 1)
      return 400;

    if (!rr_db_get(&dbcon))
      return 500;

    if (!rr_query_netblockv4_by_ip(dbcon, ipv4, &info))
    {
      rr_db_put(&dbcon);
      return 404;
    }
    rr_db_put(&dbcon);    

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

  res = MHD_create_response_from_buffer_with_free_callback(n, buffer, &(free));
  MHD_add_response_header(res, "Content-Type", "text/plain");
  MHD_queue_response(con, MHD_HTTP_OK, res);
  MHD_destroy_response(res);
  return 200;
}

static int http_handler_org(struct MHD_Connection *con, const char *uri)
{
  LOG_INFO(uri);
  return false;
}

static RRHTTPHander s_handlers[] =
{
  { "/ip/" , http_handler_ip  },
  { "/org/", http_handler_org },
};

static enum MHD_Result httpd_handler(
  void *cls,
  struct MHD_Connection *con,
  const char *url,
  const char *method,
  const char *version,
  const char *upload_data,
  size_t *upload_data_size,
  void **ptr)
{
  if (strcmp(method, "GET") != 0)
  {
    MHD_queue_response(con,
      MHD_HTTP_METHOD_NOT_ALLOWED, s_http.response.r405);
    return MHD_YES;
  }

  for(unsigned i = 0; i < ARRAY_SIZE(s_handlers); ++i)
  {
    RRHTTPHander *h = &s_handlers[i];
    int len = strlen(h->route);
    if (strncmp(h->route, url, len) == 0)
    {
      switch(h->handler(con, url + len))
      {
        case 200:
          break;

        case 400:
          MHD_queue_response(con,
            MHD_HTTP_BAD_REQUEST, s_http.response.r400);
          break;

        case 404:
          MHD_queue_response(con,
            MHD_HTTP_NOT_FOUND, s_http.response.r404);
          break;

        case 405:
          MHD_queue_response(con,
            MHD_HTTP_METHOD_NOT_ALLOWED, s_http.response.r405);
          break;

        case 500:
        default:
          MHD_queue_response(con,
                  MHD_HTTP_INTERNAL_SERVER_ERROR, s_http.response.r500);
          break;
      }
      
      return MHD_YES;
    }
  }
  MHD_queue_response(con,
    MHD_HTTP_NOT_FOUND, s_http.response.r404);
  return MHD_YES;
}

static void httpd_panic_handler(
  void *cls,
  const char *file,
  unsigned int line,
  const char *reason)
{
  LOG_ERROR("%s:%u - %s", file, line, reason);
}

bool rr_http_init(void)
{
  static const char *r400 = "400 - Bad Request";
  static const char *r404 = "404 - Not Found";
  static const char *r405 = "405 - Method Not Allowed";
  static const char *r500 = "500 - Internal Server Error";

  s_http.response.r400 =
    MHD_create_response_from_buffer_static(strlen(r400), r400);
  MHD_add_response_header(s_http.response.r400, "Content-Type", "text/plain");
  s_http.response.r404 =
    MHD_create_response_from_buffer_static(strlen(r404), r404);
  MHD_add_response_header(s_http.response.r404, "Content-Type", "text/plain");
  s_http.response.r405 =
    MHD_create_response_from_buffer_static(strlen(r405), r405);
  MHD_add_response_header(s_http.response.r405, "Content-Type", "text/plain");    
  s_http.response.r500 =
    MHD_create_response_from_buffer_static(strlen(r500), r500);
  MHD_add_response_header(s_http.response.r500, "Content-Type", "text/plain");    

  MHD_set_panic_func(httpd_panic_handler, NULL);
  s_http.daemon = MHD_start_daemon(
    MHD_USE_THREAD_PER_CONNECTION,
    g_config.http.port,
    NULL,
    NULL,
    &httpd_handler, NULL,
    MHD_OPTION_END);
  if (!s_http.daemon)
  {
    LOG_ERROR("MHD_start_daemon failed");
    return false;
  }

  return true;
}

void rr_http_deinit(void)
{
  MHD_stop_daemon(s_http.daemon);
  MHD_destroy_response(s_http.response.r400);
  MHD_destroy_response(s_http.response.r404);
  MHD_destroy_response(s_http.response.r405);
  MHD_destroy_response(s_http.response.r500);
}