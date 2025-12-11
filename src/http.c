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
  RRDBCon   *dbcon = NULL;
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
    "netblock  : %s/%d\n"
    "netname   : %s\n"
    "org_handle: %s\n"
    "org_name  : %s\n"
    "descr     : %s\n",
    ipstring,
    info.prefix_len,
    info.netname,
    info.org_handle,
    info.org_name,
    info.descr
  );

  res = MHD_create_response_from_buffer_with_free_callback(n, buffer, &(free));
  MHD_add_response_header(res, "Content-Type", "text/plain");
  MHD_queue_response(con, MHD_HTTP_OK, res);
  MHD_destroy_response(res);
  return 200;
}

static ssize_t http_handler_list_v4_cb_reader(void *cls, uint64_t pos, char *buf, size_t max)
{
  RRDBCon *dbcon = cls;
  uint32_t ip;
  uint8_t  prefix_len;

  const size_t maxLineLen = 19; //ipv4 + / + 2 + \n
  ssize_t out = 0;
  while(max >= maxLineLen)
  {
    int rc = rr_query_netblockv4_list_union_fetch(dbcon, &ip, &prefix_len);
    if (rc == 0)
      break;

    if (rc < 0)
      return MHD_CONTENT_READER_END_WITH_ERROR;

    ip = htonl(ip);
    inet_ntop(AF_INET, &ip, buf, max);
    size_t len = strlen(buf);
    len += sprintf(buf + len, "/%d\n", prefix_len);

    buf += len;
    out += len;
    max -= len;
  }

  if (out == 0)
    return MHD_CONTENT_READER_END_OF_STREAM;

  return out;
}

static void http_handler_list_v4_cb_free(void *cls)
{
  RRDBCon *dbcon = cls;
  rr_query_netblockv4_list_union_end(dbcon);
  rr_db_put(&dbcon);
}

static int http_handler_list_v4(struct MHD_Connection *con, const char *uri)
{
  bool found = false;
  for(ConfigList * list = g_config.lists; list->name; ++list)
  {
    if (!list->build_list)
      continue;

    if (strcmp(list->name, uri) == 0)
    {
      found = true;
      break;
    }
  }

  if (!found)
    return 404;

  RRDBCon *dbcon = NULL;
  if (!rr_db_get(&dbcon))
    return 500;

  unsigned list_id;
  if (rr_query_list_by_name(dbcon, uri, &list_id) != 1)
  {
    rr_db_put(&dbcon);
    return 500;
  }

  if (!rr_query_netblockv4_list_union_start(dbcon, list_id, false))
  {
    rr_db_put(&dbcon);
    return 500;
  }

  struct MHD_Response *resp = MHD_create_response_from_callback(
    MHD_SIZE_UNKNOWN,
    64 * 1024,
    http_handler_list_v4_cb_reader,
    dbcon,
    http_handler_list_v4_cb_free);

  if (!resp)
    return 500;

  MHD_add_response_header(resp, "Content-Type", "text/plain");
  if (MHD_queue_response(con, MHD_HTTP_OK, resp) != MHD_YES)
  {
    MHD_destroy_response(resp);
    return 500;
  }
  MHD_destroy_response(resp);
  return 200;
}

static ssize_t http_handler_list_v6_cb_reader(void *cls, uint64_t pos, char *buf, size_t max)
{
  RRDBCon *dbcon = cls;
  unsigned __int128 ip;
  uint8_t  prefix_len;

  const size_t maxLineLen = 44; //ipv6 + / + 3 + \n
  ssize_t out = 0;
  while(max >= maxLineLen)
  {
    int rc = rr_query_netblockv6_list_union_fetch(dbcon, &ip, &prefix_len);
    if (rc == 0)
      break;

    if (rc < 0)
      return MHD_CONTENT_READER_END_WITH_ERROR;

    inet_ntop(AF_INET6, &ip, buf, max);
    size_t len = strlen(buf);
    len += sprintf(buf + len, "/%d\n", prefix_len);

    buf += len;
    out += len;
    max -= len;
  }

  if (out == 0)
    return MHD_CONTENT_READER_END_OF_STREAM;

  return out;
}

static void http_handler_list_v6_cb_free(void *cls)
{
  RRDBCon *dbcon = cls;
  rr_query_netblockv6_list_union_end(dbcon);
  rr_db_put(&dbcon);
}

static int http_handler_list_v6(struct MHD_Connection *con, const char *uri)
{
  bool found = false;
  for(ConfigList * list = g_config.lists; list->name; ++list)
  {
    if (strcmp(list->name, uri) == 0)
    {
      found = true;
      break;
    }
  }

  if (!found)
    return 404;

  RRDBCon *dbcon = NULL;
  if (!rr_db_get(&dbcon))
    return 500;

  unsigned list_id;
  if (rr_query_list_by_name(dbcon, uri, &list_id) != 1)
  {
    rr_db_put(&dbcon);
    return 500;
  }

  if (!rr_query_netblockv6_list_union_start(dbcon, list_id, false))
  {
    rr_db_put(&dbcon);
    return 500;
  }

  struct MHD_Response *resp = MHD_create_response_from_callback(
    MHD_SIZE_UNKNOWN,
    1024,
    http_handler_list_v6_cb_reader,
    dbcon,
    http_handler_list_v6_cb_free);

  if (!resp)
    return 500;

  MHD_add_response_header(resp, "Content-Type", "text/plain");
  if (MHD_queue_response(con, MHD_HTTP_OK, resp) != MHD_YES)
  {
    MHD_destroy_response(resp);
    return 500;
  }
  MHD_destroy_response(resp);
  return 200;
}

static RRHTTPHander s_handlers[] =
{
  { "/ip/"     , http_handler_ip      },
  { "/list/v4/", http_handler_list_v4 },
  { "/list/v6/", http_handler_list_v6 }
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

static void rr_http_noop_free(void *cls)
{
  (void)cls;
}

bool rr_http_init(void)
{
  static const char *r400 = "400 - Bad Request\n";
  static const char *r404 = "404 - Not Found\n";
  static const char *r405 = "405 - Method Not Allowed\n";
  static const char *r500 = "500 - Internal Server Error\n";

  /*
    MHD_create_response_from_buffer_static doesn't exist in older version of microhttpd so we
    emulate it by providing a no-op free callback
  */

  s_http.response.r400 =
    MHD_create_response_from_buffer_with_free_callback(strlen(r400), (char *)r400, rr_http_noop_free);
  MHD_add_response_header(s_http.response.r400, "Content-Type", "text/plain");
  s_http.response.r404 =
    MHD_create_response_from_buffer_with_free_callback(strlen(r404), (char *)r404, rr_http_noop_free);
  MHD_add_response_header(s_http.response.r404, "Content-Type", "text/plain");
  s_http.response.r405 =
    MHD_create_response_from_buffer_with_free_callback(strlen(r405), (char *)r405, rr_http_noop_free);
  MHD_add_response_header(s_http.response.r405, "Content-Type", "text/plain");
  s_http.response.r500 =
    MHD_create_response_from_buffer_with_free_callback(strlen(r500), (char *)r500, rr_http_noop_free);
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
