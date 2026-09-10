#include "http.h"
#include "log.h"
#include "config.h"
#include "util.h"
#include "query.h"

#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <microhttpd.h>
#include <yyjson.h>

#define HTTP_BULK_MAX_BODY            (16U * 1024U)
#define HTTP_BULK_MAX_IPS             100U
#define HTTP_STATUS_CONTENT_TOO_LARGE 413U

typedef struct RRHTTPHandler
{
  const char *route;
  int (*handler)(struct MHD_Connection *con, const char *uri);
}
RRHTTPHander;

typedef struct RRHTTPBulkIP
{
  const char *text;
  RRDBAddr    address;
  bool        ipv6;
}
RRHTTPBulkIP;

typedef struct RRHTTPUpload
{
  size_t   size;
  unsigned status;
  bool     responded;
  char     body[HTTP_BULK_MAX_BODY + 1];
}
RRHTTPUpload;

struct
{
  struct MHD_Daemon *daemon;
  struct
  {
    struct MHD_Response *r400;
    struct MHD_Response *r404;
    struct MHD_Response *r405;
    struct MHD_Response *r413;
    struct MHD_Response *r415;
    struct MHD_Response *r500;
  }
  response;
}
s_http = {};

static int http_handler_ip(struct MHD_Connection *con, const char *uri)
{
  struct MHD_Response *res;
  RRDBCon             *dbcon = NULL;
  RRDBIPInfo           info;
  char                 ipstring[64];
  int                  rc;

  if(strstr(uri, ":"))
  {
    unsigned __int128 ipv6;
    if (rr_parse_ipv6_decimal(uri, &ipv6) != 1)
      return 400;

    if (!rr_db_get(&dbcon))
      return 500;

    rc = rr_query_netblockv6_by_ip(dbcon, ipv6, &info);
    if (rc < 1)
    {
      rr_db_put(&dbcon);
      return rc < 0 ? 500 : 404;
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

    rc = rr_query_netblockv4_by_ip(dbcon, ipv4, &info);
    if (rc < 1)
    {
      rr_db_put(&dbcon);
      return rc < 0 ? 500 : 404;
    }
    rr_db_put(&dbcon);

    uint32_t netip = htonl(info.start_ip.v4);
    inet_ntop(AF_INET, &netip, ipstring, sizeof(ipstring));
  }

  char *buffer = malloc(16384);
  if (!buffer)
    return 500;

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

  if (n < 0 || n >= 16384)
  {
    free(buffer);
    return 500;
  }

  res = MHD_create_response_from_buffer_with_free_callback(n, buffer, &(free));
  if (!res)
  {
    free(buffer);
    return 500;
  }
  MHD_add_response_header(res, "Content-Type", "text/plain");
  MHD_queue_response(con, MHD_HTTP_OK, res);
  MHD_destroy_response(res);
  return 200;
}

static struct MHD_Response * http_response_for_status(unsigned status)
{
  switch(status)
  {
    case MHD_HTTP_BAD_REQUEST           : return s_http.response.r400;
    case MHD_HTTP_NOT_FOUND             : return s_http.response.r404;
    case MHD_HTTP_METHOD_NOT_ALLOWED    : return s_http.response.r405;
    case HTTP_STATUS_CONTENT_TOO_LARGE  : return s_http.response.r413;
    case MHD_HTTP_UNSUPPORTED_MEDIA_TYPE: return s_http.response.r415;
    case MHD_HTTP_INTERNAL_SERVER_ERROR : return s_http.response.r500;
    default                             : return s_http.response.r500;
  }
}

static enum MHD_Result http_queue_status(
  struct MHD_Connection *con,
  unsigned               status)
{
  return MHD_queue_response(con, status, http_response_for_status(status));
}

static bool http_content_type_is_json(const char *content_type)
{
  static const char expected[] = "application/json";

  if (!content_type || strncasecmp(content_type, expected, sizeof(expected) - 1) != 0)
    return false;

  content_type += sizeof(expected) - 1;
  while(*content_type == ' ' || *content_type == '\t')
    ++content_type;

  return *content_type == '\0' || *content_type == ';';
}

static bool http_bulk_ip_equal(const RRHTTPBulkIP *a, const RRHTTPBulkIP *b)
{
  if (a->ipv6 != b->ipv6)
    return false;

  return a->ipv6 ? a->address.v6 == b->address.v6 :
                   a->address.v4 == b->address.v4;
}

static int http_bulk_parse_ips(
  yyjson_doc   *doc,
  RRHTTPBulkIP *ips,
  size_t       *count)
{
  yyjson_val *root = yyjson_doc_get_root(doc);
  if (!yyjson_is_obj(root) || yyjson_obj_size(root) != 1)
    return MHD_HTTP_BAD_REQUEST;

  yyjson_val *array = yyjson_obj_get(root, "ips");
  if (!yyjson_is_arr(array))
    return MHD_HTTP_BAD_REQUEST;

  *count = yyjson_arr_size(array);
  if (*count == 0 || *count > HTTP_BULK_MAX_IPS)
    return MHD_HTTP_BAD_REQUEST;

  size_t      index;
  size_t      max;
  yyjson_val *value;
  yyjson_arr_foreach(array, index, max, value)
  {
    const char *text        = yyjson_get_str(value);
    size_t      text_length = yyjson_get_len(value);
    if (!text || text_length == 0 || text_length >= INET6_ADDRSTRLEN ||
        text_length != strlen(text))
      return MHD_HTTP_BAD_REQUEST;

    for(size_t offset = 0; offset < text_length; ++offset)
      if ((unsigned char)text[offset] > 0x7f)
        return MHD_HTTP_BAD_REQUEST;

    RRHTTPBulkIP *ip = &ips[index];
    ip->text          = text;
    ip->ipv6          = strchr(text, ':') != NULL;

    int rc = ip->ipv6 ? rr_parse_ipv6_decimal(text, &ip->address.v6) :
                        rr_parse_ipv4_decimal(text, &ip->address.v4);
    if (rc != 1)
      return MHD_HTTP_BAD_REQUEST;

    for(size_t previous = 0; previous < index; ++previous)
      if (http_bulk_ip_equal(ip, &ips[previous]))
        return MHD_HTTP_BAD_REQUEST;
  }

  return MHD_HTTP_OK;
}

static bool http_bulk_add_result(
  yyjson_mut_doc   *doc,
  yyjson_mut_val   *results,
  const char       *ip,
  const RRDBIPInfo *info,
  bool              ipv6,
  bool              found)
{
  yyjson_mut_val *result = yyjson_mut_obj(doc);
  if (!result || !yyjson_mut_arr_add_val(results, result) ||
      !yyjson_mut_obj_add_strcpy(doc, result, "ip", ip) ||
      !yyjson_mut_obj_add_bool(doc, result, "found", found))
    return false;

  if (!found)
    return true;

  char netblock[INET6_ADDRSTRLEN + 5];
  if (ipv6)
  {
    if (!inet_ntop(AF_INET6, &info->start_ip.v6, netblock, sizeof(netblock)))
      return false;
  }
  else
  {
    uint32_t netip = htonl(info->start_ip.v4);
    if (!inet_ntop(AF_INET, &netip, netblock, sizeof(netblock)))
      return false;
  }

  size_t len     = strlen(netblock);
  int    written = snprintf(
    netblock + len,
    sizeof(netblock) - len,
    "/%u",
    (unsigned)info->prefix_len);
  if (written < 0 || (size_t)written >= sizeof(netblock) - len)
    return false;

  return
    yyjson_mut_obj_add_strcpy(doc, result, "netblock"  , netblock        ) &&
    yyjson_mut_obj_add_strcpy(doc, result, "netname"   , info->netname   ) &&
    yyjson_mut_obj_add_strcpy(doc, result, "org_handle", info->org_handle) &&
    yyjson_mut_obj_add_strcpy(doc, result, "org_name"  , info->org_name  ) &&
    yyjson_mut_obj_add_strcpy(doc, result, "descr"     , info->descr     );
}

static int http_build_bulk_response(
  const char           *body,
  size_t                body_size,
  struct MHD_Response **response)
{
  int             status = MHD_HTTP_INTERNAL_SERVER_ERROR;
  yyjson_doc     *input  = NULL;
  yyjson_mut_doc *output = NULL;
  RRDBCon        *dbcon  = NULL;

  RRHTTPBulkIP ips[HTTP_BULK_MAX_IPS];
  size_t       count = 0;

  input = yyjson_read(body, body_size, 0);
  if (!input)
  {
    status = MHD_HTTP_BAD_REQUEST;
    goto done;
  }

  status = http_bulk_parse_ips(input, ips, &count);
  if (status != MHD_HTTP_OK)
    goto done;
  status = MHD_HTTP_INTERNAL_SERVER_ERROR;

  if (!rr_db_get(&dbcon))
  {
    status = MHD_HTTP_INTERNAL_SERVER_ERROR;
    goto done;
  }

  output = yyjson_mut_doc_new(NULL);
  if (!output)
    goto done;

  yyjson_mut_val *root    = yyjson_mut_obj(output);
  yyjson_mut_val *results = yyjson_mut_arr(output);
  if (!root || !results ||
      !yyjson_mut_obj_add_val(output, root, "results", results))
    goto done;
  yyjson_mut_doc_set_root(output, root);

  for(size_t i = 0; i < count; ++i)
  {
    RRDBIPInfo info = { 0 };
    int        rc   = ips[i].ipv6 ?
      rr_query_netblockv6_by_ip(dbcon, ips[i].address.v6, &info) :
      rr_query_netblockv4_by_ip(dbcon, ips[i].address.v4, &info);
    if (rc < 0)
      goto done;

    if (!http_bulk_add_result(
      output,
      results,
      ips[i].text,
      &info,
      ips[i].ipv6,
      rc == 1))
      goto done;
  }
  rr_db_put(&dbcon);

  size_t  json_size = 0;
  char   *json      = yyjson_mut_write(output, YYJSON_WRITE_NOFLAG, &json_size);
  if (!json)
    goto done;

  *response = MHD_create_response_from_buffer_with_free_callback(
    json_size,
    json,
    &(free));
  if (!*response)
  {
    free(json);
    goto done;
  }

  if (MHD_add_response_header(*response, "Content-Type", "application/json") != MHD_YES)
  {
    MHD_destroy_response(*response);
    *response = NULL;
    goto done;
  }

  status = MHD_HTTP_OK;

done:
  if (dbcon)
    rr_db_put(&dbcon);
  if (output)
    yyjson_mut_doc_free(output);
  yyjson_doc_free(input);
  return status;
}

static enum MHD_Result http_handler_ip_bulk(
  struct MHD_Connection *con,
  const char            *method,
  const char            *upload_data,
  size_t                *upload_data_size,
  void                 **ptr)
{
  RRHTTPUpload *upload = *ptr;
  if (!upload)
  {
    if (strcmp(method, "POST") != 0)
      return http_queue_status(con, MHD_HTTP_METHOD_NOT_ALLOWED);

    const char *content_type = MHD_lookup_connection_value(
      con,
      MHD_HEADER_KIND,
      MHD_HTTP_HEADER_CONTENT_TYPE);
    if (!http_content_type_is_json(content_type))
      return http_queue_status(con, MHD_HTTP_UNSUPPORTED_MEDIA_TYPE);

    upload = calloc(1, sizeof(*upload));
    if (!upload)
      return http_queue_status(con, MHD_HTTP_INTERNAL_SERVER_ERROR);

    *ptr = upload;
    return MHD_YES;
  }

  if (upload->responded)
  {
    *upload_data_size = 0;
    return MHD_YES;
  }

  if (*upload_data_size > 0)
  {
    if (upload->status == 0)
    {
      if (!upload_data)
        upload->status = MHD_HTTP_BAD_REQUEST;
      else if (*upload_data_size > HTTP_BULK_MAX_BODY - upload->size)
        upload->status = HTTP_STATUS_CONTENT_TOO_LARGE;
      else
      {
        memcpy(upload->body + upload->size, upload_data, *upload_data_size);
        upload->size += *upload_data_size;
        upload->body[upload->size] = '\0';
      }
    }

    *upload_data_size = 0;
    return MHD_YES;
  }

  upload->responded = true;
  if (upload->status != 0)
    return http_queue_status(con, upload->status);

  struct MHD_Response *response = NULL;
  int                  status   = http_build_bulk_response(
    upload->body,
    upload->size,
    &response);
  if (status != MHD_HTTP_OK)
    return http_queue_status(con, status);

  enum MHD_Result result = MHD_queue_response(con, MHD_HTTP_OK, response);
  MHD_destroy_response(response);
  return result;
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
  if (strcmp(url, "/ip/bulk") == 0)
    return http_handler_ip_bulk(
      con,
      method,
      upload_data,
      upload_data_size,
      ptr);

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

static void httpd_request_completed(
  void                            *cls,
  struct MHD_Connection           *con,
  void                           **ptr,
  enum MHD_RequestTerminationCode  toe)
{
  (void)cls;
  (void)con;
  (void)toe;

  free(*ptr);
  *ptr = NULL;
}

bool rr_http_init(void)
{
  static const char *r400 = "400 - Bad Request\n";
  static const char *r404 = "404 - Not Found\n";
  static const char *r405 = "405 - Method Not Allowed\n";
  static const char *r413 = "413 - Content Too Large\n";
  static const char *r415 = "415 - Unsupported Media Type\n";
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
  s_http.response.r413 =
    MHD_create_response_from_buffer_with_free_callback(strlen(r413), (char *)r413, rr_http_noop_free);
  MHD_add_response_header(s_http.response.r413, "Content-Type", "text/plain");
  s_http.response.r415 =
    MHD_create_response_from_buffer_with_free_callback(strlen(r415), (char *)r415, rr_http_noop_free);
  MHD_add_response_header(s_http.response.r415, "Content-Type", "text/plain");
  s_http.response.r500 =
    MHD_create_response_from_buffer_with_free_callback(strlen(r500), (char *)r500, rr_http_noop_free);
  MHD_add_response_header(s_http.response.r500, "Content-Type", "text/plain");

  MHD_set_panic_func(httpd_panic_handler, NULL);
  s_http.daemon = MHD_start_daemon(
    MHD_USE_THREAD_PER_CONNECTION,
    g_config.http.port,
    NULL,
    NULL,
    &httpd_handler,
    NULL,
    MHD_OPTION_NOTIFY_COMPLETED,
    httpd_request_completed,
    NULL,
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
  MHD_destroy_response(s_http.response.r413);
  MHD_destroy_response(s_http.response.r415);
  MHD_destroy_response(s_http.response.r500);
}
