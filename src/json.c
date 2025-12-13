#include <yyjson.h>

#include "log.h"
#include "util.h"
#include "import.h"

bool rr_json_import_FILE(const char *registrar, FILE *fp,
  unsigned registrar_id, unsigned new_serial,
  const char *extra_v4, const char *extra_v6)
{
  size_t extra_v4_len = extra_v4 ? strlen(extra_v4) : 0;
  size_t extra_v6_len = extra_v6 ? strlen(extra_v6) : 0;

  yyjson_read_err rerr;
  yyjson_doc *doc = yyjson_read_fp(fp, 0, NULL, &rerr);
  if (!doc)
  {
    LOG_ERROR("yyjson_read_fp failed - code:%u pos:%lu msg:%s",
      rerr.code, rerr.pos, rerr.msg);
    return false;
  }

  yyjson_val *root = yyjson_doc_get_root(doc);

  RRDBNetBlock netblock =
  {
    .registrar_id = registrar_id,
    .serial       = new_serial
  };

  char buffer[128];
  char path[128], field[128];
  yyjson_val *base, *elem;
  size_t idx, max;

  if (extra_v4_len > 0 && sscanf(extra_v4, "%127[^#]#%127[^\n]", path, field) == 2)
  {
    base = yyjson_ptr_get(root, path);
    yyjson_arr_foreach(base, idx, max, elem)
    {
      yyjson_val *v4 = yyjson_obj_get(elem, field);
      if (!v4)
        continue;

      const char *entry = yyjson_get_str(v4);
      if (!entry)
        continue;

      strncpy(buffer, entry, sizeof(buffer));
      char *savePtr;
      const char *addr = strtok_r(buffer, "/", &savePtr);
      if (!addr)
        continue;

      const char *prefix_len = strtok_r(NULL, "/", &savePtr);
      if (!prefix_len)
        continue;

      if (!rr_parse_ipv4_decimal(addr, &netblock.startAddr.v4))
        continue;

      netblock.prefixLen = strtoul(prefix_len, NULL, 10);
      if (netblock.prefixLen == 0 || netblock.prefixLen > 32)
        continue;

      if (!rr_calc_ipv4_cidr_end(netblock.startAddr.v4, netblock.prefixLen, &netblock.endAddr.v4))
        continue;

      if (!rr_import_netblockv4_insert(&netblock))
      {
        yyjson_doc_free(doc);
        return false;
      }
    }
  }

  if (extra_v6_len > 0 && sscanf(extra_v6, "%127[^#]#%127[^\n]", path, field) == 2)
  {
    base = yyjson_ptr_get(root, path);
    if (!base)
      return false;

    yyjson_arr_foreach(base, idx, max, elem)
    {
      yyjson_val *v6 = yyjson_obj_get(elem, field);
      if (!v6)
        continue;

      const char *entry = yyjson_get_str(v6);
      if (!entry)
        continue;

      strncpy(buffer, entry, sizeof(buffer));
      char *savePtr;
      const char *addr = strtok_r(buffer, "/", &savePtr);
      if (!addr)
        continue;

      const char *prefix_len = strtok_r(NULL, "/", &savePtr);
      if (!prefix_len)
        continue;

      if (!rr_parse_ipv6_decimal(addr, &netblock.startAddr.v6))
        continue;

      netblock.prefixLen = strtoul(prefix_len, NULL, 10);
      if (netblock.prefixLen == 0 || netblock.prefixLen > 64)
        continue;

      if (!rr_calc_ipv6_cidr_end(&netblock.startAddr.v6, netblock.prefixLen, &netblock.endAddr.v6))
        continue;

      if (!rr_import_netblockv6_insert(&netblock))
      {
        yyjson_doc_free(doc);
        return false;
      }
    }
  }

  yyjson_doc_free(doc);
  return true;
}
