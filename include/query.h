#ifndef _H_RR_QUERY_
#define _H_RR_QUERY_

#include "db.h"
#include "db_structs.h"

bool rr_query_init(RRDBCon *con, void **udata);
bool rr_query_deinit(RRDBCon *con, void **udata);

int rr_query_registrar_by_name(
  RRDBCon *con,
  const char *in_name,
  unsigned *out_registrar_id,
  unsigned *out_serial,
  unsigned *out_last_import);

int rr_query_registrar_insert(
  RRDBCon *con,
  const char *in_name,
  unsigned *out_registrar_id);

int rr_query_netblockv4_by_ip(
  RRDBCon *con,
  uint32_t in_ipv4,
  RRDBIPInfo *out);

int rr_query_netblockv6_by_ip(
  RRDBCon           *con,
  unsigned __int128  in_ipv6,
  RRDBIPInfo        *out);

#endif
