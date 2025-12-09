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

int  rr_query_list_by_name(RRDBCon *con, const char *in_name, unsigned *out_list_id);
bool rr_query_netblockv4_list_start(RRDBCon *con, unsigned in_list_id, bool store);
int  rr_query_netblockv4_list_fetch(RRDBCon *con, uint32_t *out_start_ip, uint32_t *out_end_ip, uint8_t *out_prefix_len);
void rr_query_netblockv4_list_end  (RRDBCon *con);

bool rr_query_netblockv6_list_start(RRDBCon *con, unsigned in_list_id, bool store);
int rr_query_netblockv6_list_fetch(RRDBCon *con, unsigned __int128 *out_start_ip, unsigned __int128 *out_end_ip, uint8_t *out_prefix_len);
void rr_query_netblockv6_list_end  (RRDBCon *con);

bool rr_query_netblockv4_list_union_start(RRDBCon *con, unsigned in_list_id, bool store);
int rr_query_netblockv4_list_union_fetch (RRDBCon *con, uint32_t *out_ip, uint8_t *out_prefix_len);
void rr_query_netblockv4_list_union_end  (RRDBCon *con);

bool rr_query_netblockv6_list_union_start(RRDBCon *con, unsigned in_list_id, bool store);
int rr_query_netblockv6_list_union_fetch (RRDBCon *con, unsigned __int128 *out_ip, uint8_t *out_prefix_len);
void rr_query_netblockv6_list_union_end  (RRDBCon *con);

#endif
