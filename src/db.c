#include "db.h"
#include "config.h"
#include "log.h"

#include <mysql.h>

static struct
{
  bool  initialized;
  MYSQL con;
}
db = { 0 };

bool rr_db_init(void)
{
  if (!mysql_init(&db.con))
  {
    LOG_ERROR("mysql_init failed");
    return false;
  }

  if (!mysql_real_connect(
    &db.con,
    g_config.database.port != 0 ? g_config.database.host : NULL,
    g_config.database.user,
    g_config.database.pass,
    g_config.database.name,
    g_config.database.port,
    g_config.database.port == 0 ? g_config.database.host : NULL,
    0)
  )
  {
    LOG_ERROR("%s", mysql_error(&db.con));
    return false;
  };

  db.initialized = true;
  return true;
}

void rr_db_deinit(void)
{
  if (!db.initialized)
    return;

  mysql_close(&db.con);
  db.initialized = false;
}