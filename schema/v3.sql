CREATE TABLE IF NOT EXISTS org_stage
(
  registrar_id INT UNSIGNED NOT NULL,
  handle       VARCHAR(32)  NOT NULL,
  name         TEXT         NOT NULL,
  descr        TEXT         NULL,

  PRIMARY KEY(registrar_id, handle)
)
ENGINE          = InnoDB
DEFAULT CHARSET = utf8mb4
COLLATE         = utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS netblock_v4_stage
(
  registrar_id INT     UNSIGNED NOT NULL,
  org_handle   VARCHAR(32)      NOT NULL DEFAULT '',
  start_ip     INT     UNSIGNED NOT NULL,
  end_ip       INT     UNSIGNED NOT NULL,
  prefix_len   TINYINT UNSIGNED NOT NULL,
  netname      VARCHAR(255)     NOT NULL,
  descr        TEXT             NOT NULL,

  PRIMARY KEY(registrar_id, org_handle, start_ip, end_ip),

  CONSTRAINT chk_netblock_v4_stage_range  CHECK (start_ip <= end_ip),
  CONSTRAINT chk_netblock_v4_stage_prefix CHECK (prefix_len BETWEEN 0 AND 32)
)
ENGINE          = InnoDB
DEFAULT CHARSET = utf8mb4
COLLATE         = utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS netblock_v6_stage
(
  registrar_id INT     UNSIGNED NOT NULL,
  org_handle   VARCHAR(32)      NOT NULL DEFAULT '',
  start_ip     BINARY(16)       NOT NULL,
  end_ip       BINARY(16)       NOT NULL,
  prefix_len   TINYINT UNSIGNED NOT NULL,
  netname      VARCHAR(255)     NOT NULL,
  descr        TEXT             NOT NULL,

  PRIMARY KEY(registrar_id, org_handle, start_ip, end_ip),

  CONSTRAINT chk_netblock_v6_stage_prefix CHECK (prefix_len BETWEEN 0 AND 128)
)
ENGINE          = InnoDB
DEFAULT CHARSET = utf8mb4
COLLATE         = utf8mb4_unicode_ci;
