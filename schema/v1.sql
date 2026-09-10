CREATE TABLE IF NOT EXISTS registrar
(
  id                   INT UNSIGNED  NOT NULL AUTO_INCREMENT,
  name                 VARCHAR(32)   NOT NULL,
  serial               INT UNSIGNED  NOT NULL DEFAULT 0,
  last_import          INT UNSIGNED  NOT NULL,
  last_check           INT UNSIGNED  NOT NULL DEFAULT 0,
  source_config_hash   CHAR(64)      CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '',
  source_content_hash  CHAR(64)      CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '',
  source_etag          VARCHAR(1024) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '',
  source_last_modified VARCHAR(128)  CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '',

  PRIMARY KEY(id),

  CONSTRAINT uk_registrar_name
    UNIQUE INDEX(name)
)
ENGINE          = InnoDB
DEFAULT CHARSET = utf8mb4
COLLATE         = utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS import_state
(
  id           TINYINT UNSIGNED NOT NULL,
  unions_dirty TINYINT UNSIGNED NOT NULL DEFAULT 1,

  PRIMARY KEY(id),

  CONSTRAINT chk_import_state_singleton CHECK (id = 1)
)
ENGINE          = InnoDB
DEFAULT CHARSET = utf8mb4
COLLATE         = utf8mb4_unicode_ci;

INSERT IGNORE INTO import_state (id) VALUES (1);

CREATE TABLE IF NOT EXISTS org
(
  id           INT UNSIGNED NOT NULL AUTO_INCREMENT,
  registrar_id INT UNSIGNED NOT NULL,
  serial       INT UNSIGNED NOT NULL,
  handle       VARCHAR(32)  NOT NULL,
  name         TEXT         NOT NULL,
  descr        TEXT         NULL,

  PRIMARY KEY(id),

  KEY idx_serial(serial),

  CONSTRAINT uk_org_registrar_id_handle
    UNIQUE INDEX(registrar_id, handle),

  CONSTRAINT fk_org_registrar
    FOREIGN KEY (registrar_id) REFERENCES registrar(id)
    ON UPDATE RESTRICT ON DELETE CASCADE
)
ENGINE          = InnoDB
DEFAULT CHARSET = utf8mb4
COLLATE         = utf8mb4_unicode_ci;

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

CREATE TABLE IF NOT EXISTS netblock_v4
(
  id           BIGINT  UNSIGNED NOT NULL AUTO_INCREMENT,
  registrar_id INT     UNSIGNED NOT NULL,
  serial       INT     UNSIGNED NOT NULL,
  org_id       INT     UNSIGNED NULL DEFAULT NULL,
  org_handle   VARCHAR(32)      NOT NULL DEFAULT '',
  start_ip     INT     UNSIGNED NOT NULL,
  end_ip       INT     UNSIGNED NOT NULL,
  prefix_len   TINYINT UNSIGNED NOT NULL,
  netname      VARCHAR(255)     NOT NULL,
  descr        TEXT             NOT NULL,

  PRIMARY KEY(id),

  KEY idx_serial     (serial),
  KEY idx_start      (start_ip),
  KEY idx_start_end  (start_ip, end_ip),
  KEY idx_end_start  (end_ip  , start_ip),
  KEY idx_registrar  (registrar_id),
  KEY idx_org_id     (org_id),
  KEY idx_org_handle (org_handle),
  KEY idx_netname    (netname),

  CONSTRAINT uk_netblock_v4_constraint
    UNIQUE INDEX (registrar_id, org_handle, start_ip, end_ip),

  CONSTRAINT chk_netblock_v4_range  CHECK (start_ip <= end_ip),
  CONSTRAINT chk_netblock_v4_prefix CHECK (prefix_len BETWEEN 0 AND 32),

  CONSTRAINT fk_netblock_v4_registrar
    FOREIGN KEY (registrar_id) REFERENCES registrar(id)
    ON UPDATE RESTRICT ON DELETE CASCADE
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

CREATE TABLE IF NOT EXISTS netblock_v4_union
(
  start_ip INT UNSIGNED NOT NULL,
  end_ip   INT UNSIGNED NOT NULL,
  PRIMARY KEY (start_ip),
  KEY idx_end (end_ip),
  CONSTRAINT chk_netblock_v4_union_union CHECK (start_ip <= end_ip)
)
ENGINE          = InnoDB
DEFAULT CHARSET = utf8mb4
COLLATE         = utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS netblock_v4_union_next
(
  start_ip INT UNSIGNED NOT NULL,
  end_ip   INT UNSIGNED NOT NULL,
  PRIMARY KEY (start_ip),
  CONSTRAINT chk_netblock_v4_union_next_range CHECK (start_ip <= end_ip)
)
ENGINE          = InnoDB
DEFAULT CHARSET = utf8mb4
COLLATE         = utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS netblock_v6
(
  id           BIGINT  UNSIGNED NOT NULL AUTO_INCREMENT,
  registrar_id INT     UNSIGNED NOT NULL,
  serial       INT     UNSIGNED NOT NULL,
  org_id       INT     UNSIGNED NULL,
  org_handle   VARCHAR(32)      NOT NULL DEFAULT '',
  start_ip     BINARY(16)       NOT NULL,
  end_ip       BINARY(16)       NOT NULL,
  prefix_len   TINYINT UNSIGNED NOT NULL,
  netname      VARCHAR(255)     NOT NULL,
  descr        TEXT             NOT NULL,

  PRIMARY KEY(id),

  KEY idx_serial     (serial),
  KEY idx_start_end  (start_ip, end_ip),
  KEY idx_end_start  (end_ip  , start_ip),
  KEY idx_registrar  (registrar_id),
  KEY idx_org_id     (org_id),
  KEY idx_org_handle (org_handle),
  KEY idx_netname    (netname),

  CONSTRAINT uk_netblock_v6_constraint
    UNIQUE INDEX (registrar_id, org_handle, start_ip, end_ip),

  CONSTRAINT chk_netblock_v6_prefix CHECK (prefix_len BETWEEN 0 AND 128),

  CONSTRAINT fk_netblock_v6_registrar
    FOREIGN KEY (registrar_id) REFERENCES registrar(id)
    ON UPDATE RESTRICT ON DELETE CASCADE,

  CONSTRAINT fk_netblock_v6_org
    FOREIGN KEY (org_id) REFERENCES org(id)
    ON UPDATE RESTRICT ON DELETE SET NULL
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

CREATE TABLE IF NOT EXISTS netblock_v6_union
(
  start_ip BINARY(16) NOT NULL,
  end_ip   BINARY(16) NOT NULL,
  PRIMARY KEY (start_ip),
  KEY idx_end (end_ip),
  CONSTRAINT chk_netblock_v6_union_union CHECK (start_ip <= end_ip)
)
ENGINE          = InnoDB
DEFAULT CHARSET = utf8mb4
COLLATE         = utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS netblock_v6_union_next
(
  start_ip BINARY(16) NOT NULL,
  end_ip   BINARY(16) NOT NULL,
  PRIMARY KEY (start_ip),
  CONSTRAINT chk_netblock_v6_union_next_range CHECK (start_ip <= end_ip)
)
ENGINE          = InnoDB
DEFAULT CHARSET = utf8mb4
COLLATE         = utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS list
(
  id   INT UNSIGNED NOT NULL AUTO_INCREMENT,
  name VARCHAR(32)  NOT NULL,

  PRIMARY KEY(id),

  CONSTRAINT uk_list_name
    UNIQUE INDEX(name)
)
ENGINE          = InnoDB
DEFAULT CHARSET = utf8mb4
COLLATE         = utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS netblock_v4_list
(
  list_id        INT     UNSIGNED NOT NULL,
  netblock_v4_id BIGINT  UNSIGNED NOT NULL,
  start_ip       INT     UNSIGNED NOT NULL,
  end_ip         INT     UNSIGNED NOT NULL,
  prefix_len     TINYINT UNSIGNED NOT NULL,

  KEY idx_list_start_end(list_id, start_ip, end_ip),

  CONSTRAINT chk_netblock_v4_list_range  CHECK (start_ip <= end_ip),
  CONSTRAINT chk_netblock_v4_list_prefix CHECK (prefix_len BETWEEN 0 AND 32),

  CONSTRAINT fk_netblock_v4_list_list
    FOREIGN KEY (list_id) REFERENCES list(id)
    ON UPDATE RESTRICT ON DELETE CASCADE,

  CONSTRAINT fk_netblock_v4_list_netblock
    FOREIGN KEY (netblock_v4_id) REFERENCES netblock_v4(id)
    ON UPDATE RESTRICT ON DELETE CASCADE
)
ENGINE          = InnoDB
DEFAULT CHARSET = utf8mb4
COLLATE         = utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS netblock_v6_list
(
  list_id        INT     UNSIGNED NOT NULL,
  netblock_v6_id BIGINT  UNSIGNED NOT NULL,
  start_ip       BINARY(16)       NOT NULL,
  end_ip         BINARY(16)       NOT NULL,
  prefix_len     TINYINT UNSIGNED NOT NULL,

  KEY idx_list_start_end(list_id, start_ip, end_ip),

  CONSTRAINT chk_netblock_v6_list_range  CHECK (start_ip <= end_ip),
  CONSTRAINT chk_netblock_v6_list_prefix CHECK (prefix_len BETWEEN 0 AND 128),

  CONSTRAINT fk_netblock_v6_list_list
    FOREIGN KEY (list_id) REFERENCES list(id)
    ON UPDATE RESTRICT ON DELETE CASCADE,

  CONSTRAINT fk_netblock_v6_list_netblock
    FOREIGN KEY (netblock_v6_id) REFERENCES netblock_v6(id)
    ON UPDATE RESTRICT ON DELETE CASCADE
)
ENGINE          = InnoDB
DEFAULT CHARSET = utf8mb4
COLLATE         = utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS netblock_v4_list_union
(
  list_id    INT     UNSIGNED NOT NULL,
  ip         INT     UNSIGNED NOT NULL,
  prefix_len TINYINT UNSIGNED NOT NULL,

  PRIMARY KEY  (list_id, ip),

  KEY idx_list (list_id),

  CONSTRAINT fk_netblock_v4_list_union_list
    FOREIGN KEY (list_id) REFERENCES list(id)
    ON UPDATE RESTRICT ON DELETE CASCADE
)
ENGINE          = InnoDB
DEFAULT CHARSET = utf8mb4
COLLATE         = utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS netblock_v6_list_union
(
  list_id    INT     UNSIGNED NOT NULL,
  ip         BINARY(16)       NOT NULL,
  prefix_len TINYINT UNSIGNED NOT NULL,

  PRIMARY KEY  (list_id, ip),

  KEY idx_list (list_id),

  CONSTRAINT fk_netblock_v6_list_union_list
    FOREIGN KEY (list_id) REFERENCES list(id)
    ON UPDATE RESTRICT ON DELETE CASCADE
)
ENGINE          = InnoDB
DEFAULT CHARSET = utf8mb4
COLLATE         = utf8mb4_unicode_ci;
