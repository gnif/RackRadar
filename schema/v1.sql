DROP TABLE IF EXISTS netblock_v6;
DROP TABLE IF EXISTS netblock_v4;
DROP TABLE IF EXISTS org;
DROP TABLE IF EXISTS registrar;

CREATE TABLE IF NOT EXISTS registrar
(
  id          INT UNSIGNED NOT NULL AUTO_INCREMENT,
  name        VARCHAR(32)  NOT NULL,
  serial      INT UNSIGNED NOT NULL DEFAULT 0,
  last_import INT UNSIGNED NOT NULL,

  PRIMARY KEY(id),

  CONSTRAINT uk_name
    UNIQUE INDEX(name)
)
ENGINE          = InnoDB
DEFAULT CHARSET = utf8mb4
COLLATE         = utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS org
(
  id           INT UNSIGNED NOT NULL AUTO_INCREMENT,
  registrar_id INT UNSIGNED NOT NULL,
  serial       INT UNSIGNED NOT NULL,
  name         VARCHAR(32)  NOT NULL,
  org_name     TEXT         NOT NULL,
  descr        TEXT         NULL,

  PRIMARY KEY(id),

  KEY idx_serial(serial),

  CONSTRAINT uk_registrar_id_name
    UNIQUE INDEX(registrar_id, name),

  CONSTRAINT fk_org_registrar
    FOREIGN KEY (registrar_id) REFERENCES registrar(id)
    ON UPDATE RESTRICT ON DELETE CASCADE
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
  org_id_str   VARCHAR(32)      NOT NULL DEFAULT '',
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
  KEY idx_org_id_str (org_id_str),

  CONSTRAINT uk_constraint
    UNIQUE INDEX (registrar_id, org_id_str, start_ip, end_ip),

  CONSTRAINT chk_range  CHECK (start_ip <= end_ip),
  CONSTRAINT chk_prefix CHECK (prefix_len BETWEEN 0 AND 32),
  
  CONSTRAINT fk_nb_registrar
    FOREIGN KEY (registrar_id) REFERENCES registrar(id)
    ON UPDATE RESTRICT ON DELETE CASCADE
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
  CONSTRAINT chk_union CHECK (start_ip <= end_ip)
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
  org_id_str   VARCHAR(32)      NOT NULL DEFAULT '',  
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
  KEY idx_org        (org_id),
  KEY idx_org_id_str (org_id_str),

  CONSTRAINT uk_constraint
    UNIQUE INDEX (registrar_id, org_id_str, start_ip, end_ip),  

  CONSTRAINT chk_prefix_v6 CHECK (prefix_len BETWEEN 0 AND 128),

  CONSTRAINT fk_nb6_registrar
    FOREIGN KEY (registrar_id) REFERENCES registrar(id)
    ON UPDATE RESTRICT ON DELETE CASCADE,

  CONSTRAINT fk_nb6_org
    FOREIGN KEY (org_id) REFERENCES org(id)
    ON UPDATE RESTRICT ON DELETE CASCADE
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
  CONSTRAINT chk_union CHECK (start_ip <= end_ip)
)
ENGINE          = InnoDB
DEFAULT CHARSET = utf8mb4
COLLATE         = utf8mb4_unicode_ci;
