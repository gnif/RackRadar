DROP TABLE IF EXISTS netblock_v6;
DROP TABLE IF EXISTS netblock_v4;
DROP TABLE IF EXISTS org;
DROP TABLE IF EXISTS registrar;

CREATE TABLE IF NOT EXISTS registrar
(
  id   INT UNSIGNED NOT NULL AUTO_INCREMENT,
  name VARCHAR(32)  NOT NULL,
  
  PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS org
(
  id          INT UNSIGNED NOT NULL AUTO_INCREMENT,
  registar_id INT UNSIGNED NOT NULL,
  name        VARCHAR(255) NOT NULL,
  description TEXT,

  PRIMARY KEY(id),

  CONSTRAINT fk_org_registrar
    FOREIGN KEY (registar_id) REFERENCES registrar(id)
    ON UPDATE RESTRICT ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS netblock_v4
(
  id          BIGINT  UNSIGNED NOT NULL AUTO_INCREMENT,
  registar_id INT     UNSIGNED NOT NULL,
  ord_id      INT     UNSIGNED NULL,
  start_ip    INT     UNSIGNED NOT NULL,
  end_ip      INT     UNSIGNED NOT NULL,
  prefix_len  TINYINT UNSIGNED NOT NULL,

  PRIMARY KEY(id),

  KEY idx_start     (start_ip),
  KEY idx_start_end (start_ip, end_ip),
  KEY idx_registrar (registar_id),

  CONSTRAINT chk_range  CHECK (start_ip <= end_ip),
  CONSTRAINT chk_prefix CHECK (prefix_len BETWEEN 0 AND 32),
  
  CONSTRAINT fk_nb_registrar
    FOREIGN KEY (registar_id) REFERENCES registrar(id)
    ON UPDATE RESTRICT ON DELETE CASCADE,

  CONSTRAINT fk_nb_org
    FOREIGN KEY (ord_id) REFERENCES org(id)
    ON UPDATE RESTRICT ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS netblock_v6
(
  id          BIGINT  UNSIGNED NOT NULL AUTO_INCREMENT,
  registar_id INT     UNSIGNED NOT NULL,
  ord_id      INT     UNSIGNED NULL,
  start_ip    BINARY(16)       NOT NULL,
  end_ip      BINARY(16)       NOT NULL,
  prefix_len  TINYINT UNSIGNED NOT NULL,

  PRIMARY KEY(id),

  KEY idx_start     (start_ip),
  KEY idx_start_end (start_ip, end_ip),
  KEY idx_registrar (registar_id),
  KEY idx_org       (ord_id),

  CONSTRAINT chk_prefix_v6 CHECK (prefix_len BETWEEN 0 AND 128),

  CONSTRAINT fk_nb6_registrar
    FOREIGN KEY (registar_id) REFERENCES registrar(id)
    ON UPDATE RESTRICT ON DELETE CASCADE,

  CONSTRAINT fk_nb6_org
    FOREIGN KEY (ord_id) REFERENCES org(id)
    ON UPDATE RESTRICT ON DELETE SET NULL
);