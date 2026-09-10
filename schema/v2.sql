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

CREATE TABLE IF NOT EXISTS netblock_v4_union_next
(
  start_ip INT UNSIGNED NOT NULL,
  end_ip   INT UNSIGNED NOT NULL,

  PRIMARY KEY(start_ip),

  CONSTRAINT chk_netblock_v4_union_next_range CHECK (start_ip <= end_ip)
)
ENGINE          = InnoDB
DEFAULT CHARSET = utf8mb4
COLLATE         = utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS netblock_v6_union_next
(
  start_ip BINARY(16) NOT NULL,
  end_ip   BINARY(16) NOT NULL,

  PRIMARY KEY(start_ip),

  CONSTRAINT chk_netblock_v6_union_next_range CHECK (start_ip <= end_ip)
)
ENGINE          = InnoDB
DEFAULT CHARSET = utf8mb4
COLLATE         = utf8mb4_unicode_ci;

ALTER TABLE netblock_v6
  DROP FOREIGN KEY fk_netblock_v6_org;

ALTER TABLE netblock_v6
  ADD CONSTRAINT fk_netblock_v6_org
    FOREIGN KEY (org_id) REFERENCES org(id)
    ON UPDATE RESTRICT ON DELETE SET NULL;
