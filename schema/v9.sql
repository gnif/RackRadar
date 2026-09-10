CREATE TABLE IF NOT EXISTS email_domain
(
  id     INT UNSIGNED NOT NULL AUTO_INCREMENT,
  domain VARCHAR(253) CHARACTER SET ascii COLLATE ascii_general_ci NOT NULL,

  PRIMARY KEY(id),

  CONSTRAINT uk_email_domain_domain
    UNIQUE INDEX(domain)
)
ENGINE          = InnoDB
DEFAULT CHARSET = ascii
COLLATE         = ascii_general_ci;

CREATE TABLE IF NOT EXISTS org_email_domain
(
  org_id          INT UNSIGNED NOT NULL,
  email_domain_id INT UNSIGNED NOT NULL,

  PRIMARY KEY(org_id, email_domain_id),
  KEY idx_org_email_domain_reverse(email_domain_id, org_id),

  CONSTRAINT fk_org_email_domain_org
    FOREIGN KEY (org_id) REFERENCES org(id)
    ON UPDATE RESTRICT ON DELETE CASCADE,

  CONSTRAINT fk_org_email_domain_domain
    FOREIGN KEY (email_domain_id) REFERENCES email_domain(id)
    ON UPDATE RESTRICT ON DELETE CASCADE
)
ENGINE          = InnoDB
DEFAULT CHARSET = utf8mb4
COLLATE         = utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS netblock_v4_email_domain
(
  netblock_v4_id  BIGINT UNSIGNED NOT NULL,
  email_domain_id INT    UNSIGNED NOT NULL,

  PRIMARY KEY(netblock_v4_id, email_domain_id),
  KEY idx_netblock_v4_email_domain_reverse(email_domain_id, netblock_v4_id),

  CONSTRAINT fk_netblock_v4_email_domain_netblock
    FOREIGN KEY (netblock_v4_id) REFERENCES netblock_v4(id)
    ON UPDATE RESTRICT ON DELETE CASCADE,

  CONSTRAINT fk_netblock_v4_email_domain_domain
    FOREIGN KEY (email_domain_id) REFERENCES email_domain(id)
    ON UPDATE RESTRICT ON DELETE CASCADE
)
ENGINE          = InnoDB
DEFAULT CHARSET = utf8mb4
COLLATE         = utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS netblock_v6_email_domain
(
  netblock_v6_id  BIGINT UNSIGNED NOT NULL,
  email_domain_id INT    UNSIGNED NOT NULL,

  PRIMARY KEY(netblock_v6_id, email_domain_id),
  KEY idx_netblock_v6_email_domain_reverse(email_domain_id, netblock_v6_id),

  CONSTRAINT fk_netblock_v6_email_domain_netblock
    FOREIGN KEY (netblock_v6_id) REFERENCES netblock_v6(id)
    ON UPDATE RESTRICT ON DELETE CASCADE,

  CONSTRAINT fk_netblock_v6_email_domain_domain
    FOREIGN KEY (email_domain_id) REFERENCES email_domain(id)
    ON UPDATE RESTRICT ON DELETE CASCADE
)
ENGINE          = InnoDB
DEFAULT CHARSET = utf8mb4
COLLATE         = utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS email_domain_stage
(
  registrar_id INT             UNSIGNED NOT NULL,
  record_id    BIGINT          UNSIGNED NOT NULL,
  domain       VARCHAR(253) CHARACTER SET ascii COLLATE ascii_general_ci
    NOT NULL,

  PRIMARY KEY(registrar_id, record_id, domain),
  KEY idx_email_domain_stage_reverse(domain, registrar_id, record_id)
)
ENGINE          = InnoDB
DEFAULT CHARSET = ascii
COLLATE         = ascii_general_ci;

-- The old importer caps each newline-delimited address list at 8192 bytes.
-- Even the shortest valid addresses cannot produce more than 2048 entries.
CREATE TEMPORARY TABLE rr_v9_digit
(
  n TINYINT UNSIGNED NOT NULL,
  PRIMARY KEY(n)
)
ENGINE = MEMORY;

INSERT INTO rr_v9_digit (n)
VALUES (0), (1), (2), (3), (4), (5), (6), (7), (8), (9);

CREATE TEMPORARY TABLE rr_v9_sequence
(
  n SMALLINT UNSIGNED NOT NULL,
  PRIMARY KEY(n)
)
ENGINE = MEMORY;

INSERT INTO rr_v9_sequence (n)
SELECT d0.n + d1.n * 10 + d2.n * 100 + d3.n * 1000 + 1
FROM rr_v9_digit d0
CROSS JOIN rr_v9_digit d1
CROSS JOIN rr_v9_digit d2
CROSS JOIN rr_v9_digit d3
WHERE d0.n + d1.n * 10 + d2.n * 100 + d3.n * 1000 < 2048;

CREATE TEMPORARY TABLE rr_v9_domain_map
(
  entity_type TINYINT         UNSIGNED NOT NULL,
  entity_id   BIGINT          UNSIGNED NOT NULL,
  domain      VARCHAR(253) CHARACTER SET ascii COLLATE ascii_general_ci
    NOT NULL,

  PRIMARY KEY(entity_type, entity_id, domain),
  KEY idx_rr_v9_domain_map_reverse(domain, entity_type, entity_id)
)
ENGINE = InnoDB;

INSERT INTO rr_v9_domain_map (entity_type, entity_id, domain)
SELECT DISTINCT
  1,
  split.entity_id,
  LOWER(SUBSTRING_INDEX(split.address, '@', -1))
FROM
(
  SELECT
    o.id AS entity_id,
    SUBSTRING_INDEX(
      SUBSTRING_INDEX(o.email, CHAR(10), seq.n),
      CHAR(10),
      -1) AS address
  FROM org o
  STRAIGHT_JOIN rr_v9_sequence seq
    ON seq.n <= 1 + CHAR_LENGTH(o.email) -
      CHAR_LENGTH(REPLACE(o.email, CHAR(10), ''))
  WHERE o.email IS NOT NULL AND o.email <> ''
) split
WHERE INSTR(split.address, '@') > 1
AND LENGTH(split.address) - LENGTH(REPLACE(split.address, '@', '')) = 1
AND CHAR_LENGTH(SUBSTRING_INDEX(split.address, '@', -1)) BETWEEN 1 AND 253
AND LENGTH(SUBSTRING_INDEX(split.address, '@', -1)) =
  CHAR_LENGTH(SUBSTRING_INDEX(split.address, '@', -1))
ON DUPLICATE KEY UPDATE domain = VALUES(domain);

INSERT INTO rr_v9_domain_map (entity_type, entity_id, domain)
SELECT DISTINCT
  4,
  split.entity_id,
  LOWER(SUBSTRING_INDEX(split.address, '@', -1))
FROM
(
  SELECT
    nb.id AS entity_id,
    SUBSTRING_INDEX(
      SUBSTRING_INDEX(nb.email, CHAR(10), seq.n),
      CHAR(10),
      -1) AS address
  FROM netblock_v4 nb
  STRAIGHT_JOIN rr_v9_sequence seq
    ON seq.n <= 1 + CHAR_LENGTH(nb.email) -
      CHAR_LENGTH(REPLACE(nb.email, CHAR(10), ''))
  WHERE nb.email IS NOT NULL AND nb.email <> ''
) split
WHERE INSTR(split.address, '@') > 1
AND LENGTH(split.address) - LENGTH(REPLACE(split.address, '@', '')) = 1
AND CHAR_LENGTH(SUBSTRING_INDEX(split.address, '@', -1)) BETWEEN 1 AND 253
AND LENGTH(SUBSTRING_INDEX(split.address, '@', -1)) =
  CHAR_LENGTH(SUBSTRING_INDEX(split.address, '@', -1))
ON DUPLICATE KEY UPDATE domain = VALUES(domain);

INSERT INTO rr_v9_domain_map (entity_type, entity_id, domain)
SELECT DISTINCT
  6,
  split.entity_id,
  LOWER(SUBSTRING_INDEX(split.address, '@', -1))
FROM
(
  SELECT
    nb.id AS entity_id,
    SUBSTRING_INDEX(
      SUBSTRING_INDEX(nb.email, CHAR(10), seq.n),
      CHAR(10),
      -1) AS address
  FROM netblock_v6 nb
  STRAIGHT_JOIN rr_v9_sequence seq
    ON seq.n <= 1 + CHAR_LENGTH(nb.email) -
      CHAR_LENGTH(REPLACE(nb.email, CHAR(10), ''))
  WHERE nb.email IS NOT NULL AND nb.email <> ''
) split
WHERE INSTR(split.address, '@') > 1
AND LENGTH(split.address) - LENGTH(REPLACE(split.address, '@', '')) = 1
AND CHAR_LENGTH(SUBSTRING_INDEX(split.address, '@', -1)) BETWEEN 1 AND 253
AND LENGTH(SUBSTRING_INDEX(split.address, '@', -1)) =
  CHAR_LENGTH(SUBSTRING_INDEX(split.address, '@', -1))
ON DUPLICATE KEY UPDATE domain = VALUES(domain);

INSERT INTO email_domain (domain)
SELECT domain
FROM rr_v9_domain_map
GROUP BY domain
ON DUPLICATE KEY UPDATE domain = VALUES(domain);

INSERT INTO org_email_domain (org_id, email_domain_id)
SELECT map.entity_id, email.id
FROM rr_v9_domain_map map
JOIN email_domain email ON email.domain = map.domain
WHERE map.entity_type = 1
ON DUPLICATE KEY UPDATE email_domain_id = VALUES(email_domain_id);

INSERT INTO netblock_v4_email_domain
  (netblock_v4_id, email_domain_id)
SELECT map.entity_id, email.id
FROM rr_v9_domain_map map
JOIN email_domain email ON email.domain = map.domain
WHERE map.entity_type = 4
ON DUPLICATE KEY UPDATE email_domain_id = VALUES(email_domain_id);

INSERT INTO netblock_v6_email_domain
  (netblock_v6_id, email_domain_id)
SELECT map.entity_id, email.id
FROM rr_v9_domain_map map
JOIN email_domain email ON email.domain = map.domain
WHERE map.entity_type = 6
ON DUPLICATE KEY UPDATE email_domain_id = VALUES(email_domain_id);

DROP TEMPORARY TABLE rr_v9_domain_map;
DROP TEMPORARY TABLE rr_v9_sequence;
DROP TEMPORARY TABLE rr_v9_digit;

-- Staging data is transient and must not survive a schema transition.
TRUNCATE TABLE org_stage;
TRUNCATE TABLE netblock_v4_stage;
TRUNCATE TABLE netblock_v6_stage;

ALTER TABLE org_stage
  DROP COLUMN email,
  ADD COLUMN email_record_id BIGINT UNSIGNED NOT NULL AFTER descr,
  ALGORITHM = INPLACE,
  LOCK      = NONE;

ALTER TABLE netblock_v4_stage
  DROP COLUMN email,
  ADD COLUMN email_record_id BIGINT UNSIGNED NOT NULL AFTER descr,
  ALGORITHM = INPLACE,
  LOCK      = NONE;

ALTER TABLE netblock_v6_stage
  DROP COLUMN email,
  ADD COLUMN email_record_id BIGINT UNSIGNED NOT NULL AFTER descr,
  ALGORITHM = INPLACE,
  LOCK      = NONE;

-- Drop the source fields last so a failed backfill leaves them recoverable.
ALTER TABLE org
  DROP COLUMN email,
  ALGORITHM = INPLACE,
  LOCK      = NONE;

ALTER TABLE netblock_v4
  DROP COLUMN email,
  ALGORITHM = INPLACE,
  LOCK      = NONE;

ALTER TABLE netblock_v6
  DROP COLUMN email,
  ALGORITHM = INPLACE,
  LOCK      = NONE;
