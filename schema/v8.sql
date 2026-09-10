ALTER TABLE registrar
  ADD COLUMN source_check_config_hash
    CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT ''
    AFTER source_config_hash,
  ALGORITHM = INPLACE,
  LOCK      = NONE;

UPDATE registrar
SET source_check_config_hash = source_config_hash;
