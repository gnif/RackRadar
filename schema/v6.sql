ALTER TABLE import_state
  ADD COLUMN data_generation
    BIGINT UNSIGNED NOT NULL DEFAULT 0 AFTER unions_dirty,
  ADD COLUMN list_generation
    BIGINT UNSIGNED NOT NULL DEFAULT 0 AFTER data_generation,
  ADD COLUMN list_config_hash
    CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT ''
    AFTER list_generation;
