ALTER TABLE registrar
  ADD COLUMN last_check
    INT UNSIGNED NOT NULL DEFAULT 0 AFTER last_import,
  ADD COLUMN source_config_hash
    CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT ''
    AFTER last_check,
  ADD COLUMN source_content_hash
    CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT ''
    AFTER source_config_hash,
  ADD COLUMN source_etag
    VARCHAR(1024) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT ''
    AFTER source_content_hash,
  ADD COLUMN source_last_modified
    VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT ''
    AFTER source_etag;
