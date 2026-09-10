ALTER TABLE org
  DROP INDEX idx_serial,
  ALGORITHM = INPLACE,
  LOCK      = NONE;

ALTER TABLE netblock_v4
  DROP INDEX idx_serial,
  DROP INDEX idx_start,
  DROP INDEX idx_registrar,
  ALGORITHM = INPLACE,
  LOCK      = NONE;

ALTER TABLE netblock_v6
  DROP INDEX idx_serial,
  DROP INDEX idx_registrar,
  ALGORITHM = INPLACE,
  LOCK      = NONE;

ALTER TABLE netblock_v4_list_union
  DROP INDEX idx_list,
  ALGORITHM = INPLACE,
  LOCK      = NONE;

ALTER TABLE netblock_v6_list_union
  DROP INDEX idx_list,
  ALGORITHM = INPLACE,
  LOCK      = NONE;
