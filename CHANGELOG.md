# Changelog

## [0.0.6] - 2026-02-25

### Added
- `lakekeeper generate-config` CLI command (writes a commented `lakekeeper.yaml` template)
- `sort_columns` config key + `--sort-columns` CLI flag (re-sort before coalesce)
- `analyze_after_compaction` config key + `--analyze-stats / --no-analyze-stats` CLI flag
- `submit_command` in `spark_submit` config (e.g. `spark3-submit` on CDP)
- `py_files` in `spark_submit` config (`--py-files` in spark-submit command)
- Iceberg table guard: Iceberg tables are detected and skipped at analysis time
- Skewed file-distribution detection: use `min(avg, median)` as effective file size
- Compression codec preservation: reads `parquet.compression` / `orc.compress` from `TBLPROPERTIES`

[0.0.6]: https://github.com/ab2dridi/Lakekeeper/releases/tag/v0.0.6

---

## [0.0.5] - 2026-02-24

### Fixed
- `--config-file` not found on YARN driver in cluster deploy mode
- Empty partitions incorrectly marked for compaction
- Single-file partitions incorrectly marked for compaction

### Improved
- Single `SHOW PARTITIONS` call in the `DESCRIBE FORMATTED` fallback path
- Single timestamp shared across all `__old_TS` / `__compact_tmp_TS` dirs per run

[0.0.5]: https://github.com/ab2dridi/Lakekeeper/releases/tag/v0.0.5

---

## [0.0.4] - 2026-02-24

### Fixed
- Wrong partition location on Hive 3 / CDP (`DESCRIBE FORMATTED` emits `Location` twice; fixed with first-occurrence semantics)

[0.0.4]: https://github.com/ab2dridi/Lakekeeper/releases/tag/v0.0.4

---

## [0.0.3] - 2026-02-24

### Fixed
- Partitioned table treated as non-partitioned when `DESCRIBE FORMATTED` omits `# Partition Information` (added `SHOW PARTITIONS` fallback)

[0.0.3]: https://github.com/ab2dridi/Lakekeeper/releases/tag/v0.0.3

---

## [0.0.2] - 2026-02-24

### Fixed
- Cluster mode infinite re-submission loop (`LAKEKEEPER_SUBMITTED` guard propagated via `appMasterEnv`)
- Backup table creation (`CREATE EXTERNAL TABLE … LIKE` unsupported by SparkSQL; replaced with `SHOW CREATE TABLE` + DDL substitution)

### Added
- EXTERNAL table guard: MANAGED tables are skipped at analysis time
- `extra_files` in `spark_submit` config (`--files` in spark-submit command)
- `--config-file` accepted on the main group (before the subcommand)

[0.0.2]: https://github.com/ab2dridi/Lakekeeper/releases/tag/v0.0.2

---

## [0.0.1] - 2026-02-24

### Added
- Core compaction engine (HDFS rename-swap, Metastore location unchanged)
- Per-partition compaction (skips well-sized partitions)
- Zero-copy backups + row count verification + automatic rollback
- CLI: `analyze`, `compact`, `rollback`, `cleanup`
- Automatic `spark-submit` launch from YAML config
- Python 3.9+ / Cloudera CDP 7.1.9 support

[0.0.1]: https://github.com/ab2dridi/Lakekeeper/releases/tag/v0.0.1
