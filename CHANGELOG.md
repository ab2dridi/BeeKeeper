# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [0.0.5] - 2026-02-24

### Fixed

- **`--config-file` not found on YARN driver in cluster deploy mode** — in `--deploy-mode cluster` the driver runs on a remote YARN node that cannot access local edge-node files. When `--config-file` points to a local path, lakekeeper now automatically adds the file to `spark-submit --files` so YARN ships it to the driver container's working directory, and rewrites the argument to just the basename. HDFS paths are left untouched; `client` deploy mode is unaffected.
- **Empty partitions incorrectly marked for compaction** — a partition with 0 files had `avg_file_size = 0 < threshold`, which set `needs_compaction = True`. This triggered a pointless rename-swap with no data to process. Empty partitions are now detected and skipped.
- **Single-file partitions incorrectly marked for compaction** — a partition with exactly 1 file that is smaller than the compaction threshold was marked for compaction. `coalesce(1 → 1)` is a no-op rename-swap with no benefit. Compaction now requires `file_count > 1`.

### Improved

- **Single `SHOW PARTITIONS` call in fallback path** — when `DESCRIBE FORMATTED` omits the partition-column section, the `SHOW PARTITIONS` fallback previously issued the query twice (once for column detection, once for partition enumeration). The rows are now fetched once and reused, halving the Metastore round-trips for this path.
- **Single timestamp per compaction run** — all `__old_TS` and `__compact_tmp_TS` directories produced by a single `compact_table` call now share the same timestamp, making them identifiable as a group for cleanup and debugging.

[0.0.5]: https://github.com/ab2dridi/Lakekeeper/releases/tag/v0.0.5

---

## [0.0.4] - 2026-02-24

### Fixed

- **Wrong partition location on Hive 3 / CDP** — `DESCRIBE FORMATTED … PARTITION(…)` on CDP Hive 3 emits the `Location` field **twice**: the partition-specific path first (under `# Detailed Partition Information`) and the table-level path second (under `# Detailed Table Information`). A plain dict comprehension kept the last value, so every partition's `location` resolved to the table root — causing all compaction renames to target the table root instead of individual partition directories, destroying the partition structure on the first iteration. Fixed by building the metadata map with **first-occurrence semantics** in `analyzer.py::_get_partition_location`, `backup.py::_get_backup_partition_locations`, and `hive_external.py::_delete_old_data_dirs`. Applies to 2-, 3-, and 4-level partition hierarchies.

[0.0.4]: https://github.com/ab2dridi/Lakekeeper/releases/tag/v0.0.4

---

## [0.0.3] - 2026-02-24

### Fixed

- **Partitioned table treated as non-partitioned** — on some Hive 3 / SparkSQL 3.3.x versions, the `# Partition Information` block is absent from `DESCRIBE FORMATTED` output. This caused multi-level partitioned tables (e.g. `date=*/ref=*`) to be compacted as non-partitioned, collapsing all data into a single flat file at the table root and destroying the partition structure entirely. Fixed by adding a `SHOW PARTITIONS` fallback: if `DESCRIBE FORMATTED` returns no partition columns, `SHOW PARTITIONS` is called; it raises `AnalysisException` on non-partitioned tables, making it a reliable disambiguator.

[0.0.3]: https://github.com/ab2dridi/Lakekeeper/releases/tag/v0.0.3

---

## [0.0.2] - 2026-02-24

### Fixed

- **Cluster mode infinite re-submission loop** — in `deploy_mode: cluster`, the YARN driver runs on a remote node and does not inherit edge-node environment variables. The `LAKEKEEPER_SUBMITTED` guard is now propagated explicitly via `spark.yarn.appMasterEnv.LAKEKEEPER_SUBMITTED=1` injected into the spark-submit command, preventing the driver from re-launching itself.
- **Backup table creation** — `CREATE EXTERNAL TABLE … LIKE` is not supported by SparkSQL. Replaced with `SHOW CREATE TABLE` (get original DDL) + regex name substitution + `ALTER TABLE SET TBLPROPERTIES ('external.table.purge'='false')`. This also correctly handles all storage properties (SerDe, row format, compression).

### Added

- **EXTERNAL table guard** — MANAGED tables are now detected at analysis time and skipped automatically with a clear message. Only `EXTERNAL` tables are compacted (rename-swap is unsafe on managed tables).
- **`extra_files` in spark_submit config** — new list field rendered as `--files file1,file2` in the spark-submit command, useful for distributing cluster config files (`hive-site.xml`, `hdfs-site.xml`) to executors.
- **`--config-file` on main group** — the option can now be placed before the subcommand name (`lakekeeper --config-file lakekeeper.yaml compact …`) as well as after it (backward compatible).

[0.0.2]: https://github.com/ab2dridi/Lakekeeper/releases/tag/v0.0.2

---

## [0.0.1] - 2026-02-24

### Added

- **Core compaction engine** — safe compaction of Hive external tables via HDFS rename swap strategy. The table's Metastore location never changes, preserving Apache Atlas lineage and catalog properties.
- **Per-partition compaction** — only partitions exceeding the small-file threshold are compacted; well-sized partitions are skipped.
- **Zero-copy backups** — `CREATE EXTERNAL TABLE LIKE` pointing to the original HDFS location, no data duplication.
- **Row count verification** — aborts and rolls back automatically if row counts do not match after compaction.
- **CLI** (`lakekeeper`) with four commands:
  - `analyze` — dry-run analysis, reports which tables and partitions need compaction.
  - `compact` — runs compaction on a table, a list of tables, or a full database.
  - `rollback` — restores a table to its exact pre-compaction state.
  - `cleanup` — removes backup tables and reclaims HDFS space; supports age filtering (`--older-than 7d`).
- **Automatic spark-submit launch** — when `spark_submit.enabled: true` is set in the YAML config, the CLI automatically builds and executes the full `spark-submit` command (Kerberos principal/keytab, YARN queue, conda archives, executor resources, extra `--conf` pairs). The `LAKEKEEPER_SUBMITTED` environment variable prevents infinite re-submission.
- **YAML configuration** — all parameters configurable via a YAML file; CLI flags override YAML values.
- **Python 3.9+ support** — compatible with Cloudera CDP 7.1.9, Hortonworks HDP, and vanilla Hadoop distributions.

[0.0.1]: https://github.com/ab2dridi/Lakekeeper/releases/tag/v0.0.1
