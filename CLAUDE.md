# Beekeeper - Claude Code Guide

## Project Overview
Beekeeper is a Python package for safe compaction of Hive external tables on Cloudera CDP 7.1.9.
It consolidates thousands of small files into optimally-sized files without destroying Atlas cataloging properties.

## Architecture
- `src/beekeeper/` - Main package
  - `config.py` - Configuration dataclass with YAML loading
  - `models.py` - Data models (TableInfo, PartitionInfo, BackupInfo, etc.)
  - `cli.py` - Click CLI (analyze, compact, rollback, cleanup)
  - `engine/base.py` - Abstract CompactionEngine interface
  - `engine/hive_external.py` - Hive external table implementation
  - `core/analyzer.py` - Table analysis (format detection, partition discovery)
  - `core/backup.py` - Zero-copy backup management
  - `core/compactor.py` - Compaction orchestration
  - `core/reporter.py` - Human-readable reports
  - `utils/hdfs.py` - HDFS operations via Hadoop FileSystem API
  - `utils/spark.py` - SparkSession helper

## Key Design Decisions
- **No saveAsTable**: Uses read/write to new location + ALTER TABLE SET LOCATION to preserve Atlas metadata
- **Zero-copy backups**: CREATE EXTERNAL TABLE LIKE pointing to original location
- **Per-partition compaction**: Only compacts partitions that need it
- **Row count verification**: Aborts and rolls back if row counts don't match

## Commands
```bash
ruff check src/ tests/            # Lint
ruff format --check src/ tests/   # Format check
pytest tests/ -v --cov=beekeeper --cov-report=term-missing  # Tests
```

## Python Version
Target: Python 3.9+ (Cloudera CDP compatibility)
