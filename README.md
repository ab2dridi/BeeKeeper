# Beekeeper

Safe compaction tool for Hive external tables on Cloudera CDP.

## Problem

On Cloudera CDP 7.1.9, Hive external tables created via PySpark accumulate thousands of small files (e.g., 65,000 files for 3 GB). This degrades read performance, overloads the HDFS NameNode, and slows queries.

## Solution

Beekeeper compacts these tables **safely**:
- No `saveAsTable` — preserves Atlas cataloging properties
- Zero-copy backups — no data duplication
- Automatic partition detection — only compacts partitions that need it
- Dynamic target file count — based on data size and HDFS block size
- Row count verification — automatic rollback on mismatch

## Installation

```bash
pip install .

# With dev dependencies
pip install ".[dev]"
```

## Usage

### Analyze tables (dry-run)

```bash
# Analyze all external tables in a database
beekeeper analyze --database mydb

# Analyze a specific table
beekeeper analyze --table mydb.mytable
```

### Compact tables

```bash
# Compact all external tables in a database
beekeeper compact --database mydb

# Compact a specific table
beekeeper compact --table mydb.mytable

# Compact multiple tables
beekeeper compact --tables mydb.t1,mydb.t2,mydb.t3

# With custom parameters
beekeeper compact --database mydb --block-size 256 --ratio-threshold 5

# Dry-run (analyze only)
beekeeper compact --database mydb --dry-run
```

### Rollback

```bash
beekeeper rollback --table mydb.mytable
```

### Cleanup backups

```bash
# Clean all backups for a table
beekeeper cleanup --table mydb.mytable

# Clean backups older than 7 days
beekeeper cleanup --database mydb --older-than 7d
```

### With spark-submit

```bash
# 1. Create conda environment
conda create -n beekeeper_env python=3.9 -y
conda activate beekeeper_env
pip install ./beekeeper
conda-pack -o beekeeper_env.tar.gz

# 2. Submit
spark-submit \
  --master yarn \
  --deploy-mode client \
  --archives beekeeper_env.tar.gz#beekeeper_env \
  --conf spark.pyspark.python=./beekeeper_env/bin/python \
  run_beekeeper.py compact --database mydb --block-size 128
```

## Configuration

### YAML config file

```yaml
block_size_mb: 128
compaction_ratio_threshold: 10.0
backup_prefix: "__bkp"
dry_run: false
log_level: INFO
```

```bash
beekeeper compact --database mydb --config-file config.yaml
```

### Parameters

| Parameter | Default | Description |
|---|---|---|
| `block_size_mb` | 128 | Target HDFS block size in MB |
| `compaction_ratio_threshold` | 10.0 | Compact if avg file < block_size / ratio |
| `backup_prefix` | `__bkp` | Prefix for backup tables |
| `dry_run` | False | Analyze without compacting |
| `log_level` | INFO | Log level |

## How it works

### Non-partitioned table

1. **Analyze** — Detect format, count files, measure sizes
2. **Backup** — Create external table pointing to same HDFS location (zero-copy)
3. **Compact** — Read data, coalesce to target file count, write to new location
4. **Verify** — Compare row counts (rollback on mismatch)
5. **Swap** — `ALTER TABLE SET LOCATION` to new compacted location
6. **Report** — Before/after metrics

### Partitioned table

Same workflow but per-partition: only partitions exceeding the compaction threshold are processed.

## Development

```bash
# Install dev dependencies
pip install ".[dev]"

# Lint
ruff check src/ tests/
ruff format --check src/ tests/

# Test with coverage
pytest tests/ -v --cov=beekeeper --cov-report=term-missing
```
