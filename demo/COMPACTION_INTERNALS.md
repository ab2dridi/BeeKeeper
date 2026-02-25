# Lakekeeper — Compaction Internals

This document explains the step-by-step compaction mechanism for the three demo
tables shipped in this directory.  All three cases share the same core invariant:

```
┌─────────────────────────────────────────────────────────────────┐
│  The table LOCATION in the Hive Metastore is NEVER modified.   │
│  Only the HDFS directory contents are swapped via atomic        │
│  rename.  The backup table "follows" the original data by       │
│  having its partition locations updated to the __old_TS paths.  │
└─────────────────────────────────────────────────────────────────┘
```

---

## Table of contents

1. [Case 1 — `lakekeeper_flat` (non-partitioned)](#case-1--lakekeeper_flat-non-partitioned)
2. [Case 2 — `lakekeeper_events` (single partition key: `date`)](#case-2--lakekeeper_events-single-partition-key-date)
3. [Case 3 — `lakekeeper_events_2p` (two partition keys: `date` + `ref`)](#case-3--lakekeeper_events_2p-two-partition-keys-date--ref)
4. [Rollback mechanism](#rollback-mechanism)
5. [Cleanup](#cleanup)

---

## Case 1 — `lakekeeper_flat` (non-partitioned)

**Profile:** 500 small Parquet files, 300 000 rows, no partition columns.

### Before compaction

```
HMS (Hive Metastore)
┌──────────────────────────────────────────────────────────────┐
│ demo.lakekeeper_flat                                          │
│   LOCATION = hdfs:///demo/lakekeeper_flat                    │
└──────────────────────────────────────────────────────────────┘

HDFS
hdfs:///demo/lakekeeper_flat/
├── part-00000.parquet  (~600 B)
├── part-00001.parquet  (~600 B)
├── ...
└── part-00499.parquet  (~600 B)   ← 500 files × ~600 B ≈ 300 KB total
```

`lakekeeper analyze` reports: `avg_file_size=600B < threshold=128MB`
→ `needs_compaction=True`, `target_files=1`

---

### Step 1 — Zero-copy backup

```
SHOW CREATE TABLE demo.lakekeeper_flat  →  original DDL
  → substitute table name
  → CREATE EXTERNAL TABLE demo.__bkp_lakekeeper_flat_20260225_143000
        LOCATION 'hdfs:///demo/lakekeeper_flat'   ← same path, no data copied
  → ALTER TABLE __bkp... SET TBLPROPERTIES ('external.table.purge'='false')

HMS after backup
┌────────────────────────────────────────────────────────────────────┐
│ demo.lakekeeper_flat            LOCATION = .../lakekeeper_flat     │
│ demo.__bkp_lakekeeper_flat_...  LOCATION = .../lakekeeper_flat     │
└────────────────────────────────────────────────────────────────────┘
HDFS: unchanged (zero data copy)
```

---

### Step 2 — Phase 1: write compacted data to a temp sibling directory

```
Spark: df.coalesce(1).write.parquet(".../lakekeeper_flat__compact_tmp_TS")

HDFS
hdfs:///demo/lakekeeper_flat/
├── part-00000.parquet ... part-00499.parquet   ← originals untouched

hdfs:///demo/lakekeeper_flat__compact_tmp_1772054071/   ← NEW
└── part-00000.parquet  (300 KB)                        ← 1 compacted file

Verification: count(temp) == count(original) == 300 000  ✓
```

---

### Step 3 — Atomic HDFS swap + Metastore update

```
3a. HDFS rename:
      lakekeeper_flat  →  lakekeeper_flat__old_1772054071
      (original data is now safe under a new name)

3b. ALTER TABLE demo.__bkp_lakekeeper_flat_...
      SET LOCATION 'hdfs:///demo/lakekeeper_flat__old_1772054071'
      (backup now points to the original data)

3c. HDFS rename:
      lakekeeper_flat__compact_tmp_TS  →  lakekeeper_flat
      (compacted data takes the original path)
```

---

### After compaction

```
HMS
┌──────────────────────────────────────────────────────────────────────┐
│ demo.lakekeeper_flat            LOCATION = .../lakekeeper_flat       │
│                                            ↑ COMPACTED data (1 file) │
│ demo.__bkp_lakekeeper_flat_...  LOCATION = .../lakekeeper_flat__old  │
│                                            ↑ ORIGINAL data (500 files│
└──────────────────────────────────────────────────────────────────────┘

HDFS
hdfs:///demo/lakekeeper_flat/
└── part-00000.parquet  (300 KB)        ← 1 compacted file

hdfs:///demo/lakekeeper_flat__old_1772054071/
├── part-00000.parquet
├── ...
└── part-00499.parquet                  ← 500 original files (preserved)
```

---

## Case 2 — `lakekeeper_events` (single partition key: `date`)

**Profile:** 3 date partitions × 200 small files = 600 files, 300 000 rows.

### Before compaction

```
HMS
┌──────────────────────────────────────────────────────────────────────┐
│ demo.lakekeeper_events  LOCATION = hdfs:///demo/lakekeeper_events    │
│   PARTITION date='2026-02-23'  →  .../lakekeeper_events/date=2026-02-23
│   PARTITION date='2026-02-24'  →  .../lakekeeper_events/date=2026-02-24
│   PARTITION date='2026-02-25'  →  .../lakekeeper_events/date=2026-02-25
└──────────────────────────────────────────────────────────────────────┘

HDFS
hdfs:///demo/lakekeeper_events/
├── date=2026-02-23/
│   ├── part-00000.parquet ... part-00199.parquet  (200 files, 100 000 rows)
├── date=2026-02-24/
│   ├── part-00000.parquet ... part-00199.parquet  (200 files, 100 000 rows)
└── date=2026-02-25/
    └── part-00000.parquet ... part-00199.parquet  (200 files, 100 000 rows)
```

`lakekeeper analyze` reports: 3 partitions, all `needs_compaction=True`,
`target_files=1` each.

---

### Step 1 — Zero-copy backup (partitions registered explicitly)

```
CREATE EXTERNAL TABLE demo.__bkp_lakekeeper_events_20260225_143000
  LOCATION 'hdfs:///demo/lakekeeper_events'   ← same root, no data copied

For each partition to compact:
  ALTER TABLE __bkp... ADD PARTITION(date='2026-02-23')
    LOCATION 'hdfs:///demo/lakekeeper_events/date=2026-02-23'
  ALTER TABLE __bkp... ADD PARTITION(date='2026-02-24')
    LOCATION 'hdfs:///demo/lakekeeper_events/date=2026-02-24'
  ALTER TABLE __bkp... ADD PARTITION(date='2026-02-25')
    LOCATION 'hdfs:///demo/lakekeeper_events/date=2026-02-25'
```

---

### Step 2 — Per-partition compaction loop (× 3)

The same 3-phase swap is applied to each partition independently:

```
── Partition date=2026-02-23 ────────────────────────────────────────

Phase 1:  coalesce(200 → 1)
          → write to  date=2026-02-23__compact_tmp_TS/  (1 file)
Verify:   count = 100 000  ✓

Phase 3a: HDFS rename
            date=2026-02-23  →  date=2026-02-23__old_TS

Phase 3b: ALTER TABLE __bkp... PARTITION(date='2026-02-23')
            SET LOCATION '.../date=2026-02-23__old_TS'

Phase 3c: HDFS rename
            date=2026-02-23__compact_tmp_TS  →  date=2026-02-23

── Partition date=2026-02-24 ──  (same sequence)
── Partition date=2026-02-25 ──  (same sequence)
```

---

### After compaction

```
HMS
┌──────────────────────────────────────────────────────────────────────┐
│ demo.lakekeeper_events                                               │
│   PARTITION date='2026-02-23'  →  .../date=2026-02-23   ← COMPACTED │
│   PARTITION date='2026-02-24'  →  .../date=2026-02-24   ← COMPACTED │
│   PARTITION date='2026-02-25'  →  .../date=2026-02-25   ← COMPACTED │
│                                                                      │
│ demo.__bkp_lakekeeper_events_...                                     │
│   PARTITION date='2026-02-23'  →  .../date=2026-02-23__old_TS  ← ORIG
│   PARTITION date='2026-02-24'  →  .../date=2026-02-24__old_TS  ← ORIG
│   PARTITION date='2026-02-25'  →  .../date=2026-02-25__old_TS  ← ORIG
└──────────────────────────────────────────────────────────────────────┘

HDFS
hdfs:///demo/lakekeeper_events/
├── date=2026-02-23/              └── part-00000.parquet  (1 compacted file)
├── date=2026-02-23__old_TS/      ├── part-00000..199.parquet  (200 originals)
├── date=2026-02-24/              └── part-00000.parquet  (1 compacted file)
├── date=2026-02-24__old_TS/      ├── part-00000..199.parquet  (200 originals)
├── date=2026-02-25/              └── part-00000.parquet  (1 compacted file)
└── date=2026-02-25__old_TS/      └── part-00000..199.parquet  (200 originals)
```

---

## Case 3 — `lakekeeper_events_2p` (two partition keys: `date` + `ref`)

**Profile:** 3 dates × 3 refs = 9 partitions × 100 files = 900 files, 450 000 rows.

### Before compaction

```
HMS
┌──────────────────────────────────────────────────────────────────────────┐
│ demo.lakekeeper_events_2p  LOCATION = hdfs:///demo/lakekeeper_events_2p  │
│   PARTITION (date='2026-02-23', ref='A')  →  .../date=2026-02-23/ref=A   │
│   PARTITION (date='2026-02-23', ref='B')  →  .../date=2026-02-23/ref=B   │
│   PARTITION (date='2026-02-23', ref='C')  →  .../date=2026-02-23/ref=C   │
│   PARTITION (date='2026-02-24', ref='A')  →  .../date=2026-02-24/ref=A   │
│   ...  (9 partitions total)                                               │
└──────────────────────────────────────────────────────────────────────────┘

HDFS
hdfs:///demo/lakekeeper_events_2p/
├── date=2026-02-23/
│   ├── ref=A/  part-00000..099.parquet  (100 files, 50 000 rows)
│   ├── ref=B/  part-00000..099.parquet  (100 files, 50 000 rows)
│   └── ref=C/  part-00000..099.parquet  (100 files, 50 000 rows)
├── date=2026-02-24/  (same: ref=A, B, C)
└── date=2026-02-25/  (same: ref=A, B, C)
```

`lakekeeper analyze` reports: 9 partitions, all `needs_compaction=True`,
`target_files=1` each.

---

### Step 1 — Zero-copy backup (9 partition locations registered)

```
CREATE EXTERNAL TABLE demo.__bkp_lakekeeper_events_2p_20260225_143000
  LOCATION 'hdfs:///demo/lakekeeper_events_2p'

ALTER TABLE __bkp... ADD PARTITION(date='2026-02-23', ref='A')
  LOCATION '.../date=2026-02-23/ref=A'
ALTER TABLE __bkp... ADD PARTITION(date='2026-02-23', ref='B')
  LOCATION '.../date=2026-02-23/ref=B'
...  × 9 partitions
```

---

### Step 2 — Per-partition compaction loop (× 9)

Each `(date, ref)` leaf partition goes through the same 3-phase swap:

```
── Partition date=2026-02-23 / ref=A ────────────────────────────────

Phase 1:  coalesce(100 → 1)
          → write to  date=2026-02-23/ref=A__compact_tmp_TS/  (1 file)
Verify:   count = 50 000  ✓

Phase 3a: HDFS rename
            date=2026-02-23/ref=A
            →  date=2026-02-23/ref=A__old_TS

Phase 3b: ALTER TABLE __bkp...
            PARTITION(date='2026-02-23', ref='A')
            SET LOCATION '.../date=2026-02-23/ref=A__old_TS'

Phase 3c: HDFS rename
            date=2026-02-23/ref=A__compact_tmp_TS
            →  date=2026-02-23/ref=A

── Partition date=2026-02-23 / ref=B ──  (same sequence)
── Partition date=2026-02-23 / ref=C ──  (same sequence)
── ...  × 9 partitions total
```

---

### After compaction

```
HMS
┌──────────────────────────────────────────────────────────────────────────┐
│ demo.lakekeeper_events_2p                                                │
│   (date='2026-02-23', ref='A')  →  .../date=2026-02-23/ref=A  ← COMPACT │
│   (date='2026-02-23', ref='B')  →  .../date=2026-02-23/ref=B  ← COMPACT │
│   ...  (HMS location entries unchanged)                                  │
│                                                                          │
│ demo.__bkp_lakekeeper_events_2p_...                                      │
│   (date='2026-02-23', ref='A')  →  .../ref=A__old_TS  ← ORIGINAL data   │
│   (date='2026-02-23', ref='B')  →  .../ref=B__old_TS  ← ORIGINAL data   │
│   ...                                                                    │
└──────────────────────────────────────────────────────────────────────────┘

HDFS
hdfs:///demo/lakekeeper_events_2p/
├── date=2026-02-23/
│   ├── ref=A/              └── part-00000.parquet  (1 compacted file)
│   ├── ref=A__old_TS/      ├── part-00000..099.parquet  (100 originals)
│   ├── ref=B/              └── part-00000.parquet  (1 compacted file)
│   ├── ref=B__old_TS/      ├── part-00000..099.parquet  (100 originals)
│   ├── ref=C/              └── part-00000.parquet  (1 compacted file)
│   └── ref=C__old_TS/      └── part-00000..099.parquet  (100 originals)
├── date=2026-02-24/  (same)
└── date=2026-02-25/  (same)

900 files  →  9 files   ●  HMS table location never modified
```

---

## Rollback mechanism

If any phase fails (row count mismatch, HDFS error, etc.), lakekeeper
automatically reverses all in-flight renames before propagating the exception.

```
_pending_renames  (LIFO stack, unwound in reverse order)
_temp_paths       (list of temp dirs to delete on failure)

On failure:
  1. Delete every path in _temp_paths  (unverified / partial compacted data)
  2. For each (old_path, original_path) in reversed(_pending_renames):
       if old_path exists:
         if original_path exists: delete original_path  (partial/bad data)
         HDFS rename  old_path  →  original_path        (restore original)

Result: table is back to its exact pre-compaction state.
        The backup table still exists and still points to the original data.
```

Example — failure during partition `date=2026-02-24` after `date=2026-02-23`
was already swapped successfully:

```
Before rollback
  date=2026-02-23/         ← compacted  (swap completed)
  date=2026-02-23__old_TS/ ← originals
  date=2026-02-24/         ← MISSING    (rename 3a done, 3c failed)
  date=2026-02-24__old_TS/ ← originals

After rollback
  date=2026-02-23/         ← originals restored  ✓
  date=2026-02-24/         ← originals restored  ✓
  (all __old_TS and __compact_tmp_TS dirs removed)
```

---

## Cleanup

Once you have verified the compacted data, run:

```bash
lakekeeper cleanup --table demo.lakekeeper_flat
lakekeeper cleanup --table demo.lakekeeper_events
lakekeeper cleanup --table demo.lakekeeper_events_2p
```

For each table this:
1. Drops the backup table from the Metastore (`DROP TABLE IF EXISTS __bkp_...`).
   Because `external.table.purge=false` was set on all backup tables, this
   never deletes HDFS data.
2. Deletes every `__old_TS` directory found under the table root, reclaiming
   the disk space used by the original (pre-compaction) files.

```
lakekeeper_flat:        500 files  →  1 file    ~300 KB freed
lakekeeper_events:      600 files  →  3 files   ~180 KB freed  (× 3 partitions)
lakekeeper_events_2p:   900 files  →  9 files   ~270 KB freed  (× 9 partitions)
```

> **Safety guarantee** — `lakekeeper cleanup` only touches directories whose
> name matches the `__old_<timestamp>` or `__compact_tmp_<timestamp>` patterns.
> It never deletes the live table data.
