"""Click CLI for Beekeeper."""

from __future__ import annotations

import sys

import click

from beekeeper import __version__
from beekeeper.config import BeekeeperConfig
from beekeeper.core.reporter import print_analysis_report, print_compaction_report


def _build_config(ctx: click.Context) -> BeekeeperConfig:
    """Build config from YAML file and CLI overrides."""
    params = ctx.params
    config_file = params.get("config_file")

    if config_file:
        config = BeekeeperConfig.from_yaml(config_file)
    else:
        config = BeekeeperConfig()

    return config.merge_cli_overrides(**params)


def _get_engine(config: BeekeeperConfig):  # noqa: ANN202
    """Create a HiveExternalEngine with SparkSession."""
    from beekeeper.engine.hive_external import HiveExternalEngine
    from beekeeper.utils.spark import get_or_create_spark_session

    spark = get_or_create_spark_session()
    return HiveExternalEngine(spark, config)


def _resolve_tables(config: BeekeeperConfig, engine) -> list[tuple[str, str]]:  # noqa: ANN001
    """Resolve which tables to process.

    Returns:
        List of (database, table_name) tuples.
    """
    tables = []

    if config.table:
        parts = config.table.split(".")
        if len(parts) != 2:  # noqa: PLR2004
            click.echo(f"Error: table must be in format 'database.table', got '{config.table}'", err=True)
            sys.exit(1)
        tables.append((parts[0], parts[1]))

    elif config.tables:
        for t in config.tables:
            parts = t.strip().split(".")
            if len(parts) != 2:  # noqa: PLR2004
                click.echo(f"Error: table must be in format 'database.table', got '{t}'", err=True)
                sys.exit(1)
            tables.append((parts[0], parts[1]))

    elif config.database:
        table_names = engine.list_tables(config.database)
        tables = [(config.database, t) for t in table_names]

    else:
        click.echo("Error: must specify --database, --table, or --tables", err=True)
        sys.exit(1)

    return tables


@click.group()
@click.version_option(version=__version__, prog_name="beekeeper")
def main() -> None:
    """Beekeeper - Safe compaction for Hive external tables."""


@main.command()
@click.option("--database", "-d", help="Database to analyze.")
@click.option("--table", "-t", help="Specific table (format: db.table).")
@click.option("--tables", help="Comma-separated list of tables (format: db.t1,db.t2).")
@click.option("--block-size", "block_size_mb", type=int, help="Target block size in MB.")
@click.option("--ratio-threshold", "compaction_ratio_threshold", type=float, help="Compaction ratio threshold.")
@click.option("--config-file", "-c", help="YAML configuration file.")
@click.option("--log-level", help="Log level (DEBUG, INFO, WARNING, ERROR).")
@click.pass_context
def analyze(ctx: click.Context, **kwargs: str | None) -> None:
    """Analyze tables and report compaction needs (dry-run)."""
    config = _build_config(ctx)
    if kwargs.get("tables"):
        config = config.merge_cli_overrides(tables=kwargs["tables"].split(","))
    config.setup_logging()

    engine = _get_engine(config)
    tables = _resolve_tables(config, engine)

    click.echo(f"Analyzing {len(tables)} table(s)...\n")
    for database, table_name in tables:
        table_info = engine.analyze(database, table_name)
        print_analysis_report(table_info)


@main.command()
@click.option("--database", "-d", help="Database to compact.")
@click.option("--table", "-t", help="Specific table (format: db.table).")
@click.option("--tables", help="Comma-separated list of tables (format: db.t1,db.t2).")
@click.option("--block-size", "block_size_mb", type=int, help="Target block size in MB.")
@click.option("--ratio-threshold", "compaction_ratio_threshold", type=float, help="Compaction ratio threshold.")
@click.option("--dry-run", is_flag=True, help="Analyze only, do not compact.")
@click.option("--config-file", "-c", help="YAML configuration file.")
@click.option("--log-level", help="Log level (DEBUG, INFO, WARNING, ERROR).")
@click.pass_context
def compact(ctx: click.Context, **kwargs: str | None) -> None:
    """Compact Hive external tables."""
    config = _build_config(ctx)
    if kwargs.get("tables"):
        config = config.merge_cli_overrides(tables=kwargs["tables"].split(","))
    config.setup_logging()

    engine = _get_engine(config)
    tables = _resolve_tables(config, engine)

    click.echo(f"Processing {len(tables)} table(s)...\n")
    for database, table_name in tables:
        table_info = engine.analyze(database, table_name)
        print_analysis_report(table_info)

        if not table_info.needs_compaction:
            click.echo(f"  Skipping {table_info.full_name} - no compaction needed.\n")
            continue

        if config.dry_run:
            click.echo(f"  [DRY RUN] Would compact {table_info.full_name}\n")
            continue

        click.echo(f"  Creating backup for {table_info.full_name}...")
        backup_info = engine.create_backup(table_info)
        click.echo(f"  Backup created: {backup_info.backup_table}")

        click.echo(f"  Compacting {table_info.full_name}...")
        report = engine.compact(table_info, backup_info)
        print_compaction_report(report)


@main.command()
@click.option("--table", "-t", required=True, help="Table to rollback (format: db.table).")
@click.option("--config-file", "-c", help="YAML configuration file.")
@click.option("--log-level", help="Log level (DEBUG, INFO, WARNING, ERROR).")
@click.pass_context
def rollback(ctx: click.Context, **kwargs: str | None) -> None:
    """Rollback a table to its pre-compaction state."""
    config = _build_config(ctx)
    config.setup_logging()

    table = config.table
    if not table or "." not in table:
        click.echo("Error: --table must be in format 'database.table'", err=True)
        sys.exit(1)

    database, table_name = table.split(".", 1)
    engine = _get_engine(config)

    click.echo(f"Rolling back {table}...")
    engine.rollback(database, table_name)
    click.echo(f"Rollback complete for {table}.")


@main.command()
@click.option("--database", "-d", help="Database to cleanup.")
@click.option("--table", "-t", help="Specific table (format: db.table).")
@click.option("--older-than", help="Only clean backups older than duration (e.g., 7d).")
@click.option("--config-file", "-c", help="YAML configuration file.")
@click.option("--log-level", help="Log level (DEBUG, INFO, WARNING, ERROR).")
@click.pass_context
def cleanup(ctx: click.Context, **kwargs: str | None) -> None:
    """Clean up backup tables and old compacted data."""
    config = _build_config(ctx)
    config.setup_logging()

    older_than = kwargs.get("older_than")
    older_than_days = _parse_duration(older_than) if older_than else None

    engine = _get_engine(config)

    if config.table:
        parts = config.table.split(".")
        if len(parts) != 2:  # noqa: PLR2004
            click.echo("Error: --table must be in format 'database.table'", err=True)
            sys.exit(1)
        database, table_name = parts
        cleaned = engine.cleanup(database, table_name, older_than_days)
        click.echo(f"Cleaned {cleaned} backup(s) for {config.table}.")

    elif config.database:
        table_names = engine.list_tables(config.database)
        total_cleaned = 0
        for table_name in table_names:
            cleaned = engine.cleanup(config.database, table_name, older_than_days)
            total_cleaned += cleaned
        click.echo(f"Cleaned {total_cleaned} backup(s) in database {config.database}.")

    else:
        click.echo("Error: must specify --database or --table", err=True)
        sys.exit(1)


def _parse_duration(duration_str: str) -> int:
    """Parse a duration string like '7d' into days.

    Args:
        duration_str: Duration string (e.g., '7d', '30d').

    Returns:
        Number of days.

    Raises:
        click.BadParameter: If format is invalid.
    """
    duration_str = duration_str.strip().lower()
    if duration_str.endswith("d"):
        try:
            return int(duration_str[:-1])
        except ValueError:
            pass
    msg = f"Invalid duration format: '{duration_str}'. Use format like '7d'."
    raise click.BadParameter(msg)
