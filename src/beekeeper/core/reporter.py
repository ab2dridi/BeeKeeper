"""Reporting module for compaction results."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from beekeeper.models import CompactionReport, TableInfo

logger = logging.getLogger(__name__)


def format_bytes(size_bytes: int) -> str:
    """Format bytes into human-readable string.

    Args:
        size_bytes: Size in bytes.

    Returns:
        Human-readable size string.
    """
    if size_bytes == 0:
        return "0 B"
    units = ["B", "KB", "MB", "GB", "TB"]
    unit_index = 0
    size = float(size_bytes)
    while size >= 1024 and unit_index < len(units) - 1:
        size /= 1024
        unit_index += 1
    return f"{size:.1f} {units[unit_index]}"


def print_analysis_report(table_info: TableInfo) -> str:
    """Generate a human-readable analysis report for a table.

    Args:
        table_info: Analyzed table information.

    Returns:
        Formatted report string.
    """
    lines = [
        f"{'=' * 60}",
        f"Table: {table_info.full_name}",
        f"{'=' * 60}",
        f"  Location:        {table_info.location}",
        f"  Format:          {table_info.file_format.value}",
        f"  Partitioned:     {table_info.is_partitioned}",
        f"  Total files:     {table_info.total_file_count:,}",
        f"  Total size:      {format_bytes(table_info.total_size_bytes)}",
        f"  Avg file size:   {format_bytes(table_info.avg_file_size_bytes)}",
        f"  Needs compaction: {table_info.needs_compaction}",
    ]

    if table_info.is_partitioned:
        needs = sum(1 for p in table_info.partitions if p.needs_compaction)
        total = len(table_info.partitions)
        lines.append(f"  Partitions:      {total} total, {needs} need compaction")

        if needs > 0:
            lines.append("")
            lines.append("  Partitions needing compaction:")
            for p in table_info.partitions:
                if p.needs_compaction:
                    lines.append(
                        f"    - {p.partition_spec_str}: "
                        f"{p.file_count:,} files, "
                        f"{format_bytes(p.total_size_bytes)}, "
                        f"avg {format_bytes(p.avg_file_size_bytes)} "
                        f"-> target {p.target_files} files"
                    )

    lines.append("")
    report = "\n".join(lines)
    logger.info("\n%s", report)
    return report


def print_compaction_report(report: CompactionReport) -> str:
    """Generate a human-readable compaction report.

    Args:
        report: Compaction report.

    Returns:
        Formatted report string.
    """
    lines = [
        f"{'=' * 60}",
        f"Compaction Report: {report.table_name}",
        f"{'=' * 60}",
        f"  Status:          {report.status.value}",
        f"  Duration:        {report.duration_seconds:.1f}s",
        "",
        "  Before:",
        f"    Files:         {report.before_file_count:,}",
        f"    Size:          {format_bytes(report.before_size_bytes)}",
        f"    Avg file size: {format_bytes(report.before_avg_file_size)}",
    ]

    if report.row_count_before is not None:
        lines.append(f"    Row count:     {report.row_count_before:,}")

    lines.extend(
        [
            "",
            "  After:",
            f"    Files:         {report.after_file_count:,}",
            f"    Size:          {format_bytes(report.after_size_bytes)}",
            f"    Avg file size: {format_bytes(report.after_avg_file_size)}",
        ]
    )

    if report.row_count_after is not None:
        lines.append(f"    Row count:     {report.row_count_after:,}")

    if report.partitions_compacted > 0:
        lines.extend(
            [
                "",
                f"  Partitions compacted: {report.partitions_compacted}",
                f"  Partitions skipped:   {report.partitions_skipped}",
            ]
        )

    if report.error:
        lines.extend(["", f"  ERROR: {report.error}"])

    lines.append("")
    report_str = "\n".join(lines)
    logger.info("\n%s", report_str)
    return report_str
