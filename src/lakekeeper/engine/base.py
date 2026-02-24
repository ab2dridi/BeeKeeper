"""Abstract base class for compaction engines."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from lakekeeper.models import BackupInfo, CompactionReport, TableInfo


class CompactionEngine(ABC):
    """Abstract base class defining the compaction engine interface."""

    @abstractmethod
    def analyze(self, database: str, table_name: str) -> TableInfo:
        """Analyze a table and return detailed information.

        Args:
            database: Database name.
            table_name: Table name.

        Returns:
            TableInfo with analysis results.
        """

    @abstractmethod
    def create_backup(self, table_info: TableInfo) -> BackupInfo:
        """Create a zero-copy backup of a table.

        Args:
            table_info: Table information from analysis.

        Returns:
            BackupInfo with backup details.
        """

    @abstractmethod
    def compact(self, table_info: TableInfo, backup_info: BackupInfo) -> CompactionReport:
        """Perform compaction on a table.

        Args:
            table_info: Table information from analysis.
            backup_info: Backup information.

        Returns:
            CompactionReport with results.
        """

    @abstractmethod
    def rollback(self, database: str, table_name: str, backup_table: str | None = None) -> BackupInfo:
        """Rollback a table to its pre-compaction state.

        Args:
            database: Database name.
            table_name: Table name.
            backup_table: Specific backup table to use. If None, uses most recent.

        Returns:
            BackupInfo of the backup that was used for the rollback.
        """

    @abstractmethod
    def cleanup(self, database: str, table_name: str, older_than_days: int | None = None) -> int:
        """Clean up backup tables and compacted data.

        Args:
            database: Database name.
            table_name: Table name.
            older_than_days: Only clean backups older than this many days.

        Returns:
            Number of backups cleaned up.
        """

    @abstractmethod
    def list_tables(self, database: str) -> list[str]:
        """List external tables in a database.

        Args:
            database: Database name.

        Returns:
            List of external table names.
        """
