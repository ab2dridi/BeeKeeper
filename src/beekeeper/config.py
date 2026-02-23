"""Configuration management for Beekeeper."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)


@dataclass
class BeekeeperConfig:
    """Beekeeper configuration with defaults, YAML override, and CLI override."""

    block_size_mb: int = 128
    compaction_ratio_threshold: float = 10.0
    backup_prefix: str = "__bkp"
    dry_run: bool = False
    log_level: str = "INFO"
    config_file: str | None = None
    database: str | None = None
    table: str | None = None
    tables: list[str] = field(default_factory=list)

    @classmethod
    def from_yaml(cls, path: str | Path) -> BeekeeperConfig:
        """Load configuration from a YAML file.

        Args:
            path: Path to the YAML configuration file.

        Returns:
            A BeekeeperConfig instance with values from the YAML file.

        Raises:
            FileNotFoundError: If the config file does not exist.
        """
        path = Path(path)
        if not path.exists():
            msg = f"Config file not found: {path}"
            raise FileNotFoundError(msg)

        with open(path) as f:
            data = yaml.safe_load(f) or {}

        return cls._from_dict(data)

    @classmethod
    def _from_dict(cls, data: dict[str, Any]) -> BeekeeperConfig:
        """Create config from a dictionary, ignoring unknown keys."""
        valid_fields = {f.name for f in cls.__dataclass_fields__.values()}
        filtered = {k: v for k, v in data.items() if k in valid_fields}
        return cls(**filtered)

    def merge_cli_overrides(self, **kwargs: Any) -> BeekeeperConfig:
        """Return a new config with CLI overrides applied (non-None values only).

        Args:
            **kwargs: CLI parameter overrides.

        Returns:
            A new BeekeeperConfig with overrides applied.
        """
        current = {f.name: getattr(self, f.name) for f in self.__dataclass_fields__.values()}
        for key, value in kwargs.items():
            if value is not None and key in current:
                current[key] = value
        return BeekeeperConfig(**current)

    def setup_logging(self) -> None:
        """Configure logging based on the log_level setting."""
        logging.basicConfig(
            level=getattr(logging, self.log_level.upper(), logging.INFO),
            format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

    @property
    def block_size_bytes(self) -> int:
        """Block size in bytes."""
        return self.block_size_mb * 1024 * 1024

    @property
    def compaction_threshold_bytes(self) -> int:
        """Files smaller than this threshold trigger compaction."""
        return int(self.block_size_bytes / self.compaction_ratio_threshold)
