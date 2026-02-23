"""Tests for beekeeper.config module."""

from __future__ import annotations

import pytest

from beekeeper.config import BeekeeperConfig


class TestBeekeeperConfig:
    def test_defaults(self):
        config = BeekeeperConfig()
        assert config.block_size_mb == 128
        assert config.compaction_ratio_threshold == 10.0
        assert config.backup_prefix == "__bkp"
        assert config.dry_run is False
        assert config.log_level == "INFO"
        assert config.config_file is None
        assert config.database is None
        assert config.table is None
        assert config.tables == []

    def test_block_size_bytes(self):
        config = BeekeeperConfig(block_size_mb=256)
        assert config.block_size_bytes == 256 * 1024 * 1024

    def test_compaction_threshold_bytes(self):
        config = BeekeeperConfig(block_size_mb=128, compaction_ratio_threshold=10.0)
        expected = int((128 * 1024 * 1024) / 10.0)
        assert config.compaction_threshold_bytes == expected

    def test_from_yaml(self, tmp_path):
        yaml_content = "block_size_mb: 256\ncompaction_ratio_threshold: 5.0\ndry_run: true\n"
        yaml_file = tmp_path / "config.yaml"
        yaml_file.write_text(yaml_content)

        config = BeekeeperConfig.from_yaml(yaml_file)
        assert config.block_size_mb == 256
        assert config.compaction_ratio_threshold == 5.0
        assert config.dry_run is True

    def test_from_yaml_missing_file(self):
        with pytest.raises(FileNotFoundError):
            BeekeeperConfig.from_yaml("/nonexistent/config.yaml")

    def test_from_yaml_empty_file(self, tmp_path):
        yaml_file = tmp_path / "empty.yaml"
        yaml_file.write_text("")
        config = BeekeeperConfig.from_yaml(yaml_file)
        assert config.block_size_mb == 128  # defaults

    def test_from_yaml_ignores_unknown_keys(self, tmp_path):
        yaml_content = "block_size_mb: 256\nunknown_key: value\n"
        yaml_file = tmp_path / "config.yaml"
        yaml_file.write_text(yaml_content)

        config = BeekeeperConfig.from_yaml(yaml_file)
        assert config.block_size_mb == 256
        assert not hasattr(config, "unknown_key")

    def test_merge_cli_overrides(self):
        config = BeekeeperConfig(block_size_mb=128)
        new_config = config.merge_cli_overrides(block_size_mb=256, dry_run=True)
        assert new_config.block_size_mb == 256
        assert new_config.dry_run is True
        # Original unchanged
        assert config.block_size_mb == 128

    def test_merge_cli_overrides_ignores_none(self):
        config = BeekeeperConfig(block_size_mb=128)
        new_config = config.merge_cli_overrides(block_size_mb=None, dry_run=None)
        assert new_config.block_size_mb == 128
        assert new_config.dry_run is False

    def test_setup_logging(self):
        config = BeekeeperConfig(log_level="DEBUG")
        config.setup_logging()  # Should not raise
