"""Configuration management using Pydantic and YAML."""

from pathlib import Path
from typing import Any, Dict, Optional

import yaml
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Environment
    environment: str = Field(default="development")
    log_level: str = Field(default="INFO")

    # CEX API Keys
    binance_api_key: str = Field(default="")
    binance_api_secret: str = Field(default="")

    bybit_api_key: str = Field(default="")
    bybit_api_secret: str = Field(default="")

    okx_api_key: str = Field(default="")
    okx_api_secret: str = Field(default="")
    okx_passphrase: str = Field(default="")

    # PerpDEX API Keys
    hyperliquid_api_wallet_address: str = Field(default="")
    hyperliquid_api_wallet_private_key: str = Field(default="")

    lighter_api_key_private_key: str = Field(default="")
    lighter_account_index: int = Field(default=0)
    lighter_api_key_index: int = Field(default=0)
    lighter_max_slippage_bps: int = Field(default=1000)

    # Database
    duckdb_path: str = Field(default="data/duckdb/arb_engine.db")
    parquet_path: str = Field(default="data/parquet")

    # Collection
    collection_interval_seconds: int = Field(default=1)
    orderbook_depth: int = Field(default=5)

    # NTP
    ntp_server: str = Field(default="pool.ntp.org")


class ConfigLoader:
    """Load and manage YAML configuration files."""

    def __init__(self, config_dir: Path = Path("configs")):
        """
        Initialize config loader.

        Args:
            config_dir: Directory containing YAML config files
        """
        self.config_dir = config_dir
        self._cache: Dict[str, Any] = {}

    def load_yaml(self, filename: str, use_cache: bool = True) -> Dict[str, Any]:
        """
        Load YAML configuration file.

        Args:
            filename: Name of YAML file (e.g., "exchanges.yaml")
            use_cache: If True, cache loaded config

        Returns:
            Parsed YAML as dictionary
        """
        if use_cache and filename in self._cache:
            return self._cache[filename]

        filepath = self.config_dir / filename
        if not filepath.exists():
            raise FileNotFoundError(f"Config file not found: {filepath}")

        with open(filepath, "r") as f:
            config = yaml.safe_load(f)

        if use_cache:
            self._cache[filename] = config

        return config

    def load_exchanges(self) -> Dict[str, Any]:
        """Load exchanges configuration."""
        return self.load_yaml("exchanges.yaml")

    def load_fees(self) -> Dict[str, Any]:
        """Load fees configuration."""
        return self.load_yaml("fees.yaml")

    def load_strategy(self) -> Dict[str, Any]:
        """Load strategy configuration."""
        return self.load_yaml("strategy.yaml")

    def load_storage(self) -> Dict[str, Any]:
        """Load storage configuration."""
        return self.load_yaml("storage.yaml")

    def reload(self) -> None:
        """Clear cache and force reload on next access."""
        self._cache.clear()


# Global instances
_settings: Optional[Settings] = None
_config_loader: Optional[ConfigLoader] = None


def get_settings() -> Settings:
    """
    Get or create global settings instance.

    Returns:
        Application settings
    """
    global _settings
    if _settings is None:
        _settings = Settings()
    return _settings


def get_config_loader(config_dir: Optional[Path] = None) -> ConfigLoader:
    """
    Get or create global config loader instance.

    Args:
        config_dir: Optional config directory path

    Returns:
        Config loader instance
    """
    global _config_loader
    if _config_loader is None:
        if config_dir is None:
            config_dir = Path("configs")
        _config_loader = ConfigLoader(config_dir=config_dir)
    return _config_loader
