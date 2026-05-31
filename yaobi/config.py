"""Configuration loading and validation helpers for Yaobi Radar."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any


@dataclass(slots=True)
class BinanceConfig:
    """Binance REST/WebSocket related settings."""

    base_url: str = "https://fapi.binance.com"
    ws_url: str = "wss://fstream.binance.com/ws"
    api_key: str = ""
    api_secret: str = ""
    rate_limit: int = 1200


@dataclass(slots=True)
class ScanningConfig:
    """Market scanning scope and cadence settings."""

    interval_seconds: int = 60
    symbols: list[str] = field(default_factory=list)
    exclude_symbols: list[str] = field(default_factory=list)
    min_24h_volume_usdt: float = 1_000_000
    max_symbols: int = 500


@dataclass(slots=True)
class ScoringConfig:
    """Weights and thresholds used by the scoring engine."""

    volume_weight: int = 40
    price_weight: int = 25
    oi_weight: int = 20
    funding_weight: int = 10
    liquidity_weight: int = 5
    alert_threshold: int = 50
    critical_threshold: int = 70


@dataclass(slots=True)
class TelegramConfig:
    """Telegram delivery settings."""

    bot_token: str = ""
    chat_id: str = ""
    thread_id: int | None = None
    enabled: bool = True


@dataclass(slots=True)
class DatabaseConfig:
    """SQLite persistence settings."""

    path: str = "data/yaobi.db"
    retention_days: int = 7


@dataclass(slots=True)
class WebSocketConfig:
    """WebSocket monitoring settings."""

    enabled: bool = True
    reconnect_delay: int = 5
    ping_interval: int = 30


@dataclass(slots=True)
class LoggingConfig:
    """File and console logging settings."""

    level: str = "INFO"
    file: str = "logs/yaobi.log"


@dataclass(slots=True)
class AppConfig:
    """Top-level application configuration object."""

    binance: BinanceConfig = field(default_factory=BinanceConfig)
    scanning: ScanningConfig = field(default_factory=ScanningConfig)
    scoring: ScoringConfig = field(default_factory=ScoringConfig)
    cooldown_minutes: int = 30
    telegram: TelegramConfig = field(default_factory=TelegramConfig)
    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    websocket: WebSocketConfig = field(default_factory=WebSocketConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    root_dir: Path = field(default_factory=lambda: Path.cwd(), repr=False)
    source_path: Path = field(default_factory=lambda: Path("config.json"), repr=False)

    @classmethod
    def load(cls, path: str | Path = "config.json") -> "AppConfig":
        """Load configuration from disk and validate the resulting object."""

        config_path = Path(path).expanduser().resolve()
        raw = json.loads(config_path.read_text(encoding="utf-8"))

        config = cls(
            binance=BinanceConfig(**raw.get("binance", {})),
            scanning=ScanningConfig(**raw.get("scanning", {})),
            scoring=ScoringConfig(**raw.get("scoring", {})),
            cooldown_minutes=int(raw.get("cooldown_minutes", 30)),
            telegram=TelegramConfig(**raw.get("telegram", {})),
            database=DatabaseConfig(**raw.get("database", {})),
            websocket=WebSocketConfig(**raw.get("websocket", {})),
            logging=LoggingConfig(**raw.get("logging", {})),
            root_dir=config_path.parent,
            source_path=config_path,
        )
        config.validate()
        return config

    def validate(self) -> None:
        """Validate configuration values that can break runtime behavior."""

        if self.binance.rate_limit <= 0:
            raise ValueError("binance.rate_limit must be greater than 0")
        if self.scanning.interval_seconds <= 0:
            raise ValueError("scanning.interval_seconds must be greater than 0")
        if self.scanning.max_symbols <= 0:
            raise ValueError("scanning.max_symbols must be greater than 0")
        if self.scanning.min_24h_volume_usdt < 0:
            raise ValueError("scanning.min_24h_volume_usdt cannot be negative")
        if self.cooldown_minutes < 0:
            raise ValueError("cooldown_minutes cannot be negative")
        if self.database.retention_days <= 0:
            raise ValueError("database.retention_days must be greater than 0")
        if self.websocket.reconnect_delay <= 0:
            raise ValueError("websocket.reconnect_delay must be greater than 0")
        if self.websocket.ping_interval <= 0:
            raise ValueError("websocket.ping_interval must be greater than 0")

        total_weight = (
            self.scoring.volume_weight
            + self.scoring.price_weight
            + self.scoring.oi_weight
            + self.scoring.funding_weight
            + self.scoring.liquidity_weight
        )
        if total_weight <= 0:
            raise ValueError("total scoring weight must be greater than 0")
        if self.scoring.alert_threshold <= 0:
            raise ValueError("scoring.alert_threshold must be greater than 0")
        if self.scoring.critical_threshold < self.scoring.alert_threshold:
            raise ValueError("scoring.critical_threshold must be >= alert_threshold")

    @property
    def database_path(self) -> Path:
        """Return the absolute SQLite path."""

        return (self.root_dir / self.database.path).resolve()

    @property
    def log_file_path(self) -> Path:
        """Return the absolute log file path."""

        return (self.root_dir / self.logging.file).resolve()

    def to_dict(self) -> dict[str, Any]:
        """Serialize the configuration back to plain dictionaries."""

        payload = asdict(self)
        payload["root_dir"] = str(self.root_dir)
        payload["source_path"] = str(self.source_path)
        return payload
