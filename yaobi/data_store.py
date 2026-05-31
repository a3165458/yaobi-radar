"""Asynchronous SQLite persistence layer for Yaobi Radar."""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, Sequence

import aiosqlite


def utc_now() -> datetime:
    """Return a timezone-aware UTC timestamp."""

    return datetime.now(timezone.utc)


@dataclass(slots=True)
class ScanResultRow:
    """Database shape for a single scan result."""

    symbol: str
    score: int
    volume_score: int
    price_score: int
    oi_score: int
    funding_score: int
    liquidity_score: int
    timestamp: datetime


@dataclass(slots=True)
class PriceHistoryRow:
    """Database shape for price history."""

    symbol: str
    price: float
    volume: float
    timestamp: datetime


@dataclass(slots=True)
class OIHistoryRow:
    """Database shape for open interest history."""

    symbol: str
    open_interest: float
    timestamp: datetime


@dataclass(slots=True)
class AlertRow:
    """Database shape for an emitted alert."""

    symbol: str
    score: int
    alert_level: str
    message: str
    timestamp: datetime


class DataStore:
    """Thin asynchronous wrapper around SQLite with explicit schema management."""

    def __init__(self, db_path: str | Path, retention_days: int = 7) -> None:
        self.db_path = Path(db_path)
        self.retention_days = retention_days
        self._connection: aiosqlite.Connection | None = None
        self._oi_cache: dict[str, float] = {}

    async def connect(self) -> None:
        """Open the SQLite connection and create required tables."""

        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._connection = await aiosqlite.connect(self.db_path)
        self._connection.row_factory = aiosqlite.Row

        # WAL improves concurrent read/write behavior for a daemon-style workload.
        await self._connection.execute("PRAGMA journal_mode = WAL;")
        await self._connection.execute("PRAGMA synchronous = NORMAL;")
        await self._connection.execute("PRAGMA foreign_keys = ON;")
        await self._create_tables()
        await self._connection.commit()
        await self._init_oi_cache()

    async def _init_oi_cache(self) -> None:
        """Load the latest open interest from the database to warm up the in-memory cache."""

        assert self._connection is not None
        query = """
            SELECT symbol, open_interest 
            FROM oi_history 
            WHERE id IN (
                SELECT MAX(id) 
                FROM oi_history 
                GROUP BY symbol
            )
        """
        async with self._connection.execute(query) as cursor:
            rows = await cursor.fetchall()
        self._oi_cache = {str(row["symbol"]): float(row["open_interest"]) for row in rows}

    async def close(self) -> None:
        """Close the SQLite connection cleanly."""

        if self._connection is not None:
            await self._connection.close()
            self._connection = None

    async def _create_tables(self) -> None:
        """Create all application tables and indexes."""

        assert self._connection is not None
        await self._connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS scan_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                score INTEGER NOT NULL,
                volume_score INTEGER NOT NULL,
                price_score INTEGER NOT NULL,
                oi_score INTEGER NOT NULL,
                funding_score INTEGER NOT NULL,
                liquidity_score INTEGER NOT NULL,
                timestamp TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                score INTEGER NOT NULL,
                alert_level TEXT NOT NULL,
                message TEXT NOT NULL,
                timestamp TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS price_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                price REAL NOT NULL,
                volume REAL NOT NULL,
                timestamp TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS oi_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                open_interest REAL NOT NULL,
                timestamp TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_scan_results_symbol_ts
            ON scan_results(symbol, timestamp DESC);

            CREATE INDEX IF NOT EXISTS idx_alerts_symbol_ts
            ON alerts(symbol, timestamp DESC);

            CREATE INDEX IF NOT EXISTS idx_price_history_symbol_ts
            ON price_history(symbol, timestamp DESC);

            CREATE INDEX IF NOT EXISTS idx_oi_history_symbol_ts
            ON oi_history(symbol, timestamp DESC);
            """
        )

    async def record_scan_batch(
        self,
        scan_rows: Sequence[ScanResultRow],
        price_rows: Sequence[PriceHistoryRow],
        oi_rows: Sequence[OIHistoryRow],
    ) -> None:
        """Persist one full scan cycle in a single transaction."""

        if not scan_rows and not price_rows and not oi_rows:
            return

        assert self._connection is not None
        await self._connection.executemany(
            """
            INSERT INTO scan_results (
                symbol, score, volume_score, price_score, oi_score,
                funding_score, liquidity_score, timestamp
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    row.symbol,
                    row.score,
                    row.volume_score,
                    row.price_score,
                    row.oi_score,
                    row.funding_score,
                    row.liquidity_score,
                    row.timestamp.isoformat(),
                )
                for row in scan_rows
            ],
        )
        await self._connection.executemany(
            """
            INSERT INTO price_history (symbol, price, volume, timestamp)
            VALUES (?, ?, ?, ?)
            """,
            [
                (row.symbol, row.price, row.volume, row.timestamp.isoformat())
                for row in price_rows
            ],
        )
        await self._connection.executemany(
            """
            INSERT INTO oi_history (symbol, open_interest, timestamp)
            VALUES (?, ?, ?)
            """,
            [
                (row.symbol, row.open_interest, row.timestamp.isoformat())
                for row in oi_rows
            ],
        )
        await self._connection.commit()

        # Update in-memory open interest cache
        for row in oi_rows:
            self._oi_cache[row.symbol] = row.open_interest

    async def record_alert(self, alert: AlertRow) -> None:
        """Persist a Telegram alert record."""

        assert self._connection is not None
        await self._connection.execute(
            """
            INSERT INTO alerts (symbol, score, alert_level, message, timestamp)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                alert.symbol,
                alert.score,
                alert.alert_level,
                alert.message,
                alert.timestamp.isoformat(),
            ),
        )
        await self._connection.commit()

    async def get_latest_oi_map(self, symbols: Iterable[str]) -> dict[str, float]:
        """Fetch the most recent stored open interest per symbol from the cache."""

        return {symbol: self._oi_cache[symbol] for symbol in symbols if symbol in self._oi_cache}

    async def get_recent_alert_symbols(
        self,
        symbols: Iterable[str],
        since: datetime,
    ) -> set[str]:
        """Return symbols that have already alerted inside the cooldown window."""

        symbol_list = list(dict.fromkeys(symbols))
        if not symbol_list:
            return set()

        assert self._connection is not None
        placeholders = ", ".join("?" for _ in symbol_list)
        query = f"""
            SELECT DISTINCT symbol
            FROM alerts
            WHERE symbol IN ({placeholders}) AND timestamp >= ?
        """
        params = [*symbol_list, since.isoformat()]
        async with self._connection.execute(query, params) as cursor:
            rows = await cursor.fetchall()
        return {str(row["symbol"]) for row in rows}

    async def prune_old_records(self, now: datetime | None = None) -> dict[str, int]:
        """Delete records older than the configured retention window in batches."""

        cutoff = (now or utc_now()) - timedelta(days=self.retention_days)
        cutoff_text = cutoff.isoformat()
        assert self._connection is not None

        tables = ("scan_results", "alerts", "price_history", "oi_history")
        deleted: dict[str, int] = {}
        for table_name in tables:
            total_deleted = 0
            while True:
                cursor = await self._connection.execute(
                    f"""
                    DELETE FROM {table_name}
                    WHERE id IN (
                        SELECT id FROM {table_name}
                        WHERE timestamp < ?
                        LIMIT 5000
                    )
                    """,
                    (cutoff_text,),
                )
                await self._connection.commit()
                count = cursor.rowcount or 0
                total_deleted += count
                if count < 5000:
                    break
                await asyncio.sleep(0.05)
            deleted[table_name] = total_deleted

        return deleted

    async def export_debug_snapshot(self) -> str:
        """Return a compact JSON string with recent table counts for diagnostics."""

        assert self._connection is not None
        snapshot: dict[str, int] = {}
        for table_name in ("scan_results", "alerts", "price_history", "oi_history"):
            async with self._connection.execute(
                f"SELECT COUNT(*) AS total FROM {table_name}"
            ) as cursor:
                row = await cursor.fetchone()
            snapshot[table_name] = int(row["total"])
        return json.dumps(snapshot, ensure_ascii=True)
