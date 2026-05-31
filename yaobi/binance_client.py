"""Async Binance Futures REST client with rolling rate limiting."""

from __future__ import annotations

import asyncio
import logging
from collections import deque
from dataclasses import dataclass
from typing import Any

import aiohttp

from .config import AppConfig


class BinanceAPIError(RuntimeError):
    """Raised when Binance REST calls fail after retries."""


@dataclass(slots=True)
class Kline:
    """Normalized 5-minute kline representation."""

    open_time: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    close_time: int
    quote_volume: float

    @classmethod
    def from_api(cls, payload: list[Any]) -> "Kline":
        """Create a Kline from Binance's raw array response."""

        return cls(
            open_time=int(payload[0]),
            open=float(payload[1]),
            high=float(payload[2]),
            low=float(payload[3]),
            close=float(payload[4]),
            volume=float(payload[5]),
            close_time=int(payload[6]),
            quote_volume=float(payload[7]),
        )


class RollingRateLimiter:
    """Sliding-window limiter that honors Binance's per-minute request budget using a FIFO queue."""

    def __init__(self, limit_per_minute: int) -> None:
        self.limit_per_minute = limit_per_minute
        self._events: deque[tuple[float, int]] = deque()
        self._queue: deque[tuple[asyncio.Future[None], int]] = deque()
        self._lock = asyncio.Lock()
        self._checking = False

    async def acquire(self, weight: int = 1) -> None:
        """Wait until enough request budget is available."""

        if weight <= 0:
            raise ValueError("request weight must be greater than 0")
        if weight > self.limit_per_minute:
            raise ValueError("request weight cannot exceed the per-minute limit")

        future = asyncio.get_running_loop().create_future()
        async with self._lock:
            self._queue.append((future, weight))
            self._maybe_process_queue()

        await future

    def _maybe_process_queue(self) -> None:
        if self._checking or not self._queue:
            return
        self._checking = True
        asyncio.create_task(self._process_queue())

    async def _process_queue(self) -> None:
        try:
            while True:
                async with self._lock:
                    if not self._queue:
                        break

                    now = asyncio.get_running_loop().time()
                    while self._events and now - self._events[0][0] >= 60:
                        self._events.popleft()

                    used_weight = sum(item[1] for item in self._events)

                    future, weight = self._queue[0]
                    if future.cancelled():
                        self._queue.popleft()
                        continue

                    if used_weight + weight <= self.limit_per_minute:
                        self._queue.popleft()
                        self._events.append((now, weight))
                        if not future.done():
                            future.set_result(None)
                        continue

                    wait_seconds = 60 - (now - self._events[0][0])

                await asyncio.sleep(max(wait_seconds, 0.05))
        finally:
            async with self._lock:
                self._checking = False
                if self._queue:
                    self._maybe_process_queue()


class BinanceClient:
    """Binance Futures REST client used by the scanner."""

    def __init__(self, config: AppConfig, logger: logging.Logger | None = None) -> None:
        self.config = config
        self.logger = logger or logging.getLogger(__name__)
        self._session: aiohttp.ClientSession | None = None
        self._rate_limiter = RollingRateLimiter(config.binance.rate_limit)
        self._timeout = aiohttp.ClientTimeout(total=15)
        self._concurrency = asyncio.Semaphore(30)

    async def start(self) -> None:
        """Create the shared aiohttp session."""

        if self._session is not None:
            return

        headers = {}
        if self.config.binance.api_key:
            headers["X-MBX-APIKEY"] = self.config.binance.api_key

        connector = aiohttp.TCPConnector(limit=60)
        self._session = aiohttp.ClientSession(
            timeout=self._timeout,
            headers=headers,
            connector=connector,
        )

    async def close(self) -> None:
        """Close the aiohttp session."""

        if self._session is not None:
            await self._session.close()
            self._session = None

    async def _request_json(
        self,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        weight: int = 1,
        max_retries: int = 4,
    ) -> Any:
        """Send a JSON request with retry and rate-limit handling."""

        if self._session is None:
            raise RuntimeError("BinanceClient.start() must be called before requests")

        url = f"{self.config.binance.base_url}{path}"
        last_error: Exception | None = None

        for attempt in range(1, max_retries + 1):
            await self._rate_limiter.acquire(weight)
            async with self._concurrency:
                try:
                    async with self._session.get(url, params=params) as response:
                        if response.status in (418, 429):
                            retry_after = float(response.headers.get("Retry-After", "1"))
                            body = await response.text()
                            self.logger.warning(
                                "Binance rate limit response %s on %s (attempt %s/%s): %s",
                                response.status,
                                path,
                                attempt,
                                max_retries,
                                body[:200],
                            )
                            await asyncio.sleep(retry_after)
                            continue

                        if 500 <= response.status < 600:
                            body = await response.text()
                            self.logger.warning(
                                "Binance server error %s on %s (attempt %s/%s): %s",
                                response.status,
                                path,
                                attempt,
                                max_retries,
                                body[:200],
                            )
                            await asyncio.sleep(min(2**attempt, 8))
                            continue

                        response.raise_for_status()
                        return await response.json()
                except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                    last_error = exc
                    self.logger.warning(
                        "Binance request failed for %s (attempt %s/%s): %s",
                        path,
                        attempt,
                        max_retries,
                        exc,
                    )
                    await asyncio.sleep(min(2**attempt, 8))

        raise BinanceAPIError(f"request failed for {path}: {last_error}")

    async def get_ticker_24hr(self) -> list[dict[str, Any]]:
        """Fetch 24h statistics for all futures contracts."""

        payload = await self._request_json("/fapi/v1/ticker/24hr", weight=40)
        if not isinstance(payload, list):
            raise BinanceAPIError("unexpected ticker payload shape")
        return [item for item in payload if isinstance(item, dict)]

    async def get_all_funding_rates(self) -> dict[str, float]:
        """Fetch the latest funding rate snapshot for all contracts.

        Binance's premiumIndex endpoint is used here because it returns the latest
        funding rate for all symbols in one call, which is much more suitable for
        a market-wide scanner than calling /fundingRate per symbol.
        """

        payload = await self._request_json("/fapi/v1/premiumIndex", weight=10)
        if not isinstance(payload, list):
            raise BinanceAPIError("unexpected premiumIndex payload shape")

        funding_map: dict[str, float] = {}
        for item in payload:
            if isinstance(item, dict) and "symbol" in item and "lastFundingRate" in item:
                funding_map[str(item["symbol"])] = float(item["lastFundingRate"])
        return funding_map

    async def get_open_interest(self, symbol: str) -> float:
        """Fetch the current open interest for a single symbol."""

        payload = await self._request_json(
            "/fapi/v1/openInterest",
            params={"symbol": symbol},
            weight=1,
        )
        if not isinstance(payload, dict) or "openInterest" not in payload:
            raise BinanceAPIError(f"unexpected open interest payload for {symbol}")
        return float(payload["openInterest"])

    async def get_klines(self, symbol: str, interval: str = "5m", limit: int = 12) -> list[Kline]:
        """Fetch recent klines for a symbol."""

        payload = await self._request_json(
            "/fapi/v1/klines",
            params={"symbol": symbol, "interval": interval, "limit": limit},
            weight=1,
        )
        if not isinstance(payload, list):
            raise BinanceAPIError(f"unexpected kline payload for {symbol}")
        return [Kline.from_api(item) for item in payload if isinstance(item, list)]

    async def get_many_open_interests(self, symbols: list[str]) -> dict[str, float]:
        """Fetch open interest for multiple symbols concurrently."""

        async def fetch(symbol: str) -> tuple[str, float | None]:
            try:
                return symbol, await asyncio.wait_for(
                    self.get_open_interest(symbol), timeout=30.0
                )
            except Exception as exc:
                self.logger.warning("Skipping %s open interest: %s", symbol, exc)
                return symbol, None

        results = await asyncio.gather(*(fetch(symbol) for symbol in symbols))
        return {symbol: value for symbol, value in results if value is not None}

    async def get_many_recent_klines(self, symbols: list[str], limit: int = 12) -> dict[str, list[Kline]]:
        """Fetch the latest 5m kline window for multiple symbols concurrently."""

        async def fetch(symbol: str) -> tuple[str, list[Kline] | None]:
            try:
                return symbol, await asyncio.wait_for(
                    self.get_klines(symbol, interval="5m", limit=limit), timeout=30.0
                )
            except Exception as exc:
                self.logger.warning("Skipping %s klines: %s", symbol, exc)
                return symbol, None

        results = await asyncio.gather(*(fetch(symbol) for symbol in symbols))
        return {symbol: value for symbol, value in results if value is not None}
