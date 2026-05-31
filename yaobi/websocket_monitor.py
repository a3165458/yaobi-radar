"""Real-time mark price monitor using Binance Futures WebSocket streams."""

from __future__ import annotations

import asyncio
import json
import logging

import websockets
from websockets.asyncio.client import ClientConnection
from websockets.exceptions import ConnectionClosed

from .config import AppConfig


class WebSocketMonitor:
    """Maintains the freshest mark prices in memory for the scanner."""

    def __init__(self, config: AppConfig, logger: logging.Logger | None = None) -> None:
        self.config = config
        self.logger = logger or logging.getLogger(__name__)
        self._latest_prices: dict[str, float] = {}
        self._latest_tickers: dict[str, dict[str, Any]] = {}
        self._latest_funding_rates: dict[str, float] = {}

    async def run(self, stop_event: asyncio.Event) -> None:
        """Start the reconnecting mark-price listener."""

        if not self.config.websocket.enabled:
            self.logger.info("WebSocket monitor disabled by configuration.")
            return

        while not stop_event.is_set():
            try:
                async with websockets.connect(
                    self.config.binance.ws_url,
                    ping_interval=self.config.websocket.ping_interval,
                    ping_timeout=self.config.websocket.ping_interval,
                    open_timeout=15,
                    close_timeout=10,
                    max_queue=4096,
                ) as websocket:
                    await self._subscribe_all_mark_prices(websocket)
                    self.logger.info("Connected to Binance mark price WebSocket.")

                    async for message in websocket:
                        if stop_event.is_set():
                            break
                        await self._handle_message(message)
            except (ConnectionClosed, OSError, asyncio.TimeoutError) as exc:
                self.logger.warning("WebSocket connection dropped: %s", exc)
                await self._sleep_or_stop(
                    stop_event, self.config.websocket.reconnect_delay
                )

    async def _subscribe_all_mark_prices(self, websocket: ClientConnection) -> None:
        """Subscribe to the global 1-second mark price feed and 24h ticker feed."""

        request = {
            "method": "SUBSCRIBE",
            "params": ["!markPrice@arr@1s", "!ticker@arr"],
            "id": 1,
        }
        await websocket.send(json.dumps(request))

    async def _handle_message(self, message: str) -> None:
        """Parse incoming WebSocket messages and update the cache."""

        payload = json.loads(message)
        if isinstance(payload, dict) and "result" in payload:
            return

        entries = payload if isinstance(payload, list) else payload.get("data", [])
        if not isinstance(entries, list) or not entries:
            return

        event_type = entries[0].get("e")
        if event_type == "markPriceUpdate":
            for item in entries:
                symbol = item.get("s")
                mark_price = item.get("p")
                funding_rate = item.get("r")
                if symbol:
                    if mark_price is not None:
                        self._latest_prices[str(symbol)] = float(mark_price)
                    if funding_rate is not None:
                        self._latest_funding_rates[str(symbol)] = float(funding_rate)
        elif event_type == "24hrTicker":
            for item in entries:
                symbol = item.get("s")
                if symbol:
                    self._latest_tickers[str(symbol)] = {
                        "price_change_percent": float(item.get("P", 0.0)),
                        "quote_volume": float(item.get("q", 0.0)),
                        "last_price": float(item.get("c", 0.0)),
                    }

    async def _sleep_or_stop(self, stop_event: asyncio.Event, seconds: int) -> None:
        """Sleep for the reconnect delay unless a shutdown signal arrives."""

        try:
            await asyncio.wait_for(stop_event.wait(), timeout=seconds)
        except asyncio.TimeoutError:
            return

    def get_price(self, symbol: str) -> float | None:
        """Return the latest mark price for a symbol, if available."""

        return self._latest_prices.get(symbol)

    def get_all_tickers(self) -> list[dict[str, Any]]:
        """Return a list of ticker dicts in the same format as Binance REST API."""

        return [
            {
                "symbol": symbol,
                "lastPrice": str(data["last_price"]),
                "priceChangePercent": str(data["price_change_percent"]),
                "quoteVolume": str(data["quote_volume"]),
            }
            for symbol, data in self._latest_tickers.items()
        ]

    def get_all_funding_rates(self) -> dict[str, float]:
        """Return a snapshot of the latest funding rates."""

        return dict(self._latest_funding_rates)
