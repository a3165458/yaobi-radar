"""Main market scanning loop and alert orchestration."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from .binance_client import BinanceClient, Kline
from .config import AppConfig
from .data_store import AlertRow, DataStore, OIHistoryRow, PriceHistoryRow, ScanResultRow
from .scoring import ScanMetrics, calculate_score, classify_direction
from .telegram_bot import AlertPayload, TelegramBot, TelegramDeliveryError
from .websocket_monitor import WebSocketMonitor


@dataclass(slots=True)
class SymbolTicker:
    """Normalized subset of Binance 24h ticker fields."""

    symbol: str
    last_price: float
    price_change_percent: float
    quote_volume: float


def _pct_change(current: float, reference: float) -> float:
    """Return the percentage move from reference to current."""

    if reference == 0:
        return 0.0
    return (current - reference) / reference * 100


class Scanner:
    """Runs the periodic market-wide scan and handles cooldown-aware alerts."""

    def __init__(
        self,
        config: AppConfig,
        client: BinanceClient,
        store: DataStore,
        telegram: TelegramBot,
        websocket_monitor: WebSocketMonitor | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self.config = config
        self.client = client
        self.store = store
        self.telegram = telegram
        self.websocket_monitor = websocket_monitor
        self.logger = logger or logging.getLogger(__name__)
        self._last_cleanup_at: datetime | None = None
        self._cleanup_task: asyncio.Task[None] | None = None
        self._last_funding_rates: dict[str, float] = {}

    async def run(self, stop_event: asyncio.Event) -> None:
        """Run the scanning loop until shutdown is requested."""

        try:
            while not stop_event.is_set():
                cycle_started = datetime.now(timezone.utc)
                try:
                    await self.scan_once(cycle_started)
                except Exception as exc:
                    self.logger.error("Error during scan cycle: %s", exc, exc_info=True)

                elapsed = (datetime.now(timezone.utc) - cycle_started).total_seconds()
                sleep_for = max(self.config.scanning.interval_seconds - elapsed, 0)
                if sleep_for == 0:
                    # Yield control to prevent CPU-bound tight loop on continuous failures
                    await asyncio.sleep(1.0)
                    continue

                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=sleep_for)
                except asyncio.TimeoutError:
                    continue
        finally:
            if self._cleanup_task is not None and not self._cleanup_task.done():
                self.logger.info("Waiting for background DB cleanup to complete...")
                try:
                    await self._cleanup_task
                except Exception as exc:
                    self.logger.warning("Error waiting for cleanup task during shutdown: %s", exc)

    async def scan_once(self, timestamp: datetime | None = None) -> None:
        """Execute one full REST-based scan cycle."""

        scan_time = timestamp or datetime.now(timezone.utc)

        # Try to load tickers from WebSocket to save API weight
        tickers_loaded_from_ws = False
        if self.websocket_monitor is not None:
            ws_tickers = self.websocket_monitor.get_all_tickers()
            if ws_tickers:
                ticker_rows = ws_tickers
                tickers_loaded_from_ws = True

        if not tickers_loaded_from_ws:
            ticker_rows = await self.client.get_ticker_24hr()

        selected_tickers = self._select_symbols(ticker_rows)
        if not selected_tickers:
            self.logger.warning("No symbols matched the current scan filters.")
            return

        symbols = [ticker.symbol for ticker in selected_tickers]
        previous_oi_map = await self.store.get_latest_oi_map(symbols)
        recent_alerts = await self.store.get_recent_alert_symbols(
            symbols,
            scan_time - timedelta(minutes=self.config.cooldown_minutes),
        )

        # Try to load funding rates from WebSocket to save API weight
        funding_loaded_from_ws = False
        funding_map = {}
        if self.websocket_monitor is not None:
            ws_funding = self.websocket_monitor.get_all_funding_rates()
            if ws_funding:
                funding_map = ws_funding
                funding_loaded_from_ws = True

        if funding_loaded_from_ws:
            oi_map, kline_map = await asyncio.gather(
                self.client.get_many_open_interests(symbols),
                self.client.get_many_recent_klines(symbols),
            )
        else:
            ws_funding_map, oi_map, kline_map = await asyncio.gather(
                self.client.get_all_funding_rates(),
                self.client.get_many_open_interests(symbols),
                self.client.get_many_recent_klines(symbols),
            )
            funding_map = ws_funding_map

        scan_rows: list[ScanResultRow] = []
        price_rows: list[PriceHistoryRow] = []
        oi_rows: list[OIHistoryRow] = []
        alert_candidates: list[tuple[AlertPayload, str]] = []

        for ticker in selected_tickers:
            symbol = ticker.symbol
            open_interest = oi_map.get(symbol)
            klines = kline_map.get(symbol)
            if open_interest is None or not klines:
                self.logger.debug(
                    "Skipping %s because open interest or kline data is missing.",
                    symbol,
                )
                continue

            current_price = ticker.last_price
            if self.websocket_monitor is not None:
                live_price = self.websocket_monitor.get_price(symbol)
                if live_price is not None:
                    current_price = live_price

            current_funding = funding_map.get(symbol, 0.0)
            prev_funding = self._last_funding_rates.get(symbol, current_funding)
            funding_change = current_funding - prev_funding
            self._last_funding_rates[symbol] = current_funding

            metrics = self._build_metrics(
                ticker=ticker,
                current_price=current_price,
                klines=klines,
                open_interest=open_interest,
                previous_open_interest=previous_oi_map.get(symbol),
                funding_rate=current_funding,
                funding_rate_change=funding_change,
            )
            breakdown = calculate_score(metrics, self.config.scoring)

            scan_rows.append(
                ScanResultRow(
                    symbol=symbol,
                    score=breakdown.total_score,
                    volume_score=breakdown.volume_score,
                    price_score=breakdown.price_score,
                    oi_score=breakdown.oi_score,
                    funding_score=breakdown.funding_score,
                    liquidity_score=breakdown.liquidity_score,
                    timestamp=scan_time,
                )
            )
            price_rows.append(
                PriceHistoryRow(
                    symbol=symbol,
                    price=current_price,
                    volume=metrics.current_5m_volume,
                    timestamp=scan_time,
                )
            )
            oi_rows.append(
                OIHistoryRow(
                    symbol=symbol,
                    open_interest=open_interest,
                    timestamp=scan_time,
                )
            )

            if breakdown.alert_level in {"warning", "critical"} and symbol not in recent_alerts:
                alert_candidates.append(
                    (
                        AlertPayload(
                            symbol=symbol,
                            score=breakdown.total_score,
                            level=breakdown.alert_level,
                            change_5m=metrics.change_5m,
                            volume_ratio=breakdown.volume_ratio,
                            oi_change_pct=breakdown.oi_change_pct,
                            funding_rate=metrics.funding_rate * 100,
                            price=current_price,
                            volume_24h=ticker.quote_volume,
                            change_24h=ticker.price_change_percent,
                            timestamp=scan_time,
                        ),
                        breakdown.alert_level,
                    )
                )

        await self.store.record_scan_batch(scan_rows, price_rows, oi_rows)
        await self._emit_alerts(alert_candidates, scan_time)
        await self._maybe_cleanup(scan_time)

        self.logger.info(
            "Scan complete: scanned=%s stored=%s alerts=%s",
            len(selected_tickers),
            len(scan_rows),
            len(alert_candidates),
        )

    def _select_symbols(self, ticker_rows: list[dict[str, str]]) -> list[SymbolTicker]:
        """Filter tickers down to tradable USDT perpetual contracts."""

        include_set = {symbol.upper() for symbol in self.config.scanning.symbols}
        exclude_set = {symbol.upper() for symbol in self.config.scanning.exclude_symbols}
        selected: list[SymbolTicker] = []

        # Non-crypto symbols: commodities (gold/silver) and equity/index contracts.
        non_crypto_symbols = {
            "XAUUSDT",   # Gold
            "XAGUSDT",   # Silver
            "PAXGUSDT",  # Paxos Gold
            "XAUTUSDT",  # Tether Gold
            "BTCDOMUSDT",  # BTC Dominance Index
        }

        for item in ticker_rows:
            symbol = str(item.get("symbol", "")).upper()
            if not symbol.endswith("USDT") or "_" in symbol:
                continue
            if include_set and symbol not in include_set:
                continue
            if symbol in exclude_set:
                continue
            if symbol in non_crypto_symbols:
                continue

            quote_volume = float(item.get("quoteVolume", 0.0))
            if quote_volume < self.config.scanning.min_24h_volume_usdt:
                continue

            selected.append(
                SymbolTicker(
                    symbol=symbol,
                    last_price=float(item.get("lastPrice", 0.0)),
                    price_change_percent=float(item.get("priceChangePercent", 0.0)),
                    quote_volume=quote_volume,
                )
            )

        # Prioritize higher-liquidity contracts when the symbol cap is reached.
        selected.sort(key=lambda item: item.quote_volume, reverse=True)
        return selected[: self.config.scanning.max_symbols]

    def _build_metrics(
        self,
        *,
        ticker: SymbolTicker,
        current_price: float,
        klines: list[Kline],
        open_interest: float,
        previous_open_interest: float | None,
        funding_rate: float,
        funding_rate_change: float = 0.0,
    ) -> ScanMetrics:
        """Convert raw ticker/kline/open-interest data into scoring metrics."""

        if len(klines) < 2:
            raise ValueError(f"not enough kline data for {ticker.symbol}")

        latest = klines[-1]
        previous_30m_volumes = [kline.quote_volume for kline in klines[-7:-1]]
        if not previous_30m_volumes:
            previous_30m_volumes = [kline.quote_volume for kline in klines[:-1]]

        open_5m = klines[-1].open
        open_15m = klines[-3].open if len(klines) >= 3 else klines[0].open
        open_1h = klines[-12].open if len(klines) >= 12 else klines[0].open

        recent_candle_changes = [
            _pct_change(kline.close, kline.open) for kline in reversed(klines[-3:])
        ]

        return ScanMetrics(
            symbol=ticker.symbol,
            price=current_price,
            change_5m=_pct_change(current_price, open_5m),
            change_15m=_pct_change(current_price, open_15m),
            change_1h=_pct_change(current_price, open_1h),
            price_change_24h=ticker.price_change_percent,
            current_5m_volume=latest.quote_volume,
            previous_30m_volumes=previous_30m_volumes,
            current_open_interest=open_interest,
            previous_open_interest=previous_open_interest,
            funding_rate=funding_rate,
            funding_rate_change=funding_rate_change,
            quote_volume_24h=ticker.quote_volume,
            consecutive_direction=classify_direction(recent_candle_changes),
        )

    async def _emit_alerts(
        self, alert_candidates: list[tuple[AlertPayload, str]], timestamp: datetime
    ) -> None:
        """Send Telegram alerts and store the final alert log."""

        for payload, level in alert_candidates:
            message = self.telegram.build_message(payload)
            try:
                await self.telegram.send_alert(payload)
            except TelegramDeliveryError as exc:
                self.logger.error("Telegram delivery failed for %s: %s", payload.symbol, exc)

            await self.store.record_alert(
                AlertRow(
                    symbol=payload.symbol,
                    score=payload.score,
                    alert_level=level,
                    message=message,
                    timestamp=timestamp,
                )
            )

    async def _maybe_cleanup(self, now: datetime) -> None:
        """Prune old rows once per hour to keep SQLite size bounded."""

        if self._last_cleanup_at and (now - self._last_cleanup_at) < timedelta(hours=1):
            return

        if self._cleanup_task is not None and not self._cleanup_task.done():
            self.logger.warning("Previous DB cleanup task is still running. Skipping this cycle.")
            return

        self._cleanup_task = asyncio.create_task(
            self._run_cleanup_background(now),
            name="db-retention-cleanup",
        )
        self._last_cleanup_at = now

    async def _run_cleanup_background(self, now: datetime) -> None:
        try:
            deleted = await self.store.prune_old_records(now)
            self.logger.info("Retention cleanup summary: %s", deleted)
        except Exception as exc:
            self.logger.error("Retention cleanup failed in background: %s", exc, exc_info=True)
