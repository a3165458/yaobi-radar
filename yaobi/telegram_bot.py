"""Telegram alert delivery for Yaobi Radar."""

from __future__ import annotations

import asyncio
import html
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import aiohttp

from .config import TelegramConfig


class TelegramDeliveryError(RuntimeError):
    """Raised when Telegram notification delivery fails after retries."""


@dataclass(slots=True)
class AlertPayload:
    """Data needed to build a Telegram alert message."""

    symbol: str
    score: int
    level: str
    change_5m: float
    volume_ratio: float
    oi_change_pct: float
    funding_rate: float
    price: float
    volume_24h: float
    change_24h: float
    timestamp: datetime


def _format_usdt(value: float) -> str:
    """Format notional values with readable market-style suffixes."""

    absolute = abs(value)
    if absolute >= 1_000_000_000:
        return f"{value / 1_000_000_000:.2f}B USDT"
    if absolute >= 1_000_000:
        return f"{value / 1_000_000:.2f}M USDT"
    if absolute >= 1_000:
        return f"{value / 1_000:.2f}K USDT"
    return f"{value:.2f} USDT"


def _format_price(value: float) -> str:
    """Render a futures price with useful precision for both majors and micro caps."""

    if value >= 1000:
        return f"{value:,.2f}"
    if value >= 1:
        return f"{value:,.4f}"
    return f"{value:.8f}".rstrip("0").rstrip(".")


class TelegramBot:
    """Async wrapper around Telegram's sendMessage API."""

    def __init__(self, config: TelegramConfig, logger: logging.Logger | None = None) -> None:
        self.config = config
        self.logger = logger or logging.getLogger(__name__)
        self._session: aiohttp.ClientSession | None = None
        self._warned_missing_credentials = False

    @property
    def is_configured(self) -> bool:
        """Return whether Telegram notifications can be delivered."""

        return bool(self.config.enabled and self.config.bot_token and self.config.chat_id)

    async def start(self) -> None:
        """Open the shared aiohttp session."""

        if self._session is not None:
            return
        self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15))

        if self.config.enabled and not self.is_configured and not self._warned_missing_credentials:
            self.logger.warning(
                "Telegram is enabled but bot_token/chat_id is missing. Alerts will only be stored in SQLite."
            )
            self._warned_missing_credentials = True

    async def close(self) -> None:
        """Close the shared aiohttp session."""

        if self._session is not None:
            await self._session.close()
            self._session = None

    def build_message(self, payload: AlertPayload) -> str:
        """Create the HTML Telegram message matching the requested format."""

        emoji = "🚨" if payload.level == "critical" else "⚠️"
        timestamp_local = payload.timestamp.astimezone(
            timezone(timedelta(hours=8))
        ).strftime("%Y-%m-%d %H:%M:%S")
        symbol = html.escape(payload.symbol)

        return (
            f"{emoji} 妖币出没 | {symbol} | 妖币分: {payload.score}\n\n"
            f"📈 5分钟涨跌: {payload.change_5m:+.2f}%\n"
            f"📊 成交量突增: {payload.volume_ratio:.1f}x (近30分钟均值)\n"
            f"🏛️ 持仓量变化: {payload.oi_change_pct:+.2f}%\n"
            f"💰 资金费率: {payload.funding_rate:+.4f}%\n"
            f"💵 当前价格: {html.escape(_format_price(payload.price))}\n"
            f"📏 24h成交额: {html.escape(_format_usdt(payload.volume_24h))}\n"
            f"📉 24h涨跌: {payload.change_24h:+.2f}%\n\n"
            f"⏰ {timestamp_local} (UTC+8)\n"
            f'🔗 <a href="https://www.binance.com/en/futures/{symbol}">Binance 合约</a>'
        )

    async def send_alert(self, payload: AlertPayload) -> bool:
        """Send a Telegram alert when the bot is configured."""

        if not self.config.enabled:
            return False
        if not self.is_configured:
            if not self._warned_missing_credentials:
                self.logger.warning(
                    "Telegram credentials missing. Alert skipped for %s.", payload.symbol
                )
                self._warned_missing_credentials = True
            return False
        if self._session is None:
            raise RuntimeError("TelegramBot.start() must be called before sending alerts")

        message = self.build_message(payload)
        request_body: dict[str, str | int] = {
            "chat_id": self.config.chat_id,
            "text": message,
            "parse_mode": "HTML",
            "disable_web_page_preview": "true",
        }
        if self.config.thread_id is not None:
            request_body["message_thread_id"] = self.config.thread_id

        endpoint = (
            f"https://api.telegram.org/bot{self.config.bot_token}/sendMessage"
        )

        last_error: Exception | None = None
        for attempt in range(1, 4):
            try:
                async with self._session.post(endpoint, json=request_body) as response:
                    if response.status >= 500:
                        text = await response.text()
                        self.logger.warning(
                            "Telegram server error on attempt %s/3 for %s: %s",
                            attempt,
                            payload.symbol,
                            text[:200],
                        )
                        await asyncio.sleep(attempt)
                        continue

                    response.raise_for_status()
                    data = await response.json()
                    if not data.get("ok", False):
                        raise TelegramDeliveryError(str(data))
                    return True
            except (aiohttp.ClientError, asyncio.TimeoutError, TelegramDeliveryError) as exc:
                last_error = exc
                self.logger.warning(
                    "Telegram delivery failed on attempt %s/3 for %s: %s",
                    attempt,
                    payload.symbol,
                    exc,
                )
                await asyncio.sleep(attempt)

        raise TelegramDeliveryError(
            f"failed to send alert for {payload.symbol}: {last_error}"
        )
