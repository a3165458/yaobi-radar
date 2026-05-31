"""Yaobi Radar entrypoint.

PM2 example:
    pm2 start main.py --interpreter python3 --name yaobi-radar
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal
from logging.handlers import RotatingFileHandler
from pathlib import Path

from yaobi.binance_client import BinanceClient
from yaobi.config import AppConfig
from yaobi.data_store import DataStore
from yaobi.scanner import Scanner
from yaobi.telegram_bot import TelegramBot
from yaobi.websocket_monitor import WebSocketMonitor


def setup_logging(level: str, log_file: Path) -> None:
    """Configure structured logging to both console and rotating file."""

    log_file.parent.mkdir(parents=True, exist_ok=True)
    root_logger = logging.getLogger()
    root_logger.setLevel(level.upper())
    root_logger.handlers.clear()

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=5 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)

    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)


async def run_service(config: AppConfig) -> None:
    """Bootstrap all components and keep the service alive until shutdown."""

    setup_logging(config.logging.level, config.log_file_path)
    logger = logging.getLogger("yaobi-radar")
    stop_event = asyncio.Event()

    store = DataStore(config.database_path, config.database.retention_days)
    client = BinanceClient(config, logger=logging.getLogger("yaobi.binance"))
    telegram = TelegramBot(config.telegram, logger=logging.getLogger("yaobi.telegram"))
    websocket_monitor = WebSocketMonitor(
        config, logger=logging.getLogger("yaobi.websocket")
    )
    scanner = Scanner(
        config=config,
        client=client,
        store=store,
        telegram=telegram,
        websocket_monitor=websocket_monitor if config.websocket.enabled else None,
        logger=logging.getLogger("yaobi.scanner"),
    )

    def _request_shutdown() -> None:
        logger.info("Shutdown signal received. Stopping Yaobi Radar...")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for signame in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(signame, _request_shutdown)

    await store.connect()
    await client.start()
    await telegram.start()

    websocket_task: asyncio.Task[None] | None = None
    if config.websocket.enabled:
        websocket_task = asyncio.create_task(
            websocket_monitor.run(stop_event),
            name="binance-websocket-monitor",
        )

    try:
        await scanner.run(stop_event)
    finally:
        stop_event.set()
        if websocket_task is not None:
            await websocket_task
        await telegram.close()
        await client.close()
        await store.close()
        logger.info("Yaobi Radar stopped cleanly.")


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""

    parser = argparse.ArgumentParser(description="Yaobi Radar futures market scanner")
    parser.add_argument(
        "--config",
        default="config.json",
        help="Path to the JSON configuration file",
    )
    return parser.parse_args()


def main() -> None:
    """Load configuration and start the async runtime."""

    args = parse_args()
    config = AppConfig.load(args.config)
    asyncio.run(run_service(config))


if __name__ == "__main__":
    main()
