"""Per-exchange daemon: subscribe to trade feeds and aggregate per-minute.

One daemon process per exchange. The daemon embeds a cryptofeed
``FeedHandler`` plus a ``MinuteAggregator`` and a periodic flush task.

Stall handling has two layers:

1. cryptofeed's own per-connection ``timeout`` watcher closes a stalled
   socket and reconnects with exponential backoff.
2. We notify systemd ``WATCHDOG=1`` periodically. If we stop notifying
   (event loop wedged, async deadlock), systemd kills+restarts via the
   unit's ``WatchdogSec`` setting.
"""

from __future__ import annotations

import asyncio
import logging
import signal

import sdnotify
from cryptofeed import FeedHandler
from cryptofeed.defines import TRADES
from django.conf import settings

from core.models import Exchange, TradingPair
from feeds.aggregator import MinuteAggregator
from feeds.current_state import PairMeta
from feeds.exchanges import SUPPORTED_SLUGS, feed_class

log = logging.getLogger(__name__)


class FeedDaemon:
    def __init__(self, exchange_slug: str):
        if exchange_slug not in SUPPORTED_SLUGS:
            raise ValueError(f"Unsupported exchange slug: {exchange_slug}")
        self.exchange_slug = exchange_slug
        self.exchange = Exchange.objects.get(slug=exchange_slug, is_active=True)
        self.feed_class = feed_class(exchange_slug)

        pairs = list(
            TradingPair.objects.filter(exchange=self.exchange, is_active=True)
            .select_related("base", "quote")
        )
        if not pairs:
            raise RuntimeError(
                f"No active trading pairs configured for {exchange_slug}"
            )
        self.symbols = [p.cryptofeed_symbol for p in pairs]
        self.pair_meta = {
            p.pk: PairMeta(
                pair_id=p.pk,
                exchange_slug=self.exchange.slug,
                base=p.base.code,
                quote=p.quote.code,
                symbol=p.cryptofeed_symbol,
            )
            for p in pairs
        }
        self.aggregator = MinuteAggregator(self.pair_meta)
        self.notifier = sdnotify.SystemdNotifier()
        self.watchdog_timeout = settings.FEED_WATCHDOG_TIMEOUT_SEC
        self.retries = settings.FEED_RECONNECT_RETRIES
        self._stop = asyncio.Event()
        self._fh: FeedHandler | None = None

    async def _on_trade(self, t, receipt_timestamp: float) -> None:
        try:
            await self.aggregator.add_trade(
                symbol=t.symbol,
                price=t.price,
                amount=t.amount,
                ts=t.timestamp or receipt_timestamp,
            )
        except Exception:
            log.exception("failed to add trade %s", t)

    async def _flush_loop(self) -> None:
        while not self._stop.is_set():
            try:
                flushed = await self.aggregator.flush_completed()
                if flushed:
                    log.info("flushed %d minute aggregates", flushed)
            except Exception:
                log.exception("flush loop error")
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=5)
            except asyncio.TimeoutError:
                pass

    async def _watchdog_loop(self) -> None:
        interval = max(5, self.watchdog_timeout // 2)
        while not self._stop.is_set():
            try:
                self.notifier.notify("WATCHDOG=1")
                self.notifier.notify(
                    f"STATUS=trades={self.aggregator.total_trades} "
                    f"open_buckets={self.aggregator.open_buckets}"
                )
            except Exception:
                log.exception("sdnotify failed")
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=interval)
            except asyncio.TimeoutError:
                pass

    def _request_stop(self) -> None:
        # Called from a signal handler in the running loop. Just stop the
        # loop — graceful FeedHandler.stop() needs run_until_complete and
        # therefore must run *after* run_forever() returns.
        log.info("stop requested")
        self._stop.set()
        try:
            loop = asyncio.get_event_loop()
            loop.call_soon_threadsafe(loop.stop)
        except Exception:
            log.exception("loop stop failed")

    def run(self) -> None:
        log.info(
            "starting %s daemon, %d symbols: %s",
            self.exchange_slug, len(self.symbols), self.symbols,
        )
        self._fh = FeedHandler()
        self._fh.add_feed(
            self.feed_class(
                symbols=self.symbols,
                channels=[TRADES],
                callbacks={TRADES: self._on_trade},
                timeout=self.watchdog_timeout,
                retries=self.retries,
            )
        )

        # Schedule cryptofeed tasks on a fresh loop without blocking.
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            self._fh.run(start_loop=False, install_signal_handlers=False)
        except TypeError:
            # Older cryptofeed signatures: fall back to default and let it
            # install its own handlers (we still wrap with our own below).
            self._fh.run(start_loop=False)

        loop.create_task(self._flush_loop())
        loop.create_task(self._watchdog_loop())

        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, self._request_stop)
            except (NotImplementedError, RuntimeError):
                signal.signal(sig, lambda *_: self._request_stop())

        self.notifier.notify("READY=1")
        self.notifier.notify(
            f"STATUS=Subscribed to {len(self.symbols)} symbols on {self.exchange_slug}"
        )

        try:
            loop.run_forever()
        finally:
            self.notifier.notify("STOPPING=1")
            try:
                if self._fh is not None:
                    self._fh.stop(loop=loop)
            except Exception:
                log.exception("FeedHandler.stop failed")
            try:
                loop.run_until_complete(self.aggregator.flush_all())
            except Exception:
                log.exception("final flush failed")
            try:
                pending = [t for t in asyncio.all_tasks(loop) if not t.done()]
                for t in pending:
                    t.cancel()
                if pending:
                    loop.run_until_complete(
                        asyncio.gather(*pending, return_exceptions=True)
                    )
            except Exception:
                log.exception("pending task cancellation failed")
            try:
                loop.close()
            except Exception:
                log.exception("loop close failed")
            log.info("daemon exited")
