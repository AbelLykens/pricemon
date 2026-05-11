"""In-memory minute aggregation of trades.

One `MinuteAggregator` per daemon process. It keeps `(pair_id, minute_start)`
buckets, publishes the in-progress bucket per pair to memcached on every
trade (throttled), and periodically flushes finished buckets to PostgreSQL.

Buckets are considered finished `LATE_TRADE_GRACE_SEC` seconds after their
minute window has ended, to absorb late-arriving exchange trades.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from asgiref.sync import sync_to_async
from django.conf import settings
from django.db import transaction

from core.models import MinuteAggregate
from feeds.current_state import PairMeta, build_payload, publish

log = logging.getLogger(__name__)

LATE_TRADE_GRACE_SEC = 30


def _minute_floor(ts: datetime) -> datetime:
    return ts.replace(second=0, microsecond=0)


@dataclass
class _Bucket:
    pair_id: int
    minute_start: datetime
    count: int = 0
    volume_base: Decimal = field(default_factory=lambda: Decimal(0))
    volume_quote: Decimal = field(default_factory=lambda: Decimal(0))
    sum_price: Decimal = field(default_factory=lambda: Decimal(0))
    price_min: Decimal | None = None
    price_max: Decimal | None = None

    def add(self, price: Decimal, amount: Decimal) -> None:
        amt = amount if amount >= 0 else -amount
        self.count += 1
        self.volume_base += amt
        self.volume_quote += price * amt
        self.sum_price += price
        if self.price_min is None or price < self.price_min:
            self.price_min = price
        if self.price_max is None or price > self.price_max:
            self.price_max = price

    def to_kwargs(self) -> dict:
        avg = self.sum_price / self.count
        vwap = (self.volume_quote / self.volume_base) if self.volume_base else avg
        return {
            "trade_count": self.count,
            "volume_base": self.volume_base,
            "volume_quote": self.volume_quote,
            "price_min": self.price_min,
            "price_max": self.price_max,
            "price_avg": avg,
            "price_vwap": vwap,
        }


class MinuteAggregator:
    def __init__(self, pair_meta: dict[int, PairMeta]):
        self._pair_meta = pair_meta
        self._symbol_to_id = {m.symbol: m.pair_id for m in pair_meta.values()}
        self._buckets: dict[tuple[int, datetime], _Bucket] = {}
        self._lock = asyncio.Lock()
        self._last_trade_at: datetime | None = None
        self._total_trades = 0
        self._latest_minute: dict[int, datetime] = {}
        self._last_publish: dict[int, float] = {}
        self._publish_interval = float(
            getattr(settings, "CURRENT_PUBLISH_MIN_INTERVAL_SEC", 0.5)
        )

    @property
    def last_trade_at(self) -> datetime | None:
        return self._last_trade_at

    @property
    def total_trades(self) -> int:
        return self._total_trades

    @property
    def open_buckets(self) -> int:
        return len(self._buckets)

    async def add_trade(
        self, symbol: str, price: Decimal, amount: Decimal, ts: float
    ) -> None:
        pair_id = self._symbol_to_id.get(symbol)
        if pair_id is None:
            log.debug("dropping trade for unsubscribed symbol %s", symbol)
            return
        trade_dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        minute = _minute_floor(trade_dt)
        key = (pair_id, minute)
        publish_payload: dict | None = None
        async with self._lock:
            bucket = self._buckets.get(key)
            if bucket is None:
                bucket = _Bucket(pair_id=pair_id, minute_start=minute)
                self._buckets[key] = bucket
            bucket.add(price, amount)
            self._last_trade_at = datetime.now(tz=timezone.utc)
            self._total_trades += 1

            latest = self._latest_minute.get(pair_id)
            if latest is None or minute >= latest:
                self._latest_minute[pair_id] = minute
                publish_payload = self._maybe_publish_payload(
                    pair_id, bucket, price, ts
                )

        if publish_payload is not None:
            try:
                publish(publish_payload)
            except Exception:
                log.exception("current-state publish failed for pair %s", pair_id)

    def _maybe_publish_payload(
        self, pair_id: int, bucket: _Bucket, last_price: Decimal, last_trade_ts: float
    ) -> dict | None:
        now_ts = time.time()
        last = self._last_publish.get(pair_id, 0.0)
        if now_ts - last < self._publish_interval:
            return None
        meta = self._pair_meta.get(pair_id)
        if meta is None:
            return None
        self._last_publish[pair_id] = now_ts
        return build_payload(
            meta=meta,
            last_price=last_price,
            last_trade_ts=last_trade_ts,
            now_ts=now_ts,
            minute_start=bucket.minute_start,
            count=bucket.count,
            volume_base=bucket.volume_base,
            volume_quote=bucket.volume_quote,
            price_min=bucket.price_min if bucket.price_min is not None else last_price,
            price_max=bucket.price_max if bucket.price_max is not None else last_price,
        )

    async def flush_completed(self) -> int:
        now = datetime.now(tz=timezone.utc)
        # A bucket [m, m+60s) is complete once now >= m + 60 + grace.
        complete_cutoff = now - timedelta(seconds=60 + LATE_TRADE_GRACE_SEC)
        async with self._lock:
            ready_keys = [
                k for k, b in self._buckets.items() if b.minute_start <= complete_cutoff
            ]
            ready = [self._buckets.pop(k) for k in ready_keys]
        if ready:
            await sync_to_async(_persist_buckets, thread_sensitive=False)(ready)
        return len(ready)

    async def flush_all(self) -> int:
        async with self._lock:
            ready = list(self._buckets.values())
            self._buckets.clear()
        if ready:
            await sync_to_async(_persist_buckets, thread_sensitive=False)(ready)
        return len(ready)


def _persist_buckets(buckets: list[_Bucket]) -> None:
    with transaction.atomic():
        for b in buckets:
            MinuteAggregate.objects.update_or_create(
                pair_id=b.pair_id,
                minute_start=b.minute_start,
                defaults=b.to_kwargs(),
            )
