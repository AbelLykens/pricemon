"""Cross-daemon live price state, backed by memcached.

Each daemon publishes the in-progress minute bucket (and last trade) for
every pair it owns. The web layer reads these entries to render a current
price without waiting for the per-minute database flush.

One TradingPair belongs to exactly one daemon, so writers never contend
on the same key. Entries expire via the Django cache TIMEOUT, so a dead
daemon's pairs disappear automatically.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Iterable

from django.core.cache import cache


def _key(pair_id: int) -> str:
    return f"cur:p:{pair_id}"


@dataclass(frozen=True)
class PairMeta:
    pair_id: int
    exchange_slug: str
    base: str
    quote: str
    symbol: str


def build_payload(
    *,
    meta: PairMeta,
    last_price: Decimal,
    last_trade_ts: float,
    now_ts: float,
    minute_start: datetime,
    count: int,
    volume_base: Decimal,
    volume_quote: Decimal,
    price_min: Decimal,
    price_max: Decimal,
) -> dict:
    vwap = (volume_quote / volume_base) if volume_base else last_price
    return {
        "pair_id": meta.pair_id,
        "exchange": meta.exchange_slug,
        "base": meta.base,
        "quote": meta.quote,
        "symbol": meta.symbol,
        "last_price": last_price,
        "last_trade_ts": last_trade_ts,
        "last_update_ts": now_ts,
        "minute_start": minute_start,
        "count": count,
        "volume_base": volume_base,
        "volume_quote": volume_quote,
        "price_min": price_min,
        "price_max": price_max,
        "vwap_minute": vwap,
    }


def publish(payload: dict) -> None:
    cache.set(_key(payload["pair_id"]), payload)


def get_many(pair_ids: Iterable[int]) -> dict[int, dict]:
    ids = list(pair_ids)
    if not ids:
        return {}
    raw = cache.get_many([_key(pid) for pid in ids])
    return {pid: raw[_key(pid)] for pid in ids if _key(pid) in raw}
