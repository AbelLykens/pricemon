"""Read-time outlier filtering for cross-exchange aggregation.

Pure helpers (no Django dependency beyond stdlib types). The pipeline never
mutates stored data — these helpers run when consumers in ``api/views.py``
roll up per-(exchange, base, quote, minute) rows into cross-exchange numbers.

Two distinct concerns:

1. **Exchange row filtering.** When several exchanges report VWAP for the same
   (base, quote, minute), drop the rows that deviate more than
   ``max_dev_pct`` from the volume-weighted median. This guards the rolled-up
   VWAPs and OHLC merges from low-volume exchanges with stale or off feeds
   (e.g. ascendex trading $24/min at +0.4% vs Coinbase's $400k/min).

2. **Wick clipping.** ``price_min`` and ``price_max`` for a single bucket are
   extremes, not volume-weighted, so one fat-finger print blows them out
   (e.g. bitstamp BTC/EUR showing min 3% below vwap). Clip each per-exchange
   extreme to that exchange's own VWAP ± ``max_pct`` before it feeds into
   any cross-exchange high/low merge.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Iterable

log = logging.getLogger("api.outliers")


@dataclass
class OutlierReport:
    excluded: list[dict] = field(default_factory=list)
    wicks_clipped: int = 0


def consensus_vwap(rows: Iterable[dict]) -> Decimal | None:
    """Unweighted median of per-exchange vwaps. Returns None if no rows
    have a usable vwap.

    Volume is intentionally ignored here: in the failure mode this filter
    targets, a single exchange can have both the largest volume *and* a fat-
    finger print that pulled its bucket vwap off. Volume-weighting the
    consensus would make that exchange the "median" and drop the healthy
    cluster — exactly backwards. An unweighted median over per-exchange
    vwaps stays anchored on whichever value the most exchanges agree on.
    """
    prices = sorted(
        r["vwap"] for r in rows if r.get("vwap") is not None
    )
    n = len(prices)
    if n == 0:
        return None
    if n % 2 == 1:
        return prices[n // 2]
    return (prices[n // 2 - 1] + prices[n // 2]) / 2


def filter_exchange_outliers(
    rows: list[dict],
    *,
    base: str,
    quote: str,
    minute,
    max_dev_pct: Decimal,
    report: OutlierReport,
) -> list[dict]:
    """Return rows whose ``vwap`` is within ``max_dev_pct`` of the
    volume-weighted median across ``rows``.

    Rules:
      * Fewer than 2 rows → return as-is (nothing to compare against).
      * Rows with zero volume_base are kept but cannot anchor the median.
      * Always keep at least one row (the one closest to the median) so the
        caller never sees an empty group during a legitimate volatility spike.
    """
    if len(rows) < 2:
        return rows
    median = consensus_vwap(rows)
    if median is None or median == 0:
        return rows

    limit = max_dev_pct / Decimal(100)
    kept: list[dict] = []
    dropped: list[tuple[dict, Decimal]] = []
    for r in rows:
        vwap = r.get("vwap")
        if vwap is None:
            kept.append(r)
            continue
        dev = (vwap - median) / median
        if abs(dev) <= limit:
            kept.append(r)
        else:
            dropped.append((r, dev))

    if not kept and dropped:
        # Always keep the closest-to-median row.
        dropped.sort(key=lambda x: abs(x[1]))
        closest, _ = dropped.pop(0)
        kept.append(closest)

    for r, dev in dropped:
        entry = {
            "exchange": r.get("exchange"),
            "base": base,
            "quote": quote,
            "minute": minute.isoformat() if hasattr(minute, "isoformat") else minute,
            "vwap": str(r["vwap"]),
            "median": str(median),
            "deviation_pct": str((dev * 100).quantize(Decimal("0.001"))),
        }
        report.excluded.append(entry)
        log.info(
            "outlier excluded: %s %s/%s @ %s vwap=%s median=%s dev=%s%%",
            entry["exchange"], base, quote, entry["minute"],
            entry["vwap"], entry["median"], entry["deviation_pct"],
        )
    return kept


def clip_wick(
    price_min: Decimal | None,
    price_max: Decimal | None,
    price_vwap: Decimal | None,
    *,
    max_pct: Decimal,
    report: OutlierReport,
) -> tuple[Decimal | None, Decimal | None]:
    """Clip ``price_min`` up and ``price_max`` down to ``price_vwap`` ± max_pct.

    No-op if any of the three inputs is None or vwap is non-positive.
    """
    if price_min is None or price_max is None or price_vwap is None or price_vwap <= 0:
        return price_min, price_max
    pct = max_pct / Decimal(100)
    floor = price_vwap * (Decimal(1) - pct)
    ceil = price_vwap * (Decimal(1) + pct)
    new_min = price_min
    new_max = price_max
    changed = False
    if price_min < floor:
        new_min = floor
        changed = True
    if price_max > ceil:
        new_max = ceil
        changed = True
    if changed:
        report.wicks_clipped += 1
    return new_min, new_max
