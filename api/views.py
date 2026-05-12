import time
from collections import defaultdict
from datetime import datetime, timedelta
from decimal import Decimal

from django.conf import settings
from django.db.models import Max, Min, Sum
from django.shortcuts import render
from django.utils import timezone
from django.utils.decorators import method_decorator
from django.views import View
from django.views.decorators.cache import cache_page
from django.views.decorators.vary import vary_on_headers
from drf_spectacular.types import OpenApiTypes
from drf_spectacular.utils import OpenApiParameter, extend_schema
from rest_framework.response import Response
from rest_framework.views import APIView

from api.outliers import OutlierReport, clip_wick, filter_exchange_outliers
from core.models import Currency, Exchange, HistoricalBtcPrice, MinuteAggregate, TradingPair
from feeds.current_state import get_many as get_current_many


def _vwap(volume_quote: Decimal, volume_base: Decimal) -> str | None:
    if not volume_base:
        return None
    return str(volume_quote / volume_base)


def _vwap_dec(volume_quote: Decimal | None, volume_base: Decimal | None) -> Decimal | None:
    if not volume_base:
        return None
    return (volume_quote or Decimal(0)) / volume_base


def _stable_peg_map() -> dict[str, str]:
    """Mapping of stablecoin code → peg-target fiat code (e.g. USDT → USD)."""
    return dict(
        Currency.objects.filter(peg_to__isnull=False)
        .values_list("code", "peg_to__code")
    )


def _stable_rates_from_window(qs, peg_map: dict[str, str]) -> dict[str, dict]:
    """Cross-exchange VWAP per stablecoin against its peg fiat over ``qs``'s window.

    Only the stable's peg-target fiat counts (USDT-USD for USDT, not USDT-EUR),
    so we get one canonical rate per stablecoin.
    """
    if not peg_map:
        return {}
    rows = (
        qs.filter(pair__base__code__in=list(peg_map.keys()))
        .values("pair__base__code", "pair__quote__code")
        .annotate(
            volume_base=Sum("volume_base"),
            volume_quote=Sum("volume_quote"),
            trade_count=Sum("trade_count"),
        )
    )
    out: dict[str, dict] = {}
    for r in rows:
        stable = r["pair__base__code"]
        fiat = r["pair__quote__code"]
        if peg_map.get(stable) != fiat:
            continue
        vb = r["volume_base"] or Decimal(0)
        if not vb:
            continue
        vq = r["volume_quote"] or Decimal(0)
        out[stable] = {
            "fiat": fiat,
            "rate": vq / vb,
            "volume_base": vb,
            "volume_quote": vq,
            "trade_count": r["trade_count"] or 0,
        }
    return out


def _stable_rates_from_live(current_rows, peg_map: dict[str, str]) -> dict[str, dict]:
    """Cross-exchange live rate per stablecoin, from current-state cache rows.

    Uses the in-progress minute's volume where available; falls back to
    volume-1-weighted last_price for pairs whose minute just rolled.
    """
    accum: dict[str, dict] = {}
    for r in current_rows:
        if not r.get("live") or not r.get("fresh"):
            continue
        base = r["base"]
        quote = r["quote"]
        if peg_map.get(base) != quote:
            continue
        slot = accum.setdefault(base, {
            "fiat": quote,
            "volume_base": Decimal(0),
            "volume_quote": Decimal(0),
            "sum_wp": Decimal(0),
            "sum_w": Decimal(0),
            "exchanges": 0,
        })
        slot["volume_base"] += r["minute_volume_base"]
        slot["volume_quote"] += r["minute_volume_quote"]
        weight = r["minute_volume_base"] if r["minute_volume_base"] > 0 else Decimal(1)
        slot["sum_wp"] += r["last_price"] * weight
        slot["sum_w"] += weight
        slot["exchanges"] += 1
    out: dict[str, dict] = {}
    for stable, slot in accum.items():
        if slot["volume_base"] > 0:
            rate = slot["volume_quote"] / slot["volume_base"]
        elif slot["sum_w"] > 0:
            rate = slot["sum_wp"] / slot["sum_w"]
        else:
            continue
        out[stable] = {
            "fiat": slot["fiat"],
            "rate": rate,
            "exchanges": slot["exchanges"],
            "volume_base": slot["volume_base"],
        }
    return out


def _merge_into_fiat(
    per_quote_rows,
    rates: dict[str, dict],
    peg_map: dict[str, str],
    fiats: tuple[str, ...] = ("USD", "EUR"),
) -> dict[tuple[str, str], dict]:
    """Bucket per-(base, quote) aggregates into per-(base, fiat) merged aggregates.

    Direct fiat rows pass through with rate=1. Stable-quoted rows are
    multiplied by their stable→fiat rate (in ``rates``) and contributed to
    the underlying fiat bucket. Stables with no rate in this window are
    dropped (and surfaced as "missing_rates" by the caller).
    """
    bucket: dict[tuple[str, str], dict] = {}
    for row in per_quote_rows:
        base = row["base"]
        quote = row["quote"]
        vb = row["volume_base"] or Decimal(0)
        vq = row["volume_quote"] or Decimal(0)
        tc = row["trade_count"] or 0

        target_fiat = None
        rate = None
        if quote in fiats:
            target_fiat = quote
            rate = Decimal(1)
        elif quote in peg_map and peg_map[quote] in fiats and quote in rates:
            target_fiat = peg_map[quote]
            rate = rates[quote]["rate"]
        if target_fiat is None:
            continue

        key = (base, target_fiat)
        b = bucket.setdefault(key, {
            "base": base,
            "fiat": target_fiat,
            "volume_base": Decimal(0),
            "volume_quote_fiat": Decimal(0),
            "trade_count": 0,
            "components": [],
        })
        contribution = vq * rate
        b["volume_base"] += vb
        b["volume_quote_fiat"] += contribution
        b["trade_count"] += tc
        b["components"].append({
            "source_quote": quote,
            "rate": rate,
            "volume_base": vb,
            "volume_quote_source": vq,
            "contribution_fiat": contribution,
            "trade_count": tc,
        })
    return bucket


def _missing_rate_stables(per_quote_rows, rates, peg_map, fiats=("USD", "EUR")) -> list[str]:
    """Stable codes that had trades in this window but no peg-fiat conversion rate."""
    have_trades = {
        row["quote"] for row in per_quote_rows
        if row["quote"] in peg_map and peg_map[row["quote"]] in fiats
        and (row["volume_base"] or 0)
    }
    return sorted(have_trades - set(rates.keys()))


@method_decorator(cache_page(settings.API_CACHE_TTL_PRICES), name="dispatch")
@method_decorator(vary_on_headers("Accept"), name="dispatch")
class WeightedPricesView(APIView):
    """Volume-weighted prices per (base, quote) over a recent window.

    Aggregates ``MinuteAggregate`` rows across the configured exchanges and
    returns:

      * ``weighted``: raw per-(base, quote) VWAP across exchanges.
      * ``weighted_fiat``: per-(base, fiat) VWAP that merges stable-quoted
        volume (USDT, USDC) into the underlying fiat using live conversion
        rates. This is the trustworthy "USD" answer.
      * ``stable_rates``: the stable→fiat conversion rates used.
      * ``by_exchange``: per-exchange × (base, quote) breakdown.

    Query params (all optional):
        quote     repeatable, e.g. ``?quote=USD&quote=EUR``. Default: all quote currencies.
        base      repeatable, e.g. ``?base=BTC``. Default: all bases.
        exchange  repeatable, exchange slug filter. Default: all active exchanges.
        window    integer minutes back to consider, default 5, max 1440.
    """

    FIATS = ("USD", "EUR")

    @extend_schema(
        tags=["prices"],
        summary="Window-based volume-weighted prices",
        parameters=[
            OpenApiParameter("window", OpenApiTypes.INT, description="Minutes back to aggregate. Default 5, clamped to [1, 1440]."),
            OpenApiParameter("base", OpenApiTypes.STR, many=True, description="Repeatable. Filter by base currency (e.g. BTC). Default: all."),
            OpenApiParameter("quote", OpenApiTypes.STR, many=True, description="Repeatable. Filter by quote currency (e.g. USD). Default: all."),
            OpenApiParameter("exchange", OpenApiTypes.STR, many=True, description="Repeatable. Filter by exchange slug. Default: all active."),
        ],
        responses=OpenApiTypes.OBJECT,
    )
    def get(self, request):
        try:
            window = int(request.query_params.get("window", 5))
        except ValueError:
            return Response({"detail": "window must be an integer"}, status=400)
        window = max(1, min(window, 1440))

        bases = request.query_params.getlist("base")
        quotes = request.query_params.getlist("quote")
        exchanges = request.query_params.getlist("exchange")

        cutoff = timezone.now() - timedelta(minutes=window)
        qs = MinuteAggregate.objects.filter(minute_start__gte=cutoff)
        if bases:
            qs = qs.filter(pair__base__code__in=bases)
        if exchanges:
            qs = qs.filter(pair__exchange__slug__in=exchanges)
        else:
            qs = qs.filter(pair__exchange__is_active=True)

        peg_map = _stable_peg_map()
        merge_quotes = set(self.FIATS) | set(peg_map.keys())

        # Pull per-(exchange, base, quote, minute) raw rows so we can drop
        # outliers before rolling up. The unique key on MinuteAggregate is
        # (pair, minute_start), so each row here is already a single record.
        raw_rows = list(
            qs.values(
                "pair__exchange__slug",
                "pair__base__code",
                "pair__quote__code",
                "minute_start",
                "volume_base",
                "volume_quote",
                "trade_count",
                "price_vwap",
            )
        )

        outlier_enabled = getattr(settings, "OUTLIER_FILTER_ENABLED", False)
        report = OutlierReport()
        outlier_exchange_pairs: set[tuple[str, str, str]] = set()

        by_minute: dict[tuple[str, str, datetime], list[dict]] = defaultdict(list)
        for r in raw_rows:
            by_minute[(r["pair__base__code"], r["pair__quote__code"], r["minute_start"])].append(r)

        kept_rows: list[dict] = []
        for (base_, quote_, minute), rows in by_minute.items():
            if outlier_enabled and len(rows) >= 2:
                mapped = [
                    {
                        "exchange": r["pair__exchange__slug"],
                        "vwap": r["price_vwap"],
                        "volume_base": r["volume_base"] or Decimal(0),
                        "_raw": r,
                    }
                    for r in rows
                ]
                before = {m["exchange"] for m in mapped}
                kept = filter_exchange_outliers(
                    mapped,
                    base=base_, quote=quote_, minute=minute,
                    max_dev_pct=settings.OUTLIER_EXCHANGE_MAX_DEV_PCT,
                    report=report,
                )
                after = {m["exchange"] for m in kept}
                for ex in before - after:
                    outlier_exchange_pairs.add((ex, base_, quote_))
                kept_rows.extend(m["_raw"] for m in kept)
            else:
                kept_rows.extend(rows)

        # Roll up `weighted[]` (honors user quote filter) and `full_rolled`
        # (always covers merge_quotes for fiat merge) in a single pass.
        weighted_acc: dict[tuple[str, str], dict] = {}
        full_rolled_acc: dict[tuple[str, str], dict] = {}
        for r in kept_rows:
            base_ = r["pair__base__code"]
            quote_ = r["pair__quote__code"]
            vb = r["volume_base"] or Decimal(0)
            vq = r["volume_quote"] or Decimal(0)
            tc = r["trade_count"] or 0
            if not quotes or quote_ in quotes:
                s = weighted_acc.setdefault((base_, quote_), {"vb": Decimal(0), "vq": Decimal(0), "tc": 0})
                s["vb"] += vb; s["vq"] += vq; s["tc"] += tc
            if quote_ in merge_quotes:
                s = full_rolled_acc.setdefault((base_, quote_), {"vb": Decimal(0), "vq": Decimal(0), "tc": 0})
                s["vb"] += vb; s["vq"] += vq; s["tc"] += tc

        weighted = [
            {
                "base": base_,
                "quote": quote_,
                "trade_count": s["tc"],
                "volume_base": str(s["vb"]),
                "volume_quote": str(s["vq"]),
                "vwap": _vwap(s["vq"], s["vb"]),
            }
            for (base_, quote_), s in sorted(weighted_acc.items())
        ]

        full_rolled = [
            {
                "base": b,
                "quote": q,
                "volume_base": s["vb"],
                "volume_quote": s["vq"],
                "trade_count": s["tc"],
            }
            for (b, q), s in full_rolled_acc.items()
        ]
        # Stable→fiat rates derived from the same kept rows for consistency.
        stable_rates: dict[str, dict] = {}
        for (b, q), s in full_rolled_acc.items():
            if peg_map.get(b) != q or s["vb"] <= 0:
                continue
            stable_rates[b] = {
                "fiat": q,
                "rate": s["vq"] / s["vb"],
                "volume_base": s["vb"],
                "volume_quote": s["vq"],
                "trade_count": s["tc"],
            }
        merged = _merge_into_fiat(full_rolled, stable_rates, peg_map, self.FIATS)
        missing = _missing_rate_stables(full_rolled, stable_rates, peg_map, self.FIATS)

        # Apply quote filter to the merged output too (after merging is done).
        if quotes:
            merged = {k: v for k, v in merged.items() if k[1] in quotes}

        weighted_fiat = []
        for (base, fiat), b in sorted(merged.items()):
            vwap = (b["volume_quote_fiat"] / b["volume_base"]) if b["volume_base"] else None
            weighted_fiat.append({
                "base": base,
                "fiat": fiat,
                "vwap": str(vwap) if vwap is not None else None,
                "volume_base": str(b["volume_base"]),
                "volume_quote_fiat": str(b["volume_quote_fiat"]),
                "trade_count": b["trade_count"],
                "components": [
                    {
                        "source_quote": c["source_quote"],
                        "rate": str(c["rate"]),
                        "volume_base": str(c["volume_base"]),
                        "volume_quote_source": str(c["volume_quote_source"]),
                        "contribution_fiat": str(c["contribution_fiat"]),
                        "trade_count": c["trade_count"],
                    }
                    for c in sorted(b["components"], key=lambda c: c["source_quote"])
                ],
            })

        rates_payload = [
            {
                "stable": stable,
                "fiat": info["fiat"],
                "rate": str(info["rate"]),
                "volume_base": str(info["volume_base"]),
                "volume_quote": str(info["volume_quote"]),
                "trade_count": info["trade_count"],
            }
            for stable, info in sorted(stable_rates.items())
        ]

        # by_exchange[] keeps every exchange's row (operators want to see
        # the bad print) but rows that were dropped from at least one
        # minute's consensus aggregate get an additive `is_outlier: true`.
        by_ex_acc: dict[tuple[str, str, str], dict] = {}
        for r in raw_rows:
            quote_ = r["pair__quote__code"]
            if quotes and quote_ not in quotes:
                continue
            base_ = r["pair__base__code"]
            ex = r["pair__exchange__slug"]
            s = by_ex_acc.setdefault((ex, base_, quote_), {"vb": Decimal(0), "vq": Decimal(0), "tc": 0})
            s["vb"] += r["volume_base"] or Decimal(0)
            s["vq"] += r["volume_quote"] or Decimal(0)
            s["tc"] += r["trade_count"] or 0
        by_exchange = []
        for (ex, base_, quote_), s in sorted(by_ex_acc.items()):
            row = {
                "exchange": ex,
                "base": base_,
                "quote": quote_,
                "trade_count": s["tc"],
                "volume_base": str(s["vb"]),
                "volume_quote": str(s["vq"]),
                "vwap": _vwap(s["vq"], s["vb"]),
            }
            if (ex, base_, quote_) in outlier_exchange_pairs:
                row["is_outlier"] = True
            by_exchange.append(row)

        response_body = {
            "as_of": timezone.now().isoformat(),
            "window_minutes": window,
            "weighted": weighted,
            "weighted_fiat": weighted_fiat,
            "stable_rates": rates_payload,
            "missing_rate_stables": missing,
            "by_exchange": by_exchange,
        }
        if outlier_enabled:
            response_body["outliers_excluded"] = report.excluded
        return Response(response_body)


def _active_pairs_for_current():
    # Defer Currency.peg_to: column may not yet exist in the live DB while
    # another session's migration is in flight. We never need it here.
    return list(
        TradingPair.objects.filter(is_active=True, exchange__is_active=True)
        .select_related("exchange", "base", "quote")
        .defer("base__peg_to", "quote__peg_to")
    )


def _fetch_current_rows(pairs):
    """Return a list of dicts (one per pair) joining DB metadata with cache state.

    Pairs with no live cache entry yield a row with `live=False` so the caller
    can distinguish "no data" from "stale".
    """
    by_id = get_current_many(p.pk for p in pairs)
    now_ts = time.time()
    fresh_window = settings.CURRENT_FRESH_SEC
    rows = []
    for p in pairs:
        entry = by_id.get(p.pk)
        if entry is None:
            rows.append({
                "pair_id": p.pk,
                "exchange": p.exchange.slug,
                "base": p.base.code,
                "quote": p.quote.code,
                "symbol": p.cryptofeed_symbol,
                "live": False,
            })
            continue
        age = now_ts - entry["last_update_ts"]
        rows.append({
            "pair_id": p.pk,
            "exchange": entry["exchange"],
            "base": entry["base"],
            "quote": entry["quote"],
            "symbol": entry["symbol"],
            "live": True,
            "fresh": age <= fresh_window,
            "age_sec": age,
            "last_price": entry["last_price"],
            "last_trade_ts": entry["last_trade_ts"],
            "minute_start": entry["minute_start"],
            "minute_count": entry["count"],
            "minute_volume_base": entry["volume_base"],
            "minute_volume_quote": entry["volume_quote"],
            "minute_vwap": entry["vwap_minute"],
            "minute_min": entry["price_min"],
            "minute_max": entry["price_max"],
        })
    return rows


class CurrentPriceView(APIView):
    """Live current price per pair, plus cross-exchange VWAP per (base, quote).

    Reads from the shared memcached current-state layer that each daemon
    updates on every trade (throttled). Independent of the per-minute DB
    flush — so a brand-new minute has data immediately.

    Returns:
      * ``weighted``: per-(base, quote) live VWAP, raw.
      * ``weighted_fiat``: per-(base, fiat) live VWAP merging USDT/USDC into USD.
      * ``stable_rates``: live stable→fiat rates used for the merge.
      * ``by_pair``: per-pair live state.

    Query params (all optional):
        base      repeatable, filter by base currency (e.g. ``?base=BTC``).
        quote     repeatable, filter by quote currency (e.g. ``?quote=USD``).
        exchange  repeatable, filter by exchange slug.
    """

    FIATS = ("USD", "EUR")

    @extend_schema(
        tags=["prices"],
        summary="Live (sub-minute) volume-weighted prices",
        parameters=[
            OpenApiParameter("base", OpenApiTypes.STR, many=True, description="Repeatable. Filter by base currency. Default: all."),
            OpenApiParameter("quote", OpenApiTypes.STR, many=True, description="Repeatable. Filter by quote currency. Default: all."),
            OpenApiParameter("exchange", OpenApiTypes.STR, many=True, description="Repeatable. Filter by exchange slug. Default: all."),
        ],
        responses=OpenApiTypes.OBJECT,
    )
    def get(self, request):
        bases = set(request.query_params.getlist("base"))
        quotes = set(request.query_params.getlist("quote"))
        exchanges = set(request.query_params.getlist("exchange"))

        pairs = _active_pairs_for_current()
        # Filter for `by_pair`/`weighted` views, but keep stable-quoted pairs
        # in scope for the fiat merge so contributions can feed USD/EUR.
        peg_map = _stable_peg_map()
        merge_quotes = set(self.FIATS) | set(peg_map.keys())

        def _user_filter(p):
            if bases and p.base.code not in bases:
                return False
            if quotes and p.quote.code not in quotes:
                return False
            if exchanges and p.exchange.slug not in exchanges:
                return False
            return True

        def _merge_filter(p):
            if bases and p.base.code not in bases:
                return False
            if exchanges and p.exchange.slug not in exchanges:
                return False
            # For the merged view, include both fiat-quoted and stable-quoted
            # pairs regardless of the user's quote filter, plus stable→fiat
            # rate pairs.
            return p.quote.code in merge_quotes or p.base.code in peg_map

        user_pairs = [p for p in pairs if _user_filter(p)]
        merge_pairs = [p for p in pairs if _merge_filter(p)]

        rows = _fetch_current_rows(user_pairs)
        merge_rows = _fetch_current_rows(merge_pairs)

        # Per-(base, quote) outlier filtering on live rows. Use last_price as
        # the per-exchange "vwap" and minute_volume_base as the weight.
        outlier_enabled = getattr(settings, "OUTLIER_FILTER_ENABLED", False)
        report = OutlierReport()
        live_outlier_pairs: set[tuple[str, str, str]] = set()
        now_iso = timezone.now().isoformat()

        def _filter_live_rows(input_rows: list[dict]) -> list[dict]:
            if not outlier_enabled:
                return input_rows
            by_pair: dict[tuple[str, str], list[dict]] = defaultdict(list)
            passthrough: list[dict] = []
            for r in input_rows:
                if not r.get("live") or not r.get("fresh"):
                    passthrough.append(r)
                    continue
                by_pair[(r["base"], r["quote"])].append(r)
            kept_all: list[dict] = list(passthrough)
            for (base_, quote_), group in by_pair.items():
                if len(group) < 2:
                    kept_all.extend(group)
                    continue
                mapped = [
                    {
                        "exchange": r["exchange"],
                        "vwap": r["last_price"],
                        "volume_base": r.get("minute_volume_base") or Decimal(0),
                        "_raw": r,
                    }
                    for r in group
                ]
                before = {m["exchange"] for m in mapped}
                kept = filter_exchange_outliers(
                    mapped, base=base_, quote=quote_, minute=now_iso,
                    max_dev_pct=settings.OUTLIER_EXCHANGE_MAX_DEV_PCT,
                    report=report,
                )
                after = {m["exchange"] for m in kept}
                for ex in before - after:
                    live_outlier_pairs.add((ex, base_, quote_))
                kept_all.extend(m["_raw"] for m in kept)
            return kept_all

        rows_filtered = _filter_live_rows(rows)
        merge_rows_filtered = _filter_live_rows(merge_rows)

        # Cross-exchange VWAP per (base, quote) using only live+fresh rows.
        grouped: dict[tuple[str, str], dict] = {}
        for r in rows_filtered:
            if not r.get("live") or not r.get("fresh"):
                continue
            key = (r["base"], r["quote"])
            g = grouped.setdefault(key, {
                "base": r["base"],
                "quote": r["quote"],
                "volume_base": Decimal(0),
                "volume_quote": Decimal(0),
                "count": 0,
                "sum_weighted_price": Decimal(0),
                "sum_weight": Decimal(0),
                "exchanges": 0,
            })
            g["volume_base"] += r["minute_volume_base"]
            g["volume_quote"] += r["minute_volume_quote"]
            g["count"] += r["minute_count"]
            weight = r["minute_volume_base"] if r["minute_volume_base"] > 0 else Decimal(1)
            g["sum_weighted_price"] += r["last_price"] * weight
            g["sum_weight"] += weight
            g["exchanges"] += 1

        weighted = []
        for g in grouped.values():
            if g["volume_base"] > 0:
                price = g["volume_quote"] / g["volume_base"]
            elif g["sum_weight"] > 0:
                price = g["sum_weighted_price"] / g["sum_weight"]
            else:
                price = None
            weighted.append({
                "base": g["base"],
                "quote": g["quote"],
                "price": str(price) if price is not None else None,
                "minute_volume_base": str(g["volume_base"]),
                "minute_volume_quote": str(g["volume_quote"]),
                "minute_trades": g["count"],
                "exchanges": g["exchanges"],
            })
        weighted.sort(key=lambda x: (x["base"], x["quote"]))

        # Build the fiat-merged live view from merge_rows_filtered (broader scope).
        stable_rates_live = _stable_rates_from_live(merge_rows_filtered, peg_map)
        per_quote_live: dict[tuple[str, str], dict] = {}
        for r in merge_rows_filtered:
            if not r.get("live") or not r.get("fresh"):
                continue
            base = r["base"]
            quote = r["quote"]
            # Skip stable→fiat conversion pairs — they're rate inputs, not
            # asset prices we report (USDT-USD shouldn't appear as BTC-USD's
            # input twice).
            if base in peg_map:
                continue
            key = (base, quote)
            slot = per_quote_live.setdefault(key, {
                "base": base,
                "quote": quote,
                "volume_base": Decimal(0),
                "volume_quote": Decimal(0),
                "trade_count": 0,
                "sum_wp": Decimal(0),
                "sum_w": Decimal(0),
            })
            slot["volume_base"] += r["minute_volume_base"]
            slot["volume_quote"] += r["minute_volume_quote"]
            slot["trade_count"] += r["minute_count"]
            weight = r["minute_volume_base"] if r["minute_volume_base"] > 0 else Decimal(1)
            slot["sum_wp"] += r["last_price"] * weight
            slot["sum_w"] += weight

        merge_input = []
        for (base, quote), s in per_quote_live.items():
            # Use last-price fallback into volume_quote when there's no
            # minute volume yet, so a just-rolled minute still contributes.
            vb = s["volume_base"]
            vq = s["volume_quote"]
            if vb == 0 and s["sum_w"] > 0:
                # Synthesize a 1-unit base / wp-quote pseudovolume.
                vb = s["sum_w"]
                vq = s["sum_wp"]
            merge_input.append({
                "base": base, "quote": quote,
                "volume_base": vb, "volume_quote": vq,
                "trade_count": s["trade_count"],
            })
        merged = _merge_into_fiat(merge_input, stable_rates_live, peg_map, self.FIATS)
        missing = _missing_rate_stables(merge_input, stable_rates_live, peg_map, self.FIATS)

        if quotes:
            merged = {k: v for k, v in merged.items() if k[1] in quotes}

        weighted_fiat = []
        for (base, fiat), b in sorted(merged.items()):
            vwap = (b["volume_quote_fiat"] / b["volume_base"]) if b["volume_base"] else None
            weighted_fiat.append({
                "base": base,
                "fiat": fiat,
                "price": str(vwap) if vwap is not None else None,
                "trade_count": b["trade_count"],
                "components": [
                    {
                        "source_quote": c["source_quote"],
                        "rate": str(c["rate"]),
                        "contribution_fiat": str(c["contribution_fiat"]),
                    }
                    for c in sorted(b["components"], key=lambda c: c["source_quote"])
                ],
            })

        rates_payload = [
            {
                "stable": stable,
                "fiat": info["fiat"],
                "rate": str(info["rate"]),
                "exchanges": info["exchanges"],
            }
            for stable, info in sorted(stable_rates_live.items())
        ]

        per_pair = []
        for r in rows:
            if not r.get("live"):
                per_pair.append({
                    "exchange": r["exchange"],
                    "base": r["base"],
                    "quote": r["quote"],
                    "symbol": r["symbol"],
                    "live": False,
                })
                continue
            entry = {
                "exchange": r["exchange"],
                "base": r["base"],
                "quote": r["quote"],
                "symbol": r["symbol"],
                "live": True,
                "fresh": r["fresh"],
                "age_sec": r["age_sec"],
                "last_price": str(r["last_price"]),
                "last_trade_ts": r["last_trade_ts"],
                "minute_start": r["minute_start"].isoformat(),
                "minute_trades": r["minute_count"],
                "minute_volume_base": str(r["minute_volume_base"]),
                "minute_vwap": str(r["minute_vwap"]),
                "minute_min": str(r["minute_min"]),
                "minute_max": str(r["minute_max"]),
            }
            if (r["exchange"], r["base"], r["quote"]) in live_outlier_pairs:
                entry["is_outlier"] = True
            per_pair.append(entry)
        per_pair.sort(key=lambda x: (x["base"], x["quote"], x["exchange"]))

        response_body = {
            "as_of": now_iso,
            "fresh_window_sec": settings.CURRENT_FRESH_SEC,
            "weighted": weighted,
            "weighted_fiat": weighted_fiat,
            "stable_rates": rates_payload,
            "missing_rate_stables": missing,
            "by_pair": per_pair,
        }
        if outlier_enabled:
            response_body["outliers_excluded"] = report.excluded
        return Response(response_body)


def _stable_rates_by_minute(
    stables_for_fiat: list[str],
    fiat: str,
    start_dt,
    end_dt,
    *,
    report: OutlierReport | None,
) -> dict[tuple[str, datetime], Decimal]:
    """Per-(stable, minute) stable→fiat rate, with outlier-exchange rows
    dropped from the cross-exchange aggregation when filtering is enabled."""
    rates: dict[tuple[str, datetime], Decimal] = {}
    if not stables_for_fiat:
        return rates
    rate_rows = list(
        MinuteAggregate.objects
        .filter(
            pair__base__code__in=stables_for_fiat,
            pair__quote__code=fiat,
            pair__exchange__is_active=True,
            minute_start__gte=start_dt,
            minute_start__lt=end_dt,
        )
        .values(
            "pair__exchange__slug",
            "pair__base__code",
            "minute_start",
            "volume_base",
            "volume_quote",
            "price_vwap",
        )
    )
    enabled = report is not None and getattr(settings, "OUTLIER_FILTER_ENABLED", False)
    groups: dict[tuple[str, datetime], list[dict]] = defaultdict(list)
    for r in rate_rows:
        groups[(r["pair__base__code"], r["minute_start"])].append(r)
    for (stable, minute), group in groups.items():
        rows = group
        if enabled and len(rows) >= 2:
            mapped = [
                {
                    "exchange": r["pair__exchange__slug"],
                    "vwap": r["price_vwap"],
                    "volume_base": r["volume_base"] or Decimal(0),
                    "_raw": r,
                }
                for r in rows
            ]
            kept = filter_exchange_outliers(
                mapped, base=stable, quote=fiat, minute=minute,
                max_dev_pct=settings.OUTLIER_EXCHANGE_MAX_DEV_PCT,
                report=report,
            )
            rows = [m["_raw"] for m in kept]
        vb_sum = Decimal(0)
        vq_sum = Decimal(0)
        for r in rows:
            vb_sum += r["volume_base"] or Decimal(0)
            vq_sum += r["volume_quote"] or Decimal(0)
        if vb_sum > 0:
            rates[(stable, minute)] = vq_sum / vb_sum
    return rates


def _merged_minute_candles(
    start_dt,
    end_dt,
    base: str,
    fiat: str,
    peg_map: dict[str, str],
    *,
    report: OutlierReport | None = None,
) -> list[dict]:
    """Per-minute OHLC for (base, fiat) merged across exchanges and stables.

    For each minute m, contributors are the per-(exchange, quote) MinuteAggregates
    where quote is `fiat` (rate 1) or a stablecoin pegged to `fiat`. Stable rows
    are converted with that *same minute's* VWAP stable→fiat rate, falling back
    to the nearest earlier minute's rate within the window, so a minute with no
    stable trades still resolves a rate.

    The merged candle is:
      high  = max(clipped price_max contributors × rate)
      low   = min(clipped price_min contributors × rate)
      vwap  = sum(volume_quote × rate) / sum(volume_base)
      close = vwap
      open  = previous minute's vwap (first minute opens at its own vwap)
    """
    stables_for_fiat = [s for s, f in peg_map.items() if f == fiat]
    merge_quotes = {fiat, *stables_for_fiat}

    rates_by_minute = _stable_rates_by_minute(
        stables_for_fiat, fiat, start_dt, end_dt, report=report,
    )

    def _rate_for(stable: str, minute: datetime) -> Decimal | None:
        rate = rates_by_minute.get((stable, minute))
        if rate is not None:
            return rate
        # Walk back through preceding minutes in the window for a fallback.
        best: tuple[datetime, Decimal] | None = None
        for (s, m), v in rates_by_minute.items():
            if s != stable or m > minute:
                continue
            if best is None or m > best[0]:
                best = (m, v)
        return best[1] if best else None

    # Pull per-(exchange, quote, minute) rows so we can filter outlier
    # exchanges and clip per-exchange wicks before merging.
    contrib_rows = list(
        MinuteAggregate.objects
        .filter(
            pair__base__code=base,
            pair__quote__code__in=merge_quotes,
            pair__exchange__is_active=True,
            minute_start__gte=start_dt,
            minute_start__lt=end_dt,
        )
        .values(
            "pair__exchange__slug",
            "pair__quote__code",
            "minute_start",
            "volume_base",
            "volume_quote",
            "price_min",
            "price_max",
            "price_vwap",
        )
    )

    enabled = report is not None and getattr(settings, "OUTLIER_FILTER_ENABLED", False)
    wick_max_pct = settings.OUTLIER_WICK_MAX_PCT if enabled else None

    groups: dict[tuple[str, datetime], list[dict]] = defaultdict(list)
    for r in contrib_rows:
        groups[(r["pair__quote__code"], r["minute_start"])].append(r)

    kept_contribs: list[dict] = []
    for (quote, minute), group in groups.items():
        if enabled and len(group) >= 2:
            mapped = [
                {
                    "exchange": r["pair__exchange__slug"],
                    "vwap": r["price_vwap"],
                    "volume_base": r["volume_base"] or Decimal(0),
                    "_raw": r,
                }
                for r in group
            ]
            kept = filter_exchange_outliers(
                mapped, base=base, quote=quote, minute=minute,
                max_dev_pct=settings.OUTLIER_EXCHANGE_MAX_DEV_PCT,
                report=report,
            )
            kept_contribs.extend(m["_raw"] for m in kept)
        else:
            kept_contribs.extend(group)

    merged: dict[datetime, dict] = {}
    for r in kept_contribs:
        minute = r["minute_start"]
        quote = r["pair__quote__code"]
        if quote == fiat:
            rate = Decimal(1)
        else:
            rate = _rate_for(quote, minute)
            if rate is None:
                continue
        vb = r["volume_base"] or Decimal(0)
        if not vb:
            continue
        pmin = r["price_min"]
        pmax = r["price_max"]
        if wick_max_pct is not None:
            pmin, pmax = clip_wick(pmin, pmax, r["price_vwap"], max_pct=wick_max_pct, report=report)
        vq = (r["volume_quote"] or Decimal(0)) * rate
        hi = (pmax or Decimal(0)) * rate
        lo = (pmin or Decimal(0)) * rate
        slot = merged.setdefault(minute, {
            "volume_base": Decimal(0),
            "volume_quote_fiat": Decimal(0),
            "high": None,
            "low": None,
        })
        slot["volume_base"] += vb
        slot["volume_quote_fiat"] += vq
        if slot["high"] is None or hi > slot["high"]:
            slot["high"] = hi
        if slot["low"] is None or lo < slot["low"]:
            slot["low"] = lo

    out: list[dict] = []
    prev_close: Decimal | None = None
    for minute in sorted(merged.keys()):
        slot = merged[minute]
        if not slot["volume_base"]:
            continue
        vwap = slot["volume_quote_fiat"] / slot["volume_base"]
        op = prev_close if prev_close is not None else vwap
        hi = max(slot["high"], op, vwap)
        lo = min(slot["low"], op, vwap)
        out.append({
            "time": int(minute.timestamp()),
            "open": float(op),
            "high": float(hi),
            "low": float(lo),
            "close": float(vwap),
        })
        prev_close = vwap
    return out


def _merged_minute_rows(
    start_dt,
    end_dt,
    base: str,
    fiat: str,
    peg_map: dict[str, str],
    *,
    report: OutlierReport | None = None,
) -> list[dict]:
    """Per-minute rich rows for (base, fiat) merged across exchanges and stables.

    Same merging semantics as ``_merged_minute_candles`` but returns
    Decimal-valued OHLC + volume_base + trades + the set of source quote
    currencies that contributed to each minute.
    """
    stables_for_fiat = [s for s, f in peg_map.items() if f == fiat]
    merge_quotes = {fiat, *stables_for_fiat}

    rates_by_minute = _stable_rates_by_minute(
        stables_for_fiat, fiat, start_dt, end_dt, report=report,
    )

    def _rate_for(stable: str, minute: datetime) -> Decimal | None:
        rate = rates_by_minute.get((stable, minute))
        if rate is not None:
            return rate
        best: tuple[datetime, Decimal] | None = None
        for (s, m), v in rates_by_minute.items():
            if s != stable or m > minute:
                continue
            if best is None or m > best[0]:
                best = (m, v)
        return best[1] if best else None

    contrib_rows = list(
        MinuteAggregate.objects
        .filter(
            pair__base__code=base,
            pair__quote__code__in=merge_quotes,
            pair__exchange__is_active=True,
            minute_start__gte=start_dt,
            minute_start__lt=end_dt,
        )
        .values(
            "pair__exchange__slug",
            "pair__quote__code",
            "minute_start",
            "volume_base",
            "volume_quote",
            "price_min",
            "price_max",
            "price_vwap",
            "trade_count",
        )
    )

    enabled = report is not None and getattr(settings, "OUTLIER_FILTER_ENABLED", False)
    wick_max_pct = settings.OUTLIER_WICK_MAX_PCT if enabled else None

    groups: dict[tuple[str, datetime], list[dict]] = defaultdict(list)
    for r in contrib_rows:
        groups[(r["pair__quote__code"], r["minute_start"])].append(r)

    kept_contribs: list[dict] = []
    for (quote, minute), group in groups.items():
        if enabled and len(group) >= 2:
            mapped = [
                {
                    "exchange": r["pair__exchange__slug"],
                    "vwap": r["price_vwap"],
                    "volume_base": r["volume_base"] or Decimal(0),
                    "_raw": r,
                }
                for r in group
            ]
            kept = filter_exchange_outliers(
                mapped, base=base, quote=quote, minute=minute,
                max_dev_pct=settings.OUTLIER_EXCHANGE_MAX_DEV_PCT,
                report=report,
            )
            kept_contribs.extend(m["_raw"] for m in kept)
        else:
            kept_contribs.extend(group)

    merged: dict[datetime, dict] = {}
    for r in kept_contribs:
        minute = r["minute_start"]
        quote = r["pair__quote__code"]
        rate = Decimal(1) if quote == fiat else _rate_for(quote, minute)
        if rate is None:
            continue
        vb = r["volume_base"] or Decimal(0)
        if not vb:
            continue
        pmin = r["price_min"]
        pmax = r["price_max"]
        if wick_max_pct is not None:
            pmin, pmax = clip_wick(pmin, pmax, r["price_vwap"], max_pct=wick_max_pct, report=report)
        vq = (r["volume_quote"] or Decimal(0)) * rate
        hi = (pmax or Decimal(0)) * rate
        lo = (pmin or Decimal(0)) * rate
        slot = merged.setdefault(minute, {
            "volume_base": Decimal(0),
            "volume_quote_fiat": Decimal(0),
            "high": None,
            "low": None,
            "trades": 0,
            "quotes": set(),
        })
        slot["volume_base"] += vb
        slot["volume_quote_fiat"] += vq
        slot["trades"] += r["trade_count"] or 0
        slot["quotes"].add(quote)
        if slot["high"] is None or hi > slot["high"]:
            slot["high"] = hi
        if slot["low"] is None or lo < slot["low"]:
            slot["low"] = lo

    if base == "BTC" and fiat in ("USD", "EUR"):
        _fill_legacy_btc_gaps(merged, start_dt, end_dt, fiat)

    rows: list[dict] = []
    prev_close: Decimal | None = None
    for minute in sorted(merged.keys()):
        slot = merged[minute]
        if slot["volume_base"]:
            vwap = slot["volume_quote_fiat"] / slot["volume_base"]
        else:
            # Legacy-only minute: no volume, single price stamped into high/low.
            vwap = slot["high"]
        op = prev_close if prev_close is not None else vwap
        hi = max(slot["high"], op, vwap)
        lo = min(slot["low"], op, vwap)
        rows.append({
            "minute": minute,
            "open": op,
            "high": hi,
            "low": lo,
            "close": vwap,
            "volume_base": slot["volume_base"],
            "trades": slot["trades"],
            "quotes": sorted(slot["quotes"]),
        })
        prev_close = vwap
    return rows


def _fill_legacy_btc_gaps(
    merged: dict, start_dt: datetime, end_dt: datetime, fiat: str
) -> None:
    """For BTC/(USD|EUR), fill minutes not covered by MinuteAggregate from
    ``HistoricalBtcPrice``. Each legacy minute is stamped with a
    ``"legacy:<kind>"`` quote sentinel so the template can surface it.
    """
    price_field = f"price_{fiat.lower()}"
    legacy_rows = (
        HistoricalBtcPrice.objects
        .filter(
            observed_at__gte=start_dt,
            observed_at__lt=end_dt,
            **{f"{price_field}__isnull": False},
        )
        .values("observed_at", price_field, "kind")
    )
    for r in legacy_rows:
        minute = r["observed_at"].replace(second=0, microsecond=0)
        if minute in merged:
            continue  # real data wins
        price = r[price_field]
        merged[minute] = {
            "volume_base": Decimal(0),
            "volume_quote_fiat": Decimal(0),
            "high": price,
            "low": price,
            "trades": 0,
            "quotes": {f"legacy:{r['kind'] or 'reg'}"},
        }


@method_decorator(cache_page(settings.API_CACHE_TTL_CANDLES), name="dispatch")
@method_decorator(vary_on_headers("Accept"), name="dispatch")
class CandlesView(APIView):
    """Per-minute OHLC candles for BTC against each primary fiat.

    Merges stable-quoted exchanges (USDT, USDC) into the underlying fiat using
    same-minute stable→fiat rates, like the other endpoints.

    Query params:
        window  integer minutes back, default 120, max 1440.
        base    base currency code, default "BTC".
        fiat    repeatable; defaults to USD and EUR.
    """

    DEFAULT_FIATS = ("USD", "EUR")

    @extend_schema(
        tags=["candles"],
        summary="Per-minute OHLC candles",
        parameters=[
            OpenApiParameter("window", OpenApiTypes.INT, description="Minutes back. Default 120, clamped to [1, 1440]."),
            OpenApiParameter("base", OpenApiTypes.STR, description='Base currency code. Default "BTC".'),
            OpenApiParameter("fiat", OpenApiTypes.STR, many=True, description="Repeatable. Default: USD and EUR."),
        ],
        responses=OpenApiTypes.OBJECT,
    )
    def get(self, request):
        try:
            window = int(request.query_params.get("window", 120))
        except ValueError:
            return Response({"detail": "window must be an integer"}, status=400)
        window = max(1, min(window, 1440))

        base = request.query_params.get("base", "BTC")
        fiats = tuple(request.query_params.getlist("fiat")) or self.DEFAULT_FIATS

        now = timezone.now()
        end_dt = now.replace(second=0, microsecond=0)
        start_dt = end_dt - timedelta(minutes=window)
        peg_map = _stable_peg_map()

        outlier_enabled = getattr(settings, "OUTLIER_FILTER_ENABLED", False)
        report = OutlierReport() if outlier_enabled else None
        series = {
            fiat: _merged_minute_candles(start_dt, end_dt, base, fiat, peg_map, report=report)
            for fiat in fiats
        }
        body = {
            "as_of": now.isoformat(),
            "base": base,
            "window_minutes": window,
            "series": series,
        }
        if report is not None:
            body["outliers_excluded"] = len(report.excluded)
            body["wicks_clipped"] = report.wicks_clipped
        return Response(body)


def _resample_candles(rows: list[dict], interval_min: int) -> list[dict]:
    """Aggregate 1-minute OHLC rows into ``interval_min``-minute buckets.

    Buckets align to epoch second 0 mod (interval_min * 60), so a 5-minute
    series snaps to :00 / :05 / :10 / … in UTC.
    """
    if interval_min <= 1 or not rows:
        return rows
    step = interval_min * 60
    out: list[dict] = []
    bucket: dict | None = None
    for r in rows:
        b = (r["time"] // step) * step
        if bucket is None or b != bucket["time"]:
            if bucket is not None:
                out.append(bucket)
            bucket = {
                "time": b,
                "open": r["open"],
                "high": r["high"],
                "low": r["low"],
                "close": r["close"],
            }
        else:
            if r["high"] > bucket["high"]:
                bucket["high"] = r["high"]
            if r["low"] < bucket["low"]:
                bucket["low"] = r["low"]
            bucket["close"] = r["close"]
    if bucket is not None:
        out.append(bucket)
    return out


@method_decorator(cache_page(settings.API_CACHE_TTL_CANDLES), name="dispatch")
@method_decorator(vary_on_headers("Accept"), name="dispatch")
class CandlesAggView(APIView):
    """Aggregated OHLC candles for ``base`` against each requested fiat.

    Like ``/api/candles/`` but with an ``interval`` parameter that buckets the
    underlying per-minute merged series into N-minute candles aligned to UTC.

    Query params:
        window    int minutes back, default 360, clamped to [1, 1440].
        base      base currency code, default "BTC".
        fiat      repeatable; defaults to USD and EUR.
        interval  int bucket size in minutes, default 1, clamped to [1, window].
    """

    DEFAULT_FIATS = ("USD", "EUR")

    @extend_schema(
        tags=["candles"],
        summary="Resampled OHLC candles",
        parameters=[
            OpenApiParameter("window", OpenApiTypes.INT, description="Minutes back. Default 360, clamped to [1, 1440]."),
            OpenApiParameter("base", OpenApiTypes.STR, description='Base currency code. Default "BTC".'),
            OpenApiParameter("fiat", OpenApiTypes.STR, many=True, description="Repeatable. Default: USD and EUR."),
            OpenApiParameter("interval", OpenApiTypes.INT, description="Bucket size in minutes. Default 1, clamped to [1, window]. Buckets align to epoch second 0."),
        ],
        responses=OpenApiTypes.OBJECT,
    )
    def get(self, request):
        try:
            window = int(request.query_params.get("window", 360))
        except ValueError:
            return Response({"detail": "window must be an integer"}, status=400)
        window = max(1, min(window, 1440))
        try:
            interval = int(request.query_params.get("interval", 1))
        except ValueError:
            return Response({"detail": "interval must be an integer"}, status=400)
        interval = max(1, min(interval, window))

        base = request.query_params.get("base", "BTC")
        fiats = tuple(request.query_params.getlist("fiat")) or self.DEFAULT_FIATS

        now = timezone.now()
        end_dt = now.replace(second=0, microsecond=0)
        start_dt = end_dt - timedelta(minutes=window)
        peg_map = _stable_peg_map()

        series = {
            fiat: _resample_candles(
                _merged_minute_candles(start_dt, end_dt, base, fiat, peg_map),
                interval,
            )
            for fiat in fiats
        }
        return Response({
            "as_of": now.isoformat(),
            "base": base,
            "window_minutes": window,
            "interval_minutes": interval,
            "series": series,
        })


class HealthView(APIView):
    """Liveness probe. Returns the most recent minute aggregate timestamp."""

    @extend_schema(tags=["ops"], summary="Liveness probe", responses=OpenApiTypes.OBJECT)
    def get(self, request):
        latest = (
            MinuteAggregate.objects.order_by("-minute_start").values("minute_start").first()
        )
        return Response({
            "ok": True,
            "now": timezone.now().isoformat(),
            "latest_minute": latest["minute_start"].isoformat() if latest else None,
        })


@method_decorator(cache_page(settings.API_CACHE_TTL_OVERVIEW), name="dispatch")
class OverviewView(View):
    """Server-rendered single-page dashboard for BTC pricing."""

    template_name = "api/overview.html"
    fragment_template_name = "api/_overview_content.html"
    SHORT_WINDOW_MIN = 5
    LONG_WINDOW_MIN = 60
    # Hero cards: these quotes get top billing, then others alphabetical.
    PRIMARY_QUOTES = ("USD", "EUR")
    FIATS = ("USD", "EUR")

    @classmethod
    def _order_by_priority(cls, d: dict) -> dict:
        out = {q: d[q] for q in cls.PRIMARY_QUOTES if q in d}
        for q in sorted(d):
            if q not in out:
                out[q] = d[q]
        return out

    def get(self, request):
        now = timezone.now()
        short_cutoff = now - timedelta(minutes=self.SHORT_WINDOW_MIN)
        long_cutoff = now - timedelta(minutes=self.LONG_WINDOW_MIN)

        peg_map = _stable_peg_map()

        active_pairs = _active_pairs_for_current()
        current_rows = _fetch_current_rows(active_pairs)
        current_by_pair_id = {r["pair_id"]: r for r in current_rows}

        # Cross-exchange volume-weighted current price per quote (raw).
        live_current = {}
        for r in current_rows:
            if not r.get("live") or not r.get("fresh"):
                continue
            # Don't include stable→fiat pairs in BTC-quoted live cards.
            if r["base"] in peg_map:
                continue
            slot = live_current.setdefault(r["quote"], {
                "volume_base": Decimal(0),
                "volume_quote": Decimal(0),
                "sum_weighted_price": Decimal(0),
                "sum_weight": Decimal(0),
                "exchanges": 0,
            })
            slot["volume_base"] += r["minute_volume_base"]
            slot["volume_quote"] += r["minute_volume_quote"]
            weight = r["minute_volume_base"] if r["minute_volume_base"] > 0 else Decimal(1)
            slot["sum_weighted_price"] += r["last_price"] * weight
            slot["sum_weight"] += weight
            slot["exchanges"] += 1
        for quote, slot in live_current.items():
            if slot["volume_base"] > 0:
                slot["price"] = slot["volume_quote"] / slot["volume_base"]
            elif slot["sum_weight"] > 0:
                slot["price"] = slot["sum_weighted_price"] / slot["sum_weight"]
            else:
                slot["price"] = None

        # Fiat-merged live cards (USD includes USDT, USDC after rate conversion).
        stable_rates_live = _stable_rates_from_live(current_rows, peg_map)
        merge_input_live = []
        for quote, slot in live_current.items():
            vb = slot["volume_base"]
            vq = slot["volume_quote"]
            if vb == 0 and slot["sum_weight"] > 0:
                vb = slot["sum_weight"]
                vq = slot["sum_weighted_price"]
            merge_input_live.append({
                "base": "BTC", "quote": quote,
                "volume_base": vb, "volume_quote": vq,
                "trade_count": 0,
            })
        merged_live = _merge_into_fiat(
            merge_input_live, stable_rates_live, peg_map, self.FIATS,
        )
        live_fiat = {}
        for (base, fiat), b in merged_live.items():
            if base != "BTC":
                continue
            price = (b["volume_quote_fiat"] / b["volume_base"]) if b["volume_base"] else None
            live_fiat[fiat] = {
                "price": price,
                "components": [
                    {"source_quote": c["source_quote"], "rate": c["rate"]}
                    for c in sorted(b["components"], key=lambda c: c["source_quote"])
                ],
            }

        # Window-based aggregate query
        active_qs = MinuteAggregate.objects.filter(
            minute_start__gte=short_cutoff,
            pair__exchange__is_active=True,
        )

        rolled = (
            active_qs.values("pair__quote__code", "pair__base__code")
            .annotate(
                volume_base=Sum("volume_base"),
                volume_quote=Sum("volume_quote"),
                trades=Sum("trade_count"),
            )
            .order_by("pair__quote__code")
        )
        # Raw per-quote VWAP for BTC, excluding stable→fiat conversion pairs.
        global_vwap = {}
        full_rolled = []
        for r in rolled:
            full_rolled.append({
                "base": r["pair__base__code"],
                "quote": r["pair__quote__code"],
                "volume_base": r["volume_base"] or Decimal(0),
                "volume_quote": r["volume_quote"] or Decimal(0),
                "trade_count": r["trades"] or 0,
            })
            if r["pair__base__code"] == "BTC":
                global_vwap[r["pair__quote__code"]] = {
                    "vwap": _vwap_dec(r["volume_quote"], r["volume_base"]),
                    "volume_base": r["volume_base"] or Decimal(0),
                    "trades": r["trades"] or 0,
                }

        stable_rates_window = _stable_rates_from_window(active_qs, peg_map)
        merged_window = _merge_into_fiat(
            full_rolled, stable_rates_window, peg_map, self.FIATS,
        )
        global_vwap_fiat = {}
        for (base, fiat), b in merged_window.items():
            if base != "BTC":
                continue
            vwap = (b["volume_quote_fiat"] / b["volume_base"]) if b["volume_base"] else None
            global_vwap_fiat[fiat] = {
                "vwap": vwap,
                "volume_base": b["volume_base"],
                "trades": b["trade_count"],
                "components": [
                    {
                        "source_quote": c["source_quote"],
                        "rate": c["rate"],
                        "volume_base": c["volume_base"],
                    }
                    for c in sorted(b["components"], key=lambda c: c["source_quote"])
                ],
            }

        # Per-exchange × quote: latest minute aggregate + last-hour trade count
        latest_per_pair = (
            MinuteAggregate.objects.filter(pair__exchange__is_active=True)
            .values("pair_id")
            .annotate(latest=Max("minute_start"))
        )
        latest_map = {r["pair_id"]: r["latest"] for r in latest_per_pair}

        recent_aggs = (
            MinuteAggregate.objects.filter(
                pair__exchange__is_active=True,
                minute_start__in=[v for v in latest_map.values() if v is not None],
            )
            .select_related("pair", "pair__exchange", "pair__quote")
            .defer("pair__quote__peg_to")
        )
        latest_rows = {
            (a.pair_id, a.minute_start): a
            for a in recent_aggs
            if latest_map.get(a.pair_id) == a.minute_start
        }

        hourly = (
            MinuteAggregate.objects.filter(
                pair__exchange__is_active=True, minute_start__gte=long_cutoff
            )
            .values("pair_id")
            .annotate(trades=Sum("trade_count"), volume=Sum("volume_base"))
        )
        hourly_map = {r["pair_id"]: r for r in hourly}

        rows = []
        sorted_pairs = sorted(
            active_pairs,
            key=lambda p: (p.exchange.slug, p.cryptofeed_symbol),
        )
        for pair in sorted_pairs:
            latest_minute = latest_map.get(pair.id)
            agg = latest_rows.get((pair.id, latest_minute)) if latest_minute else None
            age_sec = (now - latest_minute).total_seconds() if latest_minute else None
            hour = hourly_map.get(pair.id, {})
            cur = current_by_pair_id.get(pair.id, {"live": False})
            rows.append({
                "exchange": pair.exchange.slug,
                "quote": pair.quote.code,
                "symbol": pair.cryptofeed_symbol,
                "latest_minute": latest_minute,
                "age_sec": age_sec,
                "fresh": age_sec is not None and age_sec <= 180,
                "stale": age_sec is None or age_sec > 600,
                "vwap": agg.price_vwap if agg else None,
                "min": agg.price_min if agg else None,
                "max": agg.price_max if agg else None,
                "trades_minute": agg.trade_count if agg else 0,
                "trades_hour": hour.get("trades", 0),
                "volume_hour": hour.get("volume", Decimal(0)),
                "current_price": cur.get("last_price") if cur.get("live") else None,
                "current_fresh": cur.get("live") and cur.get("fresh"),
                "current_age_sec": cur.get("age_sec"),
            })

        global_vwap = self._order_by_priority(global_vwap)
        live_current = self._order_by_priority(live_current)
        global_vwap_fiat = self._order_by_priority(global_vwap_fiat)
        live_fiat = self._order_by_priority(live_fiat)
        ctx = {
            "now": now,
            "short_window": self.SHORT_WINDOW_MIN,
            "long_window": self.LONG_WINDOW_MIN,
            "global_vwap": global_vwap,
            "global_vwap_fiat": global_vwap_fiat,
            "live_current": live_current,
            "live_fiat": live_fiat,
            "stable_rates_live": stable_rates_live,
            "stable_rates_window": stable_rates_window,
            "primary_quotes": list(self.PRIMARY_QUOTES),
            "fresh_window_sec": settings.CURRENT_FRESH_SEC,
            "rows": rows,
            "active_exchanges": Exchange.objects.filter(is_active=True).count(),
            "total_aggregates": MinuteAggregate.objects.count(),
        }
        template = (
            self.fragment_template_name
            if request.GET.get("fragment") == "1"
            else self.template_name
        )
        return render(request, template, ctx)


def _downsample_for_chart(rows: list[dict], max_candles: int = 1100) -> list[dict]:
    """Bucket-aggregate ``rows`` into at most ``max_candles`` OHLC candles.

    The chart renderer outputs one SVG group per row; rendering 10k+ candles
    blows the page past the memcached 1 MB cap and is unreadable anyway. We
    keep the per-minute table un-downsampled.
    """
    n = len(rows)
    if n <= max_candles:
        return rows
    bucket = (n + max_candles - 1) // max_candles
    out: list[dict] = []
    for i in range(0, n, bucket):
        chunk = rows[i:i + bucket]
        highs = [c["high"] for c in chunk]
        lows = [c["low"] for c in chunk]
        out.append({
            "minute": chunk[0]["minute"],
            "open": chunk[0]["open"],
            "high": max(highs),
            "low": min(lows),
            "close": chunk[-1]["close"],
            "volume_base": sum((c["volume_base"] for c in chunk), Decimal(0)),
            "trades": sum(c["trades"] for c in chunk),
            "quotes": chunk[-1]["quotes"],
        })
    return out


def _build_candle_svg(
    rows: list[dict],
    width: int = 1100,
    height: int = 320,
    pad_x: int = 8,
    pad_y: int = 20,
) -> dict:
    """Layout per-minute OHLC rows into SVG-ready primitives.

    Returns ``{"candles": [...], "y_ticks": [...], "x_ticks": [...], width, height, ...}``
    so the template just renders elements without numeric work.
    """
    if not rows:
        return {"candles": [], "y_ticks": [], "x_ticks": [], "width": width, "height": height}

    plot_w = width - pad_x * 2
    plot_h = height - pad_y * 2
    n = len(rows)
    cand_w = max(1.0, plot_w / n)
    body_w = max(1.0, cand_w * 0.75)

    lows = [float(r["low"]) for r in rows]
    highs = [float(r["high"]) for r in rows]
    y_min = min(lows)
    y_max = max(highs)
    if y_max == y_min:
        y_max = y_min + 1
    y_pad = (y_max - y_min) * 0.05
    y_min -= y_pad
    y_max += y_pad

    def y_of(v: float) -> float:
        return pad_y + (y_max - v) / (y_max - y_min) * plot_h

    candles = []
    for i, r in enumerate(rows):
        op = float(r["open"])
        cl = float(r["close"])
        hi = float(r["high"])
        lo = float(r["low"])
        x_center = pad_x + (i + 0.5) * cand_w
        body_top = y_of(max(op, cl))
        body_bot = y_of(min(op, cl))
        body_h = max(1.0, body_bot - body_top)
        candles.append({
            "x_body": x_center - body_w / 2,
            "y_body": body_top,
            "w_body": body_w,
            "h_body": body_h,
            "x_wick": x_center,
            "y_high": y_of(hi),
            "y_low": y_of(lo),
            "color": "#3aa75e" if cl >= op else "#c44e4e",
            "tooltip": (
                f"{r['minute'].strftime('%H:%M')}  "
                f"O {op:.2f}  H {hi:.2f}  L {lo:.2f}  C {cl:.2f}  "
                f"trades {r['trades']}"
            ),
        })

    # 5 horizontal price labels, evenly spaced.
    y_ticks = []
    for i in range(5):
        v = y_max - (y_max - y_min) * i / 4
        y_ticks.append({"y": y_of(v), "label": f"{v:.2f}"})

    # X labels at start, middle, end (and quartiles if room).
    x_ticks = []
    for frac in (0.0, 0.25, 0.5, 0.75, 1.0):
        idx = min(n - 1, int(frac * (n - 1)))
        x_ticks.append({
            "x": pad_x + (idx + 0.5) * cand_w,
            "label": rows[idx]["minute"].strftime("%H:%M"),
        })

    return {
        "candles": candles,
        "y_ticks": y_ticks,
        "x_ticks": x_ticks,
        "width": width,
        "height": height,
        "y_min": y_min,
        "y_max": y_max,
    }


@method_decorator(cache_page(settings.API_CACHE_TTL_HISTORY), name="dispatch")
class HistoryView(View):
    """Per-minute historical price view: chart + table for a (base, fiat).

    Query params:
        base   default ``BTC``.
        fiat   default ``USD``.
        window minutes back, default 360 (6h), max 1440 (24h).
    """

    template_name = "api/history.html"
    DEFAULT_WINDOW = 360
    MAX_WINDOW = 10080
    WINDOW_PRESETS = (60, 360, 1440, 10080)
    DEFAULT_BASE = "BTC"
    DEFAULT_FIAT = "USD"

    def get(self, request):
        base = request.GET.get("base", self.DEFAULT_BASE).upper()
        fiat = request.GET.get("fiat", self.DEFAULT_FIAT).upper()
        try:
            window = int(request.GET.get("window", self.DEFAULT_WINDOW))
        except ValueError:
            window = self.DEFAULT_WINDOW
        window = max(1, min(window, self.MAX_WINDOW))

        now = timezone.now()
        end_dt = now.replace(second=0, microsecond=0)
        start_dt = end_dt - timedelta(minutes=window)

        peg_map = _stable_peg_map()
        rows = _merged_minute_rows(start_dt, end_dt, base, fiat, peg_map)

        # Pick fiats that actually have a trading pair (direct or pegged) on file.
        fiat_codes = list(
            Currency.objects.filter(is_quote=True, peg_to__isnull=True)
            .values_list("code", flat=True)
        )
        fiat_codes = sorted(set(fiat_codes) | {self.DEFAULT_FIAT, "EUR"})

        # Summary stats over the window.
        summary = None
        if rows:
            first = rows[0]
            last = rows[-1]
            highs = [r["high"] for r in rows]
            lows = [r["low"] for r in rows]
            total_vol = sum((r["volume_base"] for r in rows), Decimal(0))
            total_tr = sum(r["trades"] for r in rows)
            change = last["close"] - first["open"]
            change_pct = (change / first["open"] * Decimal(100)) if first["open"] else Decimal(0)
            summary = {
                "open": first["open"],
                "close": last["close"],
                "high": max(highs),
                "low": min(lows),
                "change": change,
                "change_pct": change_pct,
                "change_up": change >= 0,
                "volume_base": total_vol,
                "trades": total_tr,
                "minute_count": len(rows),
            }

        # Table: newest-first, capped so the page stays reasonable.
        table_rows = []
        for r in reversed(rows[-720:]):
            change = r["close"] - r["open"]
            table_rows.append({**r, "change": change, "change_up": change >= 0})

        chart = _build_candle_svg(_downsample_for_chart(rows))

        ctx = {
            "base": base,
            "fiat": fiat,
            "window": window,
            "now": now,
            "start_dt": start_dt,
            "end_dt": end_dt,
            "fiat_codes": fiat_codes,
            "window_presets": self.WINDOW_PRESETS,
            "rows": table_rows,
            "summary": summary,
            "chart": chart,
            "has_rows": bool(rows),
        }
        return render(request, self.template_name, ctx)
