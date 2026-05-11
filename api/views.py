import time
from datetime import timedelta
from decimal import Decimal

from django.conf import settings
from django.db.models import Max, Sum
from django.shortcuts import render
from django.utils import timezone
from django.views import View
from rest_framework.response import Response
from rest_framework.views import APIView

from core.models import Currency, Exchange, MinuteAggregate, TradingPair
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

        # quote filter applies only to the raw rollup; the fiat-merged view
        # still needs stable-quoted rows to compute its USD/EUR contributions.
        qs_for_raw = qs.filter(pair__quote__code__in=quotes) if quotes else qs

        peg_map = _stable_peg_map()

        rolled = (
            qs_for_raw.values("pair__base__code", "pair__quote__code")
            .annotate(
                volume_base=Sum("volume_base"),
                volume_quote=Sum("volume_quote"),
                trade_count=Sum("trade_count"),
            )
            .order_by("pair__base__code", "pair__quote__code")
        )
        weighted = [
            {
                "base": row["pair__base__code"],
                "quote": row["pair__quote__code"],
                "trade_count": row["trade_count"],
                "volume_base": str(row["volume_base"] or Decimal(0)),
                "volume_quote": str(row["volume_quote"] or Decimal(0)),
                "vwap": _vwap(row["volume_quote"] or Decimal(0), row["volume_base"] or Decimal(0)),
            }
            for row in rolled
        ]

        # Fiat-merged view: pull *all* fiat + stable-quoted rows regardless
        # of the user's quote filter, so stable contributions still feed
        # into USD/EUR. Honor base/exchange filters.
        merge_quotes = set(self.FIATS) | set(peg_map.keys())
        full_rolled_qs = (
            qs.filter(pair__quote__code__in=merge_quotes)
            .values("pair__base__code", "pair__quote__code")
            .annotate(
                volume_base=Sum("volume_base"),
                volume_quote=Sum("volume_quote"),
                trade_count=Sum("trade_count"),
            )
        )
        full_rolled = [
            {
                "base": r["pair__base__code"],
                "quote": r["pair__quote__code"],
                "volume_base": r["volume_base"] or Decimal(0),
                "volume_quote": r["volume_quote"] or Decimal(0),
                "trade_count": r["trade_count"] or 0,
            }
            for r in full_rolled_qs
        ]
        stable_rates = _stable_rates_from_window(qs, peg_map)
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

        per_ex = (
            qs_for_raw.values(
                "pair__exchange__slug",
                "pair__base__code",
                "pair__quote__code",
            )
            .annotate(
                volume_base=Sum("volume_base"),
                volume_quote=Sum("volume_quote"),
                trade_count=Sum("trade_count"),
            )
            .order_by("pair__exchange__slug", "pair__base__code", "pair__quote__code")
        )
        by_exchange = [
            {
                "exchange": row["pair__exchange__slug"],
                "base": row["pair__base__code"],
                "quote": row["pair__quote__code"],
                "trade_count": row["trade_count"],
                "volume_base": str(row["volume_base"] or Decimal(0)),
                "volume_quote": str(row["volume_quote"] or Decimal(0)),
                "vwap": _vwap(row["volume_quote"] or Decimal(0), row["volume_base"] or Decimal(0)),
            }
            for row in per_ex
        ]

        return Response({
            "as_of": timezone.now().isoformat(),
            "window_minutes": window,
            "weighted": weighted,
            "weighted_fiat": weighted_fiat,
            "stable_rates": rates_payload,
            "missing_rate_stables": missing,
            "by_exchange": by_exchange,
        })


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

        # Cross-exchange VWAP per (base, quote) using only live+fresh rows.
        grouped: dict[tuple[str, str], dict] = {}
        for r in rows:
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

        # Build the fiat-merged live view from merge_rows (broader scope).
        stable_rates_live = _stable_rates_from_live(merge_rows, peg_map)
        per_quote_live: dict[tuple[str, str], dict] = {}
        for r in merge_rows:
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
            per_pair.append({
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
            })
        per_pair.sort(key=lambda x: (x["base"], x["quote"], x["exchange"]))

        return Response({
            "as_of": timezone.now().isoformat(),
            "fresh_window_sec": settings.CURRENT_FRESH_SEC,
            "weighted": weighted,
            "weighted_fiat": weighted_fiat,
            "stable_rates": rates_payload,
            "missing_rate_stables": missing,
            "by_pair": per_pair,
        })


class HealthView(APIView):
    """Liveness probe. Returns the most recent minute aggregate timestamp."""

    def get(self, request):
        latest = (
            MinuteAggregate.objects.order_by("-minute_start").values("minute_start").first()
        )
        return Response({
            "ok": True,
            "now": timezone.now().isoformat(),
            "latest_minute": latest["minute_start"].isoformat() if latest else None,
        })


class OverviewView(View):
    """Server-rendered single-page dashboard for BTC pricing."""

    template_name = "api/overview.html"
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
        return render(request, self.template_name, ctx)
