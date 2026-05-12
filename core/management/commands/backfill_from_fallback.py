"""Pull MinuteAggregate rows from a fallback peer and fill primary-side gaps.

Pulls from /api/v1/internal/aggregates/ on the configured fallback host
(``FALLBACK_BASE_URL`` + ``FALLBACK_BACKFILL_TOKEN``) and inserts any rows
the primary does not already have. ON CONFLICT DO NOTHING semantics mean
primary-written rows are never overwritten — this only fills holes.

Designed to run on a systemd timer every few minutes. Idempotent.
"""

from __future__ import annotations

import logging
from datetime import timedelta
from decimal import Decimal
from typing import Iterable

import requests
from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from django.utils import timezone

from core.models import MinuteAggregate, TradingPair

log = logging.getLogger("backfill")


class Command(BaseCommand):
    help = (
        "Pull MinuteAggregate rows from the configured fallback host and "
        "insert any that are missing on this host."
    )

    def add_arguments(self, parser) -> None:
        parser.add_argument(
            "--lookback-minutes",
            type=int,
            default=1440,
            help="How far back to look for missing rows (default: 1440 = 24h).",
        )
        parser.add_argument(
            "--min-age-minutes",
            type=int,
            default=5,
            help=(
                "Skip minutes newer than now - this. Gives the primary's own "
                "late writes a chance to land first (default: 5)."
            ),
        )
        parser.add_argument(
            "--chunk-hours",
            type=int,
            default=6,
            help="Time-range chunk size for paginated GETs (default: 6).",
        )
        parser.add_argument(
            "--exchange",
            action="append",
            default=None,
            help="Repeatable. Restrict to specific exchange slugs. Default: all the fallback offers.",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Fetch and count, but do not insert.",
        )

    def handle(self, *args, **opts) -> None:
        base_url = (getattr(settings, "FALLBACK_BASE_URL", "") or "").rstrip("/")
        token = getattr(settings, "FALLBACK_BACKFILL_TOKEN", "") or ""
        if not base_url or not token:
            raise CommandError(
                "FALLBACK_BASE_URL and FALLBACK_BACKFILL_TOKEN must be set in .env"
            )

        lookback = max(1, int(opts["lookback_minutes"]))
        min_age = max(0, int(opts["min_age_minutes"]))
        chunk_hours = max(1, int(opts["chunk_hours"]))
        dry_run = bool(opts["dry_run"])
        exchanges = opts.get("exchange") or []

        now = timezone.now()
        until = now - timedelta(minutes=min_age)
        since = now - timedelta(minutes=lookback)

        pair_cache = _PairCache()
        session = requests.Session()
        session.headers["Authorization"] = f"Bearer {token}"
        timeout = int(getattr(settings, "FALLBACK_BACKFILL_TIMEOUT_SEC", 30))

        totals = {"fetched": 0, "inserted": 0, "skipped_pair_missing": 0}

        chunk = timedelta(hours=chunk_hours)
        cursor_start = since
        while cursor_start < until:
            cursor_end = min(cursor_start + chunk, until)
            self._process_window(
                session=session,
                base_url=base_url,
                timeout=timeout,
                window_start=cursor_start,
                window_end=cursor_end,
                exchanges=exchanges,
                pair_cache=pair_cache,
                totals=totals,
                dry_run=dry_run,
            )
            cursor_start = cursor_end

        log.info(
            "backfill done: fetched=%d inserted=%d skipped_pair_missing=%d dry_run=%s",
            totals["fetched"], totals["inserted"], totals["skipped_pair_missing"], dry_run,
        )
        self.stdout.write(self.style.SUCCESS(
            f"fetched={totals['fetched']} "
            f"inserted={totals['inserted']} "
            f"skipped_pair_missing={totals['skipped_pair_missing']} "
            f"dry_run={dry_run}"
        ))

    def _process_window(
        self,
        *,
        session: requests.Session,
        base_url: str,
        timeout: int,
        window_start,
        window_end,
        exchanges: list[str],
        pair_cache: "_PairCache",
        totals: dict,
        dry_run: bool,
    ) -> None:
        url = f"{base_url}/api/v1/internal/aggregates/"
        params: list[tuple[str, str]] = [
            ("since", window_start.isoformat()),
            ("until", window_end.isoformat()),
            ("limit", "2000"),
        ]
        for ex in exchanges:
            params.append(("exchange", ex))

        cursor = 0
        while True:
            page_params = list(params)
            if cursor:
                page_params.append(("cursor", str(cursor)))
            resp = session.get(url, params=page_params, timeout=timeout)
            if resp.status_code != 200:
                raise CommandError(
                    f"fallback returned HTTP {resp.status_code}: {resp.text[:200]}"
                )
            payload = resp.json()
            rows = payload.get("rows") or []
            totals["fetched"] += len(rows)

            to_insert = []
            for r in rows:
                pair_id = pair_cache.lookup(r["exchange"], r["base"], r["quote"])
                if pair_id is None:
                    totals["skipped_pair_missing"] += 1
                    continue
                to_insert.append(MinuteAggregate(
                    pair_id=pair_id,
                    minute_start=r["minute_start"],
                    trade_count=int(r["trade_count"]),
                    volume_base=Decimal(r["volume_base"]),
                    volume_quote=Decimal(r["volume_quote"]),
                    price_min=Decimal(r["price_min"]),
                    price_max=Decimal(r["price_max"]),
                    price_avg=Decimal(r["price_avg"]),
                    price_vwap=Decimal(r["price_vwap"]),
                ))

            if to_insert and not dry_run:
                with transaction.atomic():
                    created = MinuteAggregate.objects.bulk_create(
                        to_insert, ignore_conflicts=True, batch_size=500,
                    )
                # bulk_create with ignore_conflicts returns the input list
                # on Postgres, but the rows that conflicted have pk=None.
                inserted = sum(1 for r in created if r.pk is not None)
                totals["inserted"] += inserted
            elif to_insert and dry_run:
                totals["inserted"] += len(to_insert)  # potential, for visibility

            next_cursor = payload.get("next_cursor")
            if not next_cursor:
                break
            cursor = int(next_cursor)


class _PairCache:
    """Resolves (exchange_slug, base_code, quote_code) -> primary's pair.pk."""

    def __init__(self) -> None:
        rows = TradingPair.objects.select_related(
            "exchange", "base", "quote",
        ).values_list("exchange__slug", "base__code", "quote__code", "id")
        self._map = {
            (slug, base, quote): pk for slug, base, quote, pk in rows
        }
        self._missing_logged: set[tuple[str, str, str]] = set()

    def lookup(self, exchange: str, base: str, quote: str) -> int | None:
        key = (exchange, base, quote)
        pk = self._map.get(key)
        if pk is None and key not in self._missing_logged:
            log.warning(
                "backfill: no local TradingPair for (%s, %s, %s); skipping rows",
                exchange, base, quote,
            )
            self._missing_logged.add(key)
        return pk
