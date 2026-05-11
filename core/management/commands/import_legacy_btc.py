"""Ingest a legacy BTC price archive into ``core.HistoricalBtcPrice``.

CSV columns: id, price_usd, price_eur, price_type, when, added.
Per-row entries carry both USD and EUR prices for the same timestamp; the
``price_type`` letter labels cadence (``''`` = 1-min regular, ``H`` = backfill,
``L`` = sparse live, ``M`` = manual). Re-imports are idempotent via the
``(source, legacy_id)`` unique constraint.
"""

from __future__ import annotations

import csv
import gzip
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from django.db.models.functions import TruncDate

from core.models import HistoricalBtcPrice, MinuteAggregate


PROGRESS_EVERY = 100_000


def _open_csv(path: Path):
    if path.suffix == ".gz":
        return gzip.open(path, mode="rt", newline="", encoding="utf-8")
    return open(path, mode="rt", newline="", encoding="utf-8")


def _parse_decimal(raw: str | None) -> Decimal | None:
    if raw is None or raw == "":
        return None
    try:
        return Decimal(raw)
    except InvalidOperation as exc:
        raise CommandError(f"bad decimal {raw!r}: {exc}") from None


class Command(BaseCommand):
    help = (
        "Import a legacy BTC price archive CSV (gzipped or plain) into "
        "core.HistoricalBtcPrice. Idempotent on (source, legacy_id). Skips "
        "any (utc_date, quote) where MinuteAggregate already has BTC data."
    )

    def add_arguments(self, parser) -> None:
        parser.add_argument("path", type=Path, help="Path to archive .csv or .csv.gz")
        parser.add_argument(
            "--source",
            default="legacy-archive",
            help='Value for HistoricalBtcPrice.source (default: "legacy-archive")',
        )
        parser.add_argument("--batch-size", type=int, default=5000)
        parser.add_argument("--dry-run", action="store_true")
        parser.add_argument(
            "--limit",
            type=int,
            default=0,
            help="Stop after processing this many CSV rows (0 = unlimited)",
        )

    def handle(self, *args, **opts) -> None:
        path: Path = opts["path"]
        source: str = opts["source"]
        batch_size: int = opts["batch_size"]
        dry_run: bool = opts["dry_run"]
        limit: int = opts["limit"]

        if not path.exists():
            raise CommandError(f"file not found: {path}")

        covered = self._build_coverage_set()
        self.stdout.write(
            f"coverage skip set: {len(covered)} (utc_date, quote) entries "
            "for BTC/USD or BTC/EUR"
        )

        processed = 0
        written = 0
        skipped_covered = 0
        skipped_null = 0
        batch: list[HistoricalBtcPrice] = []

        with _open_csv(path) as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                processed += 1
                if limit and processed > limit:
                    processed -= 1  # do not count the unread row
                    break

                observed_at = datetime.fromisoformat(row["when"])
                obs_date = observed_at.date()
                kind = row.get("price_type") or ""

                usd = _parse_decimal(row.get("price_usd"))
                eur = _parse_decimal(row.get("price_eur"))

                if usd is not None and (obs_date, "USD") in covered:
                    usd = None
                if eur is not None and (obs_date, "EUR") in covered:
                    eur = None

                if usd is None and eur is None:
                    # Either both prices were already null in the source, or
                    # both got dropped because their day is covered.
                    if row.get("price_usd") or row.get("price_eur"):
                        skipped_covered += 1
                    else:
                        skipped_null += 1
                    continue

                added_raw = row.get("added") or ""
                added_at = datetime.fromisoformat(added_raw) if added_raw else None

                batch.append(
                    HistoricalBtcPrice(
                        source=source,
                        legacy_id=int(row["id"]),
                        observed_at=observed_at,
                        kind=kind,
                        price_usd=usd,
                        price_eur=eur,
                        legacy_added_at=added_at,
                    )
                )

                if len(batch) >= batch_size:
                    written += self._flush(batch, dry_run)
                    batch.clear()

                if processed % PROGRESS_EVERY == 0:
                    self.stdout.write(
                        f"  processed={processed:>9}  written={written:>9}  "
                        f"skipped_covered={skipped_covered:>7}  "
                        f"skipped_null={skipped_null:>5}"
                    )

            if batch:
                written += self._flush(batch, dry_run)
                batch.clear()

        verb = "would write" if dry_run else "wrote"
        self.stdout.write(self.style.SUCCESS(
            f"done. processed={processed} {verb}={written} "
            f"skipped_covered={skipped_covered} skipped_null={skipped_null}"
        ))

    def _build_coverage_set(self) -> set[tuple]:
        rows = (
            MinuteAggregate.objects
            .filter(pair__base__code="BTC", pair__quote__code__in=["USD", "EUR"])
            .annotate(d=TruncDate("minute_start"))
            .values_list("d", "pair__quote__code")
            .distinct()
        )
        return set(rows)

    def _flush(self, batch: list[HistoricalBtcPrice], dry_run: bool) -> int:
        if dry_run:
            return len(batch)
        with transaction.atomic():
            HistoricalBtcPrice.objects.bulk_create(
                batch,
                update_conflicts=True,
                unique_fields=["source", "legacy_id"],
                update_fields=[
                    "observed_at",
                    "kind",
                    "price_usd",
                    "price_eur",
                    "legacy_added_at",
                ],
                batch_size=len(batch),
            )
        return len(batch)
