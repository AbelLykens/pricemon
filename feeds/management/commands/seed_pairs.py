from django.core.management.base import BaseCommand
from django.db import transaction

from core.models import Currency, Exchange, TradingPair
from feeds.exchanges import EXCHANGE_DEFINITIONS


# (code, name, peg_to_code) — peg_to is the fiat a stablecoin tracks, used
# by the API to merge stable-quoted volume into the fiat bucket.
SEED_QUOTES = [
    ("USD",  "US Dollar",           None),
    ("EUR",  "Euro",                None),
    ("GBP",  "British Pound",       None),
    ("CHF",  "Swiss Franc",         None),
    ("JPY",  "Japanese Yen",        None),
    ("AUD",  "Australian Dollar",   None),
    ("CAD",  "Canadian Dollar",     None),
    ("USDT", "Tether USD",          "USD"),
    ("USDC", "USD Coin",            "USD"),
]
SEED_BASES = [
    ("BTC", "Bitcoin"),
]


class Command(BaseCommand):
    help = (
        "Seed Currency, Exchange and TradingPair rows from "
        "feeds.exchanges.EXCHANGE_DEFINITIONS. Idempotent."
    )

    @transaction.atomic
    def handle(self, *args, **options):
        # Two-pass currency seed so peg_to FKs can resolve in pass 2.
        for code, name, _peg in SEED_QUOTES:
            Currency.objects.update_or_create(
                code=code, defaults={"name": name, "is_quote": True}
            )
        for code, name in SEED_BASES:
            Currency.objects.update_or_create(
                code=code, defaults={"name": name, "is_quote": False}
            )
        for code, _name, peg in SEED_QUOTES:
            if peg is None:
                continue
            cur = Currency.objects.get(code=code)
            cur.peg_to = Currency.objects.get(code=peg)
            cur.save(update_fields=["peg_to"])

        currencies = {c.code: c for c in Currency.objects.all()}

        new_exchanges = 0
        new_pairs = 0
        for d in EXCHANGE_DEFINITIONS:
            ex, ex_created = Exchange.objects.update_or_create(
                slug=d.slug,
                defaults={"name": d.cryptofeed_id, "is_active": d.enabled},
            )
            new_exchanges += int(ex_created)

            # BTC-quoted pairs
            btc = currencies["BTC"]
            for q_code in d.quotes:
                q_obj = currencies[q_code]
                _, created = TradingPair.objects.update_or_create(
                    exchange=ex,
                    base=btc,
                    quote=q_obj,
                    defaults={"cryptofeed_symbol": f"BTC-{q_code}", "is_active": True},
                )
                new_pairs += int(created)

            # Extra pairs (stable→fiat conversion markets)
            for base_code, quote_code in d.extra_pairs:
                b_obj = currencies[base_code]
                q_obj = currencies[quote_code]
                _, created = TradingPair.objects.update_or_create(
                    exchange=ex,
                    base=b_obj,
                    quote=q_obj,
                    defaults={
                        "cryptofeed_symbol": f"{base_code}-{quote_code}",
                        "is_active": True,
                    },
                )
                new_pairs += int(created)

        self.stdout.write(self.style.SUCCESS(
            f"seed complete: {Exchange.objects.count()} exchanges "
            f"({new_exchanges} new), {Currency.objects.count()} currencies, "
            f"{TradingPair.objects.count()} pairs ({new_pairs} new)"
        ))
