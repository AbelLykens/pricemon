from django.core.management.base import BaseCommand, CommandError

from feeds import cryptofeed_patches  # noqa: F401  applies KuCoin subscribe fix
from feeds.daemon import FeedDaemon
from feeds.exchanges import SUPPORTED_SLUGS


class Command(BaseCommand):
    help = "Run the cryptofeed daemon for one exchange (one process per exchange)."

    def add_arguments(self, parser):
        parser.add_argument(
            "--exchange",
            required=True,
            help=(
                "Exchange slug to run; one of: "
                + ", ".join(sorted(SUPPORTED_SLUGS))
            ),
        )

    def handle(self, *args, exchange: str, **options):
        if exchange not in SUPPORTED_SLUGS:
            raise CommandError(f"Unsupported exchange '{exchange}'")
        FeedDaemon(exchange_slug=exchange).run()
