"""Per-exchange registry of cryptofeed feeds.

For each exchange we store:
  * ``quotes``: BTC-quoted spot pairs (BTC-X for each X in quotes)
  * ``extra_pairs``: additional (base, quote) spot pairs that aren't BTC-based,
    notably stablecoin↔fiat (USDT-USD, USDC-EUR, …) used by the API to
    convert USDT/USDC volume into the underlying fiat bucket.

Both lists were derived empirically by calling ``Exchange.symbols()`` on
every class in ``cryptofeed.exchanges.EXCHANGE_MAP`` and keeping the spot
markets we care about.

To add a new exchange: append to ``EXCHANGE_DEFINITIONS`` and re-run
``manage.py seed_pairs``.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from cryptofeed.exchanges import EXCHANGE_MAP


@dataclass(frozen=True)
class ExchangeDef:
    slug: str           # lowercase, used in DB and systemd unit instance names
    cryptofeed_id: str  # key in cryptofeed.exchanges.EXCHANGE_MAP
    quotes: tuple[str, ...]  # BTC-quoted pairs (BTC-X for each X)
    extra_pairs: tuple[tuple[str, str], ...] = ()  # non-BTC spot pairs (base, quote)
    enabled: bool = True


# Quotes per exchange derived empirically from cryptofeed's symbols()
# against the BTC base. extra_pairs lists stablecoin↔fiat markets used as
# conversion rates for the API.
EXCHANGE_DEFINITIONS: list[ExchangeDef] = [
    ExchangeDef("kraken", "KRAKEN",
                ("USD", "EUR", "GBP", "CHF", "JPY", "AUD", "CAD", "USDT", "USDC"),
                extra_pairs=(("USDT", "USD"), ("USDT", "EUR"),
                             ("USDC", "USD"), ("USDC", "EUR"))),
    ExchangeDef("bitfinex", "BITFINEX",
                ("USD", "EUR", "GBP", "USDT")),
    ExchangeDef("bitstamp", "BITSTAMP",
                ("USD", "EUR", "GBP", "USDT", "USDC"),
                extra_pairs=(("USDT", "USD"), ("USDT", "EUR"),
                             ("USDC", "USD"), ("USDC", "EUR"))),
    # Coinbase Advanced Trade requires API credentials even to enumerate
    # public products. Set enabled=True after configuring cryptofeed creds.
    ExchangeDef("coinbase", "COINBASE",
                ("USD", "EUR", "GBP"), enabled=False),
    ExchangeDef("ascendex", "ASCENDEX",
                ("USD", "USDT"),
                extra_pairs=(("USDT", "USD"),)),
    ExchangeDef("bequant", "BEQUANT",
                ("USD", "CHF", "USDT", "USDC"),
                extra_pairs=(("USDT", "USD"),)),
    ExchangeDef("binance", "BINANCE",
                ("USD", "EUR", "JPY", "USDT", "USDC"),
                extra_pairs=(("USDT", "USD"), ("USDC", "USD"))),
    ExchangeDef("binance_us", "BINANCE_US",
                ("USD", "USDT", "USDC"),
                extra_pairs=(("USDT", "USD"), ("USDC", "USD"))),
    ExchangeDef("binance_tr", "BINANCE_TR",
                ("USD", "EUR", "JPY", "USDT", "USDC"),
                extra_pairs=(("USDT", "USD"), ("USDC", "USD"))),
    ExchangeDef("bitflyer", "BITFLYER",
                ("USD", "EUR", "JPY")),
    # Blockchain.com Exchange: WS reachable but effectively zero trade
    # volume on BTC pairs (0 trades observed over 60s probes). Daemon sits
    # in a permanent stall-reconnect loop. Disabled until volume returns.
    ExchangeDef("blockchain", "BLOCKCHAIN",
                ("USD", "EUR", "GBP", "USDT"), enabled=False),
    ExchangeDef("bybit", "BYBIT",
                ("USD", "EUR", "GBP", "USDT", "USDC"),
                extra_pairs=(("USDT", "USD"), ("USDT", "EUR"),
                             ("USDC", "EUR"))),
    ExchangeDef("gemini", "GEMINI",
                ("USD", "EUR", "GBP", "USDT", "USDC"),
                extra_pairs=(("USDT", "USD"), ("USDC", "USD"))),
    # Independent Reserve: WS reachable but BTC pairs have ~no volume
    # (one stray flush per hour); intermittent opening-handshake timeouts.
    # Disabled until it's worth the noise.
    ExchangeDef("independent_reserve", "INDEPENDENT_RESERVE",
                ("USD", "AUD"),
                extra_pairs=(("USDT", "USD"), ("USDC", "USD")),
                enabled=False),
    ExchangeDef("kucoin", "KUCOIN",
                ("EUR", "USDT", "USDC"),
                extra_pairs=(("USDT", "EUR"), ("USDC", "EUR"))),
    ExchangeDef("okx", "OKX",
                ("USD", "EUR", "AUD", "USDT", "USDC"),
                extra_pairs=(("USDT", "USD"), ("USDT", "EUR"),
                             ("USDC", "EUR"))),
]


SUPPORTED_SLUGS: list[str] = [d.slug for d in EXCHANGE_DEFINITIONS]


def find(slug: str) -> ExchangeDef:
    for d in EXCHANGE_DEFINITIONS:
        if d.slug == slug:
            return d
    raise KeyError(slug)


def feed_class(slug: str):
    """Return the cryptofeed Feed class for ``slug``."""
    return EXCHANGE_MAP[find(slug).cryptofeed_id]
