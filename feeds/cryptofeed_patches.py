"""Runtime fixes applied to bundled cryptofeed adapters.

cryptofeed 2.4.1 ships a KuCoin subscribe that emits ``topic`` strings
like ``/market/match: BTC-EUR`` (note the leading space after the
colon). Most KuCoin markets tolerate the space, but the FIAT BTC-EUR
pair is rejected with::

    {'code': 400, 'data': 'topic /market/match: BTC-EUR is invalid'}

Verified against the live KuCoin public WS: ``/market/match:BTC-EUR``
is acknowledged, ``/market/match: BTC-EUR`` is not. Importing this
module rebinds ``KuCoin.subscribe`` with the space removed.

It also redirects the bundled ``Coinbase`` adapter from Advanced Trade
(which requires HMAC-signed subscriptions, i.e. API credentials, even
for public trade data) to the legacy *Coinbase Exchange* public WS
(``wss://ws-feed.exchange.coinbase.com``) plus its public products
REST endpoint. Trade messages on that endpoint arrive as ``type=match``
events with the same product_id format (``BTC-USD``) used internally.
"""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from decimal import Decimal

from yapic import json as yjson  # parses ISO-8601 strings to datetime

from cryptofeed.connection import AsyncConnection, RestEndpoint, Routes, WebsocketEndpoint
from cryptofeed.defines import BUY, CANDLES, L2_BOOK, SELL, TRADES
from cryptofeed.exchanges.coinbase import Coinbase
from cryptofeed.exchanges.kucoin import KuCoin
from cryptofeed.symbols import Symbol
from cryptofeed.types import Trade

log = logging.getLogger(__name__)


async def _kucoin_subscribe(self, conn: AsyncConnection):
    self._KuCoin__reset()
    for chan in self.subscription:
        symbols = list(self.subscription[chan])
        nchan = self.exchange_channel_to_std(chan)
        if nchan == CANDLES:
            for symbol in symbols:
                await conn.write(json.dumps({
                    'id': 1,
                    'type': 'subscribe',
                    'topic': f"{chan}:{symbol}_{self.candle_interval_map[self.candle_interval]}",
                    'privateChannel': False,
                    'response': True,
                }))
        else:
            for slice_index in range(0, len(symbols), 100):
                batch = ','.join(symbols[slice_index:slice_index + 100])
                await conn.write(json.dumps({
                    'id': 1,
                    'type': 'subscribe',
                    'topic': f"{chan}:{batch}",
                    'privateChannel': False,
                    'response': True,
                }))


KuCoin.subscribe = _kucoin_subscribe


# --- Coinbase: use the public Exchange WS instead of Advanced Trade -----

Coinbase.websocket_endpoints = [
    WebsocketEndpoint(
        "wss://ws-feed.exchange.coinbase.com",
        options={"compression": None},
    ),
]
Coinbase.rest_endpoints = [
    RestEndpoint(
        "https://api.exchange.coinbase.com",
        routes=Routes("/products"),
    ),
]
# Map cryptofeed's normalized channel names to the public-WS channel names.
Coinbase.websocket_channels = {
    L2_BOOK: "level2_batch",
    TRADES: "matches",
}


@classmethod
def _coinbase_parse_symbol_data(cls, data):
    ret: dict = {}
    info: dict = defaultdict(dict)
    for entry in data:
        if entry.get("trading_disabled"):
            continue
        sym = Symbol(entry["base_currency"], entry["quote_currency"])
        info["tick_size"][sym.normalized] = entry["quote_increment"]
        info["instrument_type"][sym.normalized] = sym.type
        ret[sym.normalized] = entry["id"]
    return ret, info


Coinbase._parse_symbol_data = _coinbase_parse_symbol_data


@classmethod
def _coinbase_symbols(cls, config=None, refresh=False):
    return list(cls.symbol_mapping(refresh=refresh).keys())


Coinbase.symbols = _coinbase_symbols


async def _coinbase_subscribe(self, conn: AsyncConnection):
    # self.subscription is keyed by public-WS channel name thanks to the
    # rebound websocket_channels above.
    for chan, product_ids in self.subscription.items():
        await conn.write(json.dumps({
            "type": "subscribe",
            "product_ids": list(product_ids),
            "channels": [chan],
        }))


Coinbase.subscribe = _coinbase_subscribe


async def _coinbase_message_handler(self, msg: str, conn: AsyncConnection, timestamp: float):
    # yapic.json auto-parses ISO-8601 strings to datetime, which
    # ``self.timestamp_normalize`` requires.
    data = yjson.loads(msg, parse_float=Decimal)
    mtype = data.get("type")
    if mtype in ("match", "last_match"):
        pair = self.exchange_symbol_to_std_symbol(data["product_id"])
        ts = self.timestamp_normalize(data["time"])
        t = Trade(
            self.id,
            pair,
            SELL if data["side"] == "sell" else BUY,
            Decimal(data["size"]),
            Decimal(data["price"]),
            ts,
            id=str(data.get("trade_id", "")),
            type="market",
            raw=data,
        )
        await self.callback(TRADES, t, timestamp)
    elif mtype in ("subscriptions", "heartbeat"):
        pass
    elif mtype == "error":
        log.warning("Coinbase WS error: %s", data)


Coinbase.message_handler = _coinbase_message_handler
