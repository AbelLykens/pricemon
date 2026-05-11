"""Runtime fixes applied to bundled cryptofeed adapters.

cryptofeed 2.4.1 ships a KuCoin subscribe that emits ``topic`` strings
like ``/market/match: BTC-EUR`` (note the leading space after the
colon). Most KuCoin markets tolerate the space, but the FIAT BTC-EUR
pair is rejected with::

    {'code': 400, 'data': 'topic /market/match: BTC-EUR is invalid'}

Verified against the live KuCoin public WS: ``/market/match:BTC-EUR``
is acknowledged, ``/market/match: BTC-EUR`` is not. Importing this
module rebinds ``KuCoin.subscribe`` with the space removed.
"""

from __future__ import annotations

import json

from cryptofeed.connection import AsyncConnection
from cryptofeed.defines import CANDLES
from cryptofeed.exchanges.kucoin import KuCoin


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
