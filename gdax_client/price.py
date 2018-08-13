# This file is part of Maker Keeper Framework.
#
# Copyright (C) 2017-2018 reverendus
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import json
import logging
import threading
import time
from typing import Optional

import websocket

from pymaker.numeric import Wad


GDAX_WS_URL = "wss://ws-feed.pro.coinbase.com"


class GdaxPriceClient:
    logger = logging.getLogger()

    def __init__(self, ws_url: str, product_id: str, expiry: int):
        assert(isinstance(ws_url, str))
        assert(isinstance(product_id, str))
        assert(isinstance(expiry, int))

        self.ws_url = ws_url
        self.product_id = product_id
        self.expiry = expiry
        self._last_price = None
        self._last_timestamp = 0
        self._expired = True
        threading.Thread(target=self._background_run, daemon=True).start()

    def _background_run(self):
        while True:
            ws = websocket.WebSocketApp(url=self.ws_url,
                                        on_message=self._on_message,
                                        on_error=self._on_error,
                                        on_open=self._on_open,
                                        on_close=self._on_close)
            ws.run_forever(ping_interval=15, ping_timeout=10)
            time.sleep(1)

    def _on_open(self, ws):
        self.logger.info(f"GDAX {self.product_id} WebSocket connected")
        ws.send("""{
            "type": "subscribe",
            "channels": [
                { "name": "ticker", "product_ids": ["%s"] },
                { "name": "heartbeat", "product_ids": ["%s"] }
            ]}""" % (self.product_id, self.product_id))

    def _on_close(self, ws):
        self.logger.info(f"GDAX {self.product_id} WebSocket disconnected")

    def _on_message(self, ws, message):
        print(message)
        try:
            message_obj = json.loads(message)
            if message_obj['type'] == 'subscriptions':
                pass
            elif message_obj['type'] == 'ticker':
                self._process_ticker(message_obj)
            elif message_obj['type'] == 'heartbeat':
                self._process_heartbeat()
            else:
                self.logger.warning(f"GDAX {self.product_id} WebSocket received unknown message type: '{message}'")
        except:
            self.logger.warning(f"GDAX {self.product_id} WebSocket received invalid message: '{message}'")

    def _on_error(self, ws, error):
        self.logger.info(f"GDAX {self.product_id} WebSocket error: '{error}'")

    def get_price(self) -> Optional[Wad]:
        if time.time() - self._last_timestamp > self.expiry:
            if not self._expired:
                self.logger.warning(f"Price feed from GDAX ({self.product_id}) has expired")
                self._expired = True

            return None

        else:
            value = self._last_price

            return value

    def _process_ticker(self, message_obj):
        self._last_price = Wad.from_number(message_obj['price'])
        self._last_timestamp = time.time()

        self.logger.debug(f"Price feed from GDAX is {self._last_price} ({self.product_id})")

        if self._expired:
            self.logger.info(f"Price feed from GDAX ({self.product_id}) became available")
            self._expired = False

    def _process_heartbeat(self):
        self._last_timestamp = time.time()
