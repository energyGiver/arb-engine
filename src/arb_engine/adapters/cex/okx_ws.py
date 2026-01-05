"""OKX WebSocket adapter."""

import json
from decimal import Decimal
from typing import List

import websockets

from arb_engine.adapters.common.base import BaseAdapter
from arb_engine.adapters.common.symbol_map import get_symbol_mapper
from arb_engine.core.clock import get_clock
from arb_engine.core.types import Symbol
from arb_engine.domain.models import OrderbookL5, OrderbookLevel, PriceTick


class OKXAdapter(BaseAdapter):
    """
    OKX WebSocket adapter.

    API Docs: https://www.okx.com/docs-v5/en/#websocket-api-public-channel-tickers-channel
    """

    def __init__(self, symbols: List[Symbol], **kwargs):
        """Initialize OKX adapter."""
        super().__init__(
            exchange_id="okx",
            symbols=symbols,
            ws_endpoint="wss://ws.okx.com:8443/ws/v5/public",
            rest_endpoint="https://www.okx.com",
            **kwargs,
        )
        self.symbol_mapper = get_symbol_mapper()
        self.clock = get_clock()

    def _to_okx_symbol(self, symbol: Symbol) -> str:
        """Convert standard symbol to OKX format (e.g., BTC/USDT -> BTC-USDT)."""
        return symbol.replace("/", "-")

    async def connect(self) -> None:
        """Establish WebSocket connection."""
        self.logger.info("connecting", url=self.ws_endpoint)
        self.ws_connection = await websockets.connect(self.ws_endpoint)
        self.is_connected = True
        self.logger.info("connected")

        # Start message handler
        import asyncio

        asyncio.create_task(self._message_loop())

    async def disconnect(self) -> None:
        """Close WebSocket connection."""
        if self.ws_connection:
            await self.ws_connection.close()
            self.is_connected = False

    async def subscribe_tickers(self, symbols: List[Symbol]) -> None:
        """Subscribe to ticker updates."""
        args = []
        for symbol in symbols:
            okx_symbol = self._to_okx_symbol(symbol)
            args.append({"channel": "tickers", "instId": okx_symbol})

        sub_message = {"op": "subscribe", "args": args}
        await self.ws_connection.send(json.dumps(sub_message))
        self.logger.debug("subscribed_tickers", symbols=symbols)

    async def subscribe_orderbook(self, symbols: List[Symbol], depth: int = 5) -> None:
        """Subscribe to orderbook updates."""
        args = []
        for symbol in symbols:
            okx_symbol = self._to_okx_symbol(symbol)
            # OKX books5 gives top 5 levels
            args.append({"channel": "books5", "instId": okx_symbol})

        sub_message = {"op": "subscribe", "args": args}
        await self.ws_connection.send(json.dumps(sub_message))
        self.logger.debug("subscribed_orderbooks", symbols=symbols)

    async def _message_loop(self) -> None:
        """Main message processing loop."""
        try:
            async for message in self.ws_connection:
                data = json.loads(message)
                await self.handle_message(data)
        except websockets.exceptions.ConnectionClosed:
            self.logger.warning("connection_closed")
            self.is_connected = False
        except Exception as e:
            self.logger.error("message_loop_error", error=str(e))
            self.is_connected = False

    async def handle_message(self, message: dict) -> None:
        """
        Handle incoming WebSocket message from OKX.

        Message types:
        - Subscription response: {"event": "subscribe", "arg": {...}}
        - Data update: {"arg": {"channel": "tickers", "instId": "BTC-USDT"}, "data": [...]}
        """
        # Skip subscription confirmations and events
        if "event" in message:
            return

        if "arg" not in message or "data" not in message:
            return

        channel = message["arg"].get("channel")
        data_list = message["data"]

        if not data_list:
            return

        if channel == "tickers":
            for data in data_list:
                await self._handle_ticker(data)
        elif channel == "books5":
            for data in data_list:
                await self._handle_orderbook(data)

    async def _handle_ticker(self, data: dict) -> None:
        """
        Handle ticker update.

        Example data:
        {
            "instId": "BTC-USDT",
            "last": "50000.00",
            "vol24h": "1000.0",
            "ts": "1234567890000"
        }
        """
        try:
            okx_symbol = data["instId"]
            standard_symbol = self.symbol_mapper.normalize(okx_symbol)

            tick = PriceTick(
                exchange=self.exchange_id,
                symbol=standard_symbol,
                price=Decimal(data["last"]),
                volume_24h=Decimal(data.get("vol24h", "0")),
                exchange_timestamp=self.clock.from_timestamp_ms(int(data.get("ts", 0))),
                received_timestamp=self.clock.now(),
            )

            self.update_tick(tick)
            self.logger.debug(
                "tick_received",
                symbol=standard_symbol,
                price=float(tick.price),
            )

        except Exception as e:
            self.logger.error("ticker_parse_error", error=str(e), data=data)

    async def _handle_orderbook(self, data: dict) -> None:
        """
        Handle orderbook depth update.

        Example data:
        {
            "instId": "BTC-USDT",
            "bids": [["50000.00", "1.0", "0", "1"], ...],
            "asks": [["50001.00", "1.0", "0", "1"], ...],
            "ts": "1234567890000"
        }
        """
        try:
            okx_symbol = data["instId"]
            standard_symbol = self.symbol_mapper.normalize(okx_symbol)

            # OKX orderbook format: [price, size, liquidated_orders, num_orders]
            # We only need price and size (first 2 elements)
            bids = [
                OrderbookLevel(price=Decimal(level[0]), size=Decimal(level[1]))
                for level in data.get("bids", [])[:5]
            ]
            asks = [
                OrderbookLevel(price=Decimal(level[0]), size=Decimal(level[1]))
                for level in data.get("asks", [])[:5]
            ]

            orderbook = OrderbookL5(
                exchange=self.exchange_id,
                symbol=standard_symbol,
                bids=bids,
                asks=asks,
                exchange_timestamp=self.clock.from_timestamp_ms(int(data.get("ts", 0))),
                received_timestamp=self.clock.now(),
            )

            self.update_orderbook(orderbook)
            self.logger.debug(
                "orderbook_received",
                symbol=standard_symbol,
                best_bid=float(orderbook.best_bid) if orderbook.best_bid else None,
                best_ask=float(orderbook.best_ask) if orderbook.best_ask else None,
            )

        except Exception as e:
            self.logger.error("orderbook_parse_error", error=str(e), data=data)
