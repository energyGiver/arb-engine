"""Bybit WebSocket adapter."""

import json
from decimal import Decimal
from typing import List

import websockets

from arb_engine.adapters.common.base import BaseAdapter
from arb_engine.adapters.common.symbol_map import get_symbol_mapper
from arb_engine.core.clock import get_clock
from arb_engine.core.types import Symbol
from arb_engine.domain.models import OrderbookL5, OrderbookLevel, PriceTick


class BybitAdapter(BaseAdapter):
    """
    Bybit WebSocket adapter.

    API Docs: https://bybit-exchange.github.io/docs/v5/websocket/public/ticker
    """

    def __init__(self, symbols: List[Symbol], **kwargs):
        """Initialize Bybit adapter."""
        super().__init__(
            exchange_id="bybit",
            symbols=symbols,
            ws_endpoint="wss://stream.bybit.com/v5/public/linear",
            rest_endpoint="https://api.bybit.com",
            **kwargs,
        )
        self.symbol_mapper = get_symbol_mapper()
        self.clock = get_clock()

    def _to_bybit_symbol(self, symbol: Symbol) -> str:
        """Convert standard symbol to Bybit format (e.g., BTC/USDT -> BTCUSDT)."""
        return symbol.replace("/", "")

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
        for symbol in symbols:
            bybit_symbol = self._to_bybit_symbol(symbol)
            sub_message = {
                "op": "subscribe",
                "args": [f"tickers.{bybit_symbol}"],
            }
            await self.ws_connection.send(json.dumps(sub_message))
            self.logger.debug("subscribed_ticker", symbol=symbol)

    async def subscribe_orderbook(self, symbols: List[Symbol], depth: int = 5) -> None:
        """Subscribe to orderbook updates."""
        for symbol in symbols:
            bybit_symbol = self._to_bybit_symbol(symbol)
            # Bybit orderbook depth levels: 1, 50, 200, 500
            # We'll use 50 and take top 5
            sub_message = {
                "op": "subscribe",
                "args": [f"orderbook.50.{bybit_symbol}"],
            }
            await self.ws_connection.send(json.dumps(sub_message))
            self.logger.debug("subscribed_orderbook", symbol=symbol)

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
        Handle incoming WebSocket message from Bybit.

        Message types:
        - Subscription response: {"success": true, "op": "subscribe"}
        - Ticker update: {"topic": "tickers.BTCUSDT", "data": {...}}
        - Orderbook update: {"topic": "orderbook.50.BTCUSDT", "data": {...}}
        """
        # Skip subscription confirmations and pings
        if "op" in message or "ret_msg" in message:
            return

        if "topic" not in message or "data" not in message:
            return

        topic = message["topic"]
        data = message["data"]

        if topic.startswith("tickers."):
            await self._handle_ticker(data)
        elif topic.startswith("orderbook."):
            await self._handle_orderbook(data)

    async def _handle_ticker(self, data: dict) -> None:
        """
        Handle ticker update.

        Example data:
        {
            "symbol": "BTCUSDT",
            "lastPrice": "50000.00",
            "volume24h": "1000.0",
            "ts": 1234567890000
        }
        """
        try:
            bybit_symbol = data["symbol"]
            standard_symbol = self.symbol_mapper.normalize(bybit_symbol)

            tick = PriceTick(
                exchange=self.exchange_id,
                symbol=standard_symbol,
                price=Decimal(data["lastPrice"]),
                volume_24h=Decimal(data.get("volume24h", "0")),
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
            "s": "BTCUSDT",
            "b": [["50000.00", "1.0"], ["49999.00", "2.0"]],
            "a": [["50001.00", "1.0"], ["50002.00", "2.0"]],
            "ts": 1234567890000
        }
        """
        try:
            bybit_symbol = data["s"]
            standard_symbol = self.symbol_mapper.normalize(bybit_symbol)

            # Parse bids and asks (top 5)
            bids = [
                OrderbookLevel(price=Decimal(price), size=Decimal(size))
                for price, size in data.get("b", [])[:5]
            ]
            asks = [
                OrderbookLevel(price=Decimal(price), size=Decimal(size))
                for price, size in data.get("a", [])[:5]
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
