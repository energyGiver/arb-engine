"""Binance WebSocket adapter."""

import json
from decimal import Decimal
from datetime import datetime
from typing import List

import websockets

from arb_engine.adapters.common.base import BaseAdapter
from arb_engine.adapters.common.symbol_map import get_symbol_mapper
from arb_engine.core.clock import get_clock
from arb_engine.core.types import Symbol
from arb_engine.domain.models import OrderbookL5, OrderbookLevel, PriceTick


class BinanceAdapter(BaseAdapter):
    """
    Binance WebSocket adapter.

    API Docs: https://binance-docs.github.io/apidocs/spot/en/#websocket-market-streams
    """

    def __init__(self, symbols: List[Symbol], **kwargs):
        """Initialize Binance adapter."""
        super().__init__(
            exchange_id="binance",
            symbols=symbols,
            ws_endpoint="wss://stream.binance.com:9443/ws",
            rest_endpoint="https://api.binance.com",
            **kwargs,
        )
        self.symbol_mapper = get_symbol_mapper()
        self.clock = get_clock()

    def _to_binance_symbol(self, symbol: Symbol) -> str:
        """Convert standard symbol to Binance format (e.g., BTC/USDT -> btcusdt)."""
        return symbol.replace("/", "").lower()

    async def connect(self) -> None:
        """Establish WebSocket connection."""
        # Build stream names for all symbols
        streams = []
        for symbol in self.symbols:
            binance_symbol = self._to_binance_symbol(symbol)
            # Subscribe to ticker and orderbook depth
            streams.append(f"{binance_symbol}@ticker")
            streams.append(f"{binance_symbol}@depth5@100ms")

        # Binance combined stream format
        stream_path = "/stream?streams=" + "/".join(streams)
        ws_url = self.ws_endpoint.replace("/ws", stream_path)

        self.logger.info("connecting", url=ws_url)
        self.ws_connection = await websockets.connect(ws_url)
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
        """Subscribe to tickers (handled in connect for Binance)."""
        pass  # Subscription happens in connect()

    async def subscribe_orderbook(self, symbols: List[Symbol], depth: int = 5) -> None:
        """Subscribe to orderbook (handled in connect for Binance)."""
        pass  # Subscription happens in connect()

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
        Handle incoming WebSocket message from Binance.

        Message format: {"stream": "btcusdt@ticker", "data": {...}}
        """
        if "stream" not in message or "data" not in message:
            return

        stream = message["stream"]
        data = message["data"]

        if "@ticker" in stream:
            await self._handle_ticker(data)
        elif "@depth5" in stream:
            # Extract symbol from stream name (e.g., "btcusdt@depth5@100ms" -> "btcusdt")
            symbol_part = stream.split("@")[0]
            await self._handle_orderbook(data, symbol_part)

    async def _handle_ticker(self, data: dict) -> None:
        """
        Handle ticker update.

        Example data:
        {
            "s": "BTCUSDT",
            "c": "50000.00",  # Last price
            "v": "1000.0",    # 24h volume
            "E": 1234567890   # Event time
        }
        """
        try:
            binance_symbol = data["s"]
            standard_symbol = self.symbol_mapper.normalize(binance_symbol)

            tick = PriceTick(
                exchange=self.exchange_id,
                symbol=standard_symbol,
                price=Decimal(data["c"]),
                volume_24h=Decimal(data.get("v", "0")),
                exchange_timestamp=self.clock.from_timestamp_ms(data["E"]),
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

    async def _handle_orderbook(self, data: dict, binance_symbol: str) -> None:
        """
        Handle orderbook depth update.

        Args:
            data: Orderbook data
            binance_symbol: Symbol from stream name (e.g., "btcusdt")

        Example data:
        {
            "lastUpdateId": 12345,
            "bids": [["50000.00", "1.0"], ["49999.00", "2.0"]],
            "asks": [["50001.00", "1.0"], ["50002.00", "2.0"]]
        }
        """
        try:
            standard_symbol = self.symbol_mapper.normalize(binance_symbol.upper())

            # Parse bids and asks (top 5)
            bids = [
                OrderbookLevel(price=Decimal(price), size=Decimal(size))
                for price, size in data["bids"][:5]
            ]
            asks = [
                OrderbookLevel(price=Decimal(price), size=Decimal(size))
                for price, size in data["asks"][:5]
            ]

            # Binance depth stream doesn't include event time, use current time
            orderbook = OrderbookL5(
                exchange=self.exchange_id,
                symbol=standard_symbol,
                bids=bids,
                asks=asks,
                exchange_timestamp=self.clock.now(),  # No timestamp in depth snapshot
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
