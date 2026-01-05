"""Lighter (zkLighter) WebSocket adapter for perpetual futures."""

import json
from decimal import Decimal
from typing import Dict, List, Optional

import websockets

from arb_engine.adapters.common.base import BaseAdapter
from arb_engine.adapters.common.symbol_map import get_symbol_mapper
from arb_engine.core.clock import get_clock
from arb_engine.core.logging import get_logger
from arb_engine.core.types import Symbol
from arb_engine.domain.models import FundingRate, OrderbookL5, OrderbookLevel, PriceTick

logger = get_logger(__name__)


class LighterAdapter(BaseAdapter):
    """
    Lighter (zkLighter) WebSocket adapter.

    API Docs: https://apidocs.lighter.xyz/docs/websocket-reference
    WebSocket: wss://mainnet.zklighter.elliot.ai/stream
    """

    # Market index mapping for Lighter
    MARKET_INDICES: Dict[str, int] = {
        "BTC-PERP": 0,
        "ETH-PERP": 1,
    }

    def __init__(
        self,
        symbols: List[Symbol],
        api_key_private_key: Optional[str] = None,
        account_index: Optional[int] = None,
        api_key_index: Optional[int] = None,
        **kwargs,
    ):
        """
        Initialize Lighter adapter.

        Args:
            symbols: List of symbols to track (e.g., ["BTC-PERP", "ETH-PERP"])
            api_key_private_key: Lighter API key private key (for authenticated endpoints)
            account_index: Account index
            api_key_index: API key index
        """
        super().__init__(
            exchange_id="lighter",
            symbols=symbols,
            ws_endpoint="wss://mainnet.zklighter.elliot.ai/stream",
            rest_endpoint="https://api.lighter.xyz",
            **kwargs,
        )
        self.symbol_mapper = get_symbol_mapper()
        self.clock = get_clock()

        # Authentication (for private endpoints, not needed for public market data)
        self.api_key_private_key = api_key_private_key
        self.account_index = account_index
        self.api_key_index = api_key_index

        # Track last trade price for each symbol (for ticker)
        self._last_trade_price: Dict[Symbol, Decimal] = {}

    def _get_market_index(self, symbol: Symbol) -> int:
        """
        Get Lighter market index for a symbol.

        Args:
            symbol: Standard symbol (e.g., "BTC-PERP")

        Returns:
            Market index (0 for BTC-PERP, 1 for ETH-PERP)

        Raises:
            ValueError: If symbol not supported
        """
        if symbol not in self.MARKET_INDICES:
            raise ValueError(f"Symbol {symbol} not supported by Lighter")
        return self.MARKET_INDICES[symbol]

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
            self.logger.info("disconnected")

    async def subscribe_tickers(self, symbols: List[Symbol]) -> None:
        """
        Subscribe to ticker/trade updates.

        Lighter uses trade channel for price updates.
        """
        for symbol in symbols:
            try:
                market_index = self._get_market_index(symbol)
                sub_message = {"type": "subscribe", "channel": f"trade/{market_index}"}
                await self.ws_connection.send(json.dumps(sub_message))
                self.logger.debug("subscribed_trade", symbol=symbol, market_index=market_index)
            except Exception as e:
                self.logger.error("subscribe_ticker_failed", symbol=symbol, error=str(e))

    async def subscribe_orderbook(self, symbols: List[Symbol], depth: int = 5) -> None:
        """
        Subscribe to orderbook updates.

        Args:
            symbols: List of symbols to subscribe
            depth: Orderbook depth (Lighter sends all levels, we take top 5)
        """
        for symbol in symbols:
            try:
                market_index = self._get_market_index(symbol)
                sub_message = {
                    "type": "subscribe",
                    "channel": f"order_book/{market_index}",
                }
                await self.ws_connection.send(json.dumps(sub_message))
                self.logger.debug(
                    "subscribed_orderbook", symbol=symbol, market_index=market_index
                )
            except Exception as e:
                self.logger.error("subscribe_orderbook_failed", symbol=symbol, error=str(e))

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
        Handle incoming WebSocket message from Lighter.

        Message types:
        - Connection: {"type": "connected", "session_id": "..."}
        - Subscription response: {"type": "subscribed/trade", "channel": "trade:0"}
        - Trade update: {"type": "update/trade", "channel": "trade:0", ...}
        - Orderbook update: {"type": "update/order_book" or "subscribed/order_book", "channel": "order_book:0", ...}
        """
        msg_type = message.get("type", "")
        channel = message.get("channel", "")

        # Skip connection messages
        if msg_type == "connected":
            self.logger.debug("connection_confirmed", session_id=message.get("session_id"))
            return

        # Skip subscription confirmations
        if msg_type.startswith("subscribed/"):
            self.logger.debug("subscription_confirmed", channel=channel, type=msg_type)
            return

        # Handle trade updates (channel: "trade:0", "trade:1")
        if channel.startswith("trade:"):
            await self._handle_trade(message)

        # Handle orderbook updates (channel: "order_book:0", "order_book:1")
        elif channel.startswith("order_book:"):
            await self._handle_orderbook(message)

    async def _handle_trade(self, message: dict) -> None:
        """
        Handle trade update.

        Example message:
        {
            "type": "trade",
            "channel": "trade/0",
            "data": {
                "price": "50000.50",
                "size": "0.1",
                "timestamp": 1234567890
            }
        }
        """
        try:
            channel = message.get("channel", "")
            market_index = int(channel.split(":")[-1])

            # Find symbol for this market index
            symbol = None
            for sym, idx in self.MARKET_INDICES.items():
                if idx == market_index:
                    symbol = sym
                    break

            if not symbol:
                return

            # Get trades array
            trades = message.get("trades", [])
            if not trades:
                return

            # Use most recent trade
            latest_trade = trades[-1]
            price = Decimal(str(latest_trade.get("price", "0")))
            timestamp = latest_trade.get("timestamp", 0)

            # Update last trade price cache
            self._last_trade_price[symbol] = price

            # Create price tick
            tick = PriceTick(
                exchange=self.exchange_id,
                symbol=symbol,
                price=price,
                volume_24h=None,  # Not provided in trade message
                exchange_timestamp=self.clock.from_timestamp_ms(timestamp),
                received_timestamp=self.clock.now(),
            )

            self.update_tick(tick)
            self.logger.debug("tick_received", symbol=symbol, price=float(price))

        except Exception as e:
            self.logger.error("trade_parse_error", error=str(e))

    async def _handle_orderbook(self, message: dict) -> None:
        """
        Handle orderbook update.

        Example message:
        {
            "type": "update/order_book" or "subscribed/order_book",
            "channel": "order_book:0",
            "order_book": {
                "code": "BTC",
                "asks": [
                    {"price": "50001.0", "size": "1.5"},
                    {"price": "50002.0", "size": "2.0"}
                ],
                "bids": [
                    {"price": "50000.0", "size": "1.0"},
                    {"price": "49999.0", "size": "2.5"}
                ],
                "offset": 123,
                "nonce": 456
            },
            "timestamp": 1234567890
        }
        """
        try:
            channel = message.get("channel", "")
            market_index = int(channel.split(":")[-1])

            # Find symbol for this market index
            symbol = None
            for sym, idx in self.MARKET_INDICES.items():
                if idx == market_index:
                    symbol = sym
                    break

            if not symbol:
                return

            order_book_data = message.get("order_book", {})

            # Parse bids and asks (top 5)
            bids = [
                OrderbookLevel(
                    price=Decimal(str(level.get("price", "0"))),
                    size=Decimal(str(level.get("size", "0"))),
                )
                for level in order_book_data.get("bids", [])[:5]
            ]

            asks = [
                OrderbookLevel(
                    price=Decimal(str(level.get("price", "0"))),
                    size=Decimal(str(level.get("size", "0"))),
                )
                for level in order_book_data.get("asks", [])[:5]
            ]

            # Bids should already be sorted descending, asks ascending
            # But ensure it for safety
            bids.sort(key=lambda x: x.price, reverse=True)
            asks.sort(key=lambda x: x.price)

            timestamp = message.get("timestamp", 0)

            orderbook = OrderbookL5(
                exchange=self.exchange_id,
                symbol=symbol,
                bids=bids,
                asks=asks,
                exchange_timestamp=self.clock.from_timestamp_ms(timestamp),
                received_timestamp=self.clock.now(),
            )

            self.update_orderbook(orderbook)
            self.logger.debug(
                "orderbook_received",
                symbol=symbol,
                best_bid=float(orderbook.best_bid) if orderbook.best_bid else None,
                best_ask=float(orderbook.best_ask) if orderbook.best_ask else None,
            )

        except Exception as e:
            self.logger.error("orderbook_parse_error", error=str(e))

    async def subscribe_funding(self, symbols: List[Symbol]) -> None:
        """
        Subscribe to funding rate updates.

        Note: Lighter funding rate API might be different from public WebSocket.
        This is a placeholder for future implementation.
        """
        # TODO: Implement funding rate subscription if available via WebSocket
        # For now, funding rates might need to be fetched via REST API
        self.logger.warning(
            "funding_rate_subscription_not_implemented",
            note="Funding rates may need to be fetched via REST API",
        )
