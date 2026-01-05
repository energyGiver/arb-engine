"""Hyperliquid WebSocket adapter for perpetual futures."""

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


class HyperliquidAdapter(BaseAdapter):
    """
    Hyperliquid WebSocket adapter.

    API Docs: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket
    WebSocket: wss://api.hyperliquid.xyz/ws
    """

    # Symbol to coin mapping (standard symbol -> Hyperliquid coin)
    SYMBOL_TO_COIN: Dict[str, str] = {
        "BTC-PERP": "BTC",
        "ETH-PERP": "ETH",
    }

    def __init__(
        self,
        symbols: List[Symbol],
        wallet_address: Optional[str] = None,
        wallet_private_key: Optional[str] = None,
        **kwargs,
    ):
        """
        Initialize Hyperliquid adapter.

        Args:
            symbols: List of symbols to track (e.g., ["BTC-PERP", "ETH-PERP"])
            wallet_address: Wallet address (for authenticated endpoints)
            wallet_private_key: Wallet private key (for trading)
        """
        super().__init__(
            exchange_id="hyperliquid",
            symbols=symbols,
            ws_endpoint="wss://api.hyperliquid.xyz/ws",
            rest_endpoint="https://api.hyperliquid.xyz",
            **kwargs,
        )
        self.symbol_mapper = get_symbol_mapper()
        self.clock = get_clock()

        # Authentication (for private endpoints, not needed for public market data)
        self.wallet_address = wallet_address
        self.wallet_private_key = wallet_private_key

        # Track channel subscriptions
        self._subscribed_channels: set = set()

    def _symbol_to_coin(self, symbol: Symbol) -> str:
        """
        Convert standard symbol to Hyperliquid coin.

        Args:
            symbol: Standard symbol (e.g., "BTC-PERP")

        Returns:
            Hyperliquid coin (e.g., "BTC")
        """
        if symbol in self.SYMBOL_TO_COIN:
            return self.SYMBOL_TO_COIN[symbol]

        # Fallback: extract base currency
        if symbol.endswith("-PERP"):
            return symbol.replace("-PERP", "")

        raise ValueError(f"Cannot convert symbol {symbol} to Hyperliquid coin")

    def _coin_to_symbol(self, coin: str) -> Symbol:
        """
        Convert Hyperliquid coin to standard symbol.

        Args:
            coin: Hyperliquid coin (e.g., "BTC")

        Returns:
            Standard symbol (e.g., "BTC-PERP")
        """
        # Reverse lookup
        for symbol, c in self.SYMBOL_TO_COIN.items():
            if c == coin:
                return symbol

        # Fallback
        return f"{coin}-PERP"

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
        Subscribe to ticker updates.

        Hyperliquid uses 'allMids' for ticker (mid price) data.
        We'll also subscribe to trades to get last price.
        """
        # Subscribe to allMids (gets mid prices for all coins)
        if "allMids" not in self._subscribed_channels:
            sub_message = {"method": "subscribe", "subscription": {"type": "allMids"}}
            await self.ws_connection.send(json.dumps(sub_message))
            self._subscribed_channels.add("allMids")
            self.logger.debug("subscribed_allMids")

        # Subscribe to trades for each symbol
        for symbol in symbols:
            try:
                coin = self._symbol_to_coin(symbol)
                channel_key = f"trades:{coin}"

                if channel_key not in self._subscribed_channels:
                    sub_message = {
                        "method": "subscribe",
                        "subscription": {"type": "trades", "coin": coin},
                    }
                    await self.ws_connection.send(json.dumps(sub_message))
                    self._subscribed_channels.add(channel_key)
                    self.logger.debug("subscribed_trades", symbol=symbol, coin=coin)
            except Exception as e:
                self.logger.error("subscribe_ticker_failed", symbol=symbol, error=str(e))

    async def subscribe_orderbook(self, symbols: List[Symbol], depth: int = 5) -> None:
        """
        Subscribe to orderbook updates.

        Args:
            symbols: List of symbols to subscribe
            depth: Orderbook depth (Hyperliquid sends full book, we take top 5)
        """
        for symbol in symbols:
            try:
                coin = self._symbol_to_coin(symbol)
                channel_key = f"l2Book:{coin}"

                if channel_key not in self._subscribed_channels:
                    sub_message = {
                        "method": "subscribe",
                        "subscription": {"type": "l2Book", "coin": coin},
                    }
                    await self.ws_connection.send(json.dumps(sub_message))
                    self._subscribed_channels.add(channel_key)
                    self.logger.debug("subscribed_l2Book", symbol=symbol, coin=coin)
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
        Handle incoming WebSocket message from Hyperliquid.

        Message types:
        - Subscription response: {"method": "subscriptionResponse", ...}
        - allMids: {"channel": "allMids", "data": {"mids": {...}}}
        - trades: {"channel": "trades", "data": [{...}]}
        - l2Book: {"channel": "l2Book", "data": {...}}
        """
        channel = message.get("channel")

        # Skip subscription responses
        if message.get("method") == "subscriptionResponse":
            self.logger.debug("subscription_confirmed", subscription=message.get("subscription"))
            return

        # Handle different channel types
        if channel == "allMids":
            await self._handle_all_mids(message)
        elif channel == "trades":
            await self._handle_trades(message)
        elif channel == "l2Book":
            await self._handle_l2_book(message)

    async def _handle_all_mids(self, message: dict) -> None:
        """
        Handle allMids (ticker mid price) update.

        Example message:
        {
            "channel": "allMids",
            "data": {
                "mids": {
                    "BTC": "50000.0",
                    "ETH": "3000.0"
                }
            }
        }
        """
        try:
            data = message.get("data", {})
            mids = data.get("mids", {})

            for coin, mid_price in mids.items():
                try:
                    symbol = self._coin_to_symbol(coin)

                    # Only process symbols we're tracking
                    if symbol not in self.symbols:
                        continue

                    tick = PriceTick(
                        exchange=self.exchange_id,
                        symbol=symbol,
                        price=Decimal(str(mid_price)),
                        volume_24h=None,  # Not provided in allMids
                        exchange_timestamp=self.clock.now(),  # No timestamp in allMids
                        received_timestamp=self.clock.now(),
                    )

                    self.update_tick(tick)
                    self.logger.debug("tick_received", symbol=symbol, price=float(tick.price))

                except Exception as e:
                    self.logger.error("mid_price_parse_error", coin=coin, error=str(e))

        except Exception as e:
            self.logger.error("allmids_parse_error", error=str(e), message=message)

    async def _handle_trades(self, message: dict) -> None:
        """
        Handle trades update.

        Example message:
        {
            "channel": "trades",
            "data": [
                {
                    "coin": "BTC",
                    "side": "B",
                    "px": "50000.5",
                    "sz": "0.1",
                    "time": 1234567890000,
                    "hash": "0x...",
                    "tid": 123
                }
            ]
        }
        """
        try:
            trades = message.get("data", [])

            for trade in trades:
                coin = trade.get("coin")
                symbol = self._coin_to_symbol(coin)

                # Only process symbols we're tracking
                if symbol not in self.symbols:
                    continue

                price = Decimal(str(trade.get("px", "0")))
                timestamp_ms = trade.get("time", 0)

                tick = PriceTick(
                    exchange=self.exchange_id,
                    symbol=symbol,
                    price=price,
                    volume_24h=None,  # Not provided in individual trades
                    exchange_timestamp=self.clock.from_timestamp_ms(timestamp_ms),
                    received_timestamp=self.clock.now(),
                )

                self.update_tick(tick)
                self.logger.debug("tick_received", symbol=symbol, price=float(price))

        except Exception as e:
            self.logger.error("trades_parse_error", error=str(e), message=message)

    async def _handle_l2_book(self, message: dict) -> None:
        """
        Handle L2 orderbook update.

        Example message:
        {
            "channel": "l2Book",
            "data": {
                "coin": "BTC",
                "levels": [
                    [
                        {"px": "50000.0", "sz": "1.0", "n": 1},
                        {"px": "49999.0", "sz": "2.0", "n": 2}
                    ],
                    [
                        {"px": "50001.0", "sz": "1.5", "n": 1},
                        {"px": "50002.0", "sz": "2.5", "n": 2}
                    ]
                ],
                "time": 1234567890000
            }
        }
        """
        try:
            data = message.get("data", {})
            coin = data.get("coin")
            symbol = self._coin_to_symbol(coin)

            # Only process symbols we're tracking
            if symbol not in self.symbols:
                return

            levels = data.get("levels", [[], []])
            bids_data = levels[0] if len(levels) > 0 else []
            asks_data = levels[1] if len(levels) > 1 else []

            # Parse bids and asks (top 5)
            bids = [
                OrderbookLevel(
                    price=Decimal(str(level.get("px", "0"))),
                    size=Decimal(str(level.get("sz", "0"))),
                )
                for level in bids_data[:5]
            ]

            asks = [
                OrderbookLevel(
                    price=Decimal(str(level.get("px", "0"))),
                    size=Decimal(str(level.get("sz", "0"))),
                )
                for level in asks_data[:5]
            ]

            timestamp_ms = data.get("time", 0)

            orderbook = OrderbookL5(
                exchange=self.exchange_id,
                symbol=symbol,
                bids=bids,
                asks=asks,
                exchange_timestamp=self.clock.from_timestamp_ms(timestamp_ms),
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
            self.logger.error("l2book_parse_error", error=str(e), message=message)

    async def subscribe_funding(self, symbols: List[Symbol]) -> None:
        """
        Subscribe to funding rate updates.

        Note: Funding rates might need to be fetched via REST API.
        This is a placeholder for future implementation.
        """
        # TODO: Implement funding rate subscription if available via WebSocket
        # For now, funding rates might need to be fetched via REST API
        self.logger.warning(
            "funding_rate_subscription_not_implemented",
            note="Funding rates may need to be fetched via REST API",
        )
