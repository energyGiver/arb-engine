"""Base adapter interface for exchange connections."""

import asyncio
from abc import ABC, abstractmethod
from datetime import datetime
from typing import List, Optional

from arb_engine.core.errors import ExchangeError
from arb_engine.core.logging import get_logger
from arb_engine.core.types import Exchange, Symbol
from arb_engine.domain.models import FundingRate, OrderbookL5, PriceTick

logger = get_logger(__name__)


class BaseAdapter(ABC):
    """
    Base class for all exchange adapters.

    Each exchange (CEX or PerpDEX) must implement this interface.
    """

    def __init__(
        self,
        exchange_id: Exchange,
        symbols: List[Symbol],
        ws_endpoint: str,
        rest_endpoint: Optional[str] = None,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
    ):
        """
        Initialize adapter.

        Args:
            exchange_id: Exchange identifier (e.g., "binance")
            symbols: List of symbols to track
            ws_endpoint: WebSocket endpoint URL
            rest_endpoint: Optional REST API endpoint
            api_key: Optional API key
            api_secret: Optional API secret
        """
        self.exchange_id = exchange_id
        self.symbols = symbols
        self.ws_endpoint = ws_endpoint
        self.rest_endpoint = rest_endpoint
        self.api_key = api_key
        self.api_secret = api_secret

        self.is_connected = False
        self.ws_connection = None

        # Latest data cache
        self._latest_ticks: dict[Symbol, PriceTick] = {}
        self._latest_orderbooks: dict[Symbol, OrderbookL5] = {}
        self._latest_funding: dict[Symbol, FundingRate] = {}

        self.logger = get_logger(f"{__name__}.{exchange_id}")

    @abstractmethod
    async def connect(self) -> None:
        """
        Establish WebSocket connection.

        Must be implemented by each exchange adapter.
        """
        pass

    @abstractmethod
    async def disconnect(self) -> None:
        """
        Close WebSocket connection gracefully.

        Must be implemented by each exchange adapter.
        """
        pass

    @abstractmethod
    async def subscribe_tickers(self, symbols: List[Symbol]) -> None:
        """
        Subscribe to ticker/price updates.

        Args:
            symbols: List of symbols to subscribe
        """
        pass

    @abstractmethod
    async def subscribe_orderbook(self, symbols: List[Symbol], depth: int = 5) -> None:
        """
        Subscribe to orderbook updates.

        Args:
            symbols: List of symbols to subscribe
            depth: Orderbook depth (default 5)
        """
        pass

    @abstractmethod
    async def handle_message(self, message: dict) -> None:
        """
        Handle incoming WebSocket message.

        This is where exchange-specific message parsing happens.

        Args:
            message: Raw message from WebSocket
        """
        pass

    async def start(self) -> None:
        """Start the adapter (connect and subscribe)."""
        try:
            await self.connect()
            await self.subscribe_tickers(self.symbols)
            await self.subscribe_orderbook(self.symbols)
            self.logger.info("adapter_started", symbols=self.symbols)
        except Exception as e:
            self.logger.error("adapter_start_failed", error=str(e))
            raise ExchangeError(f"Failed to start {self.exchange_id} adapter: {e}")

    async def stop(self) -> None:
        """Stop the adapter (disconnect)."""
        try:
            await self.disconnect()
            self.logger.info("adapter_stopped")
        except Exception as e:
            self.logger.error("adapter_stop_failed", error=str(e))

    def get_latest_tick(self, symbol: Symbol) -> Optional[PriceTick]:
        """Get latest price tick for symbol."""
        return self._latest_ticks.get(symbol)

    def get_latest_orderbook(self, symbol: Symbol) -> Optional[OrderbookL5]:
        """Get latest orderbook for symbol."""
        return self._latest_orderbooks.get(symbol)

    def get_latest_funding(self, symbol: Symbol) -> Optional[FundingRate]:
        """Get latest funding rate for symbol."""
        return self._latest_funding.get(symbol)

    def update_tick(self, tick: PriceTick) -> None:
        """Update latest tick cache."""
        self._latest_ticks[tick.symbol] = tick

    def update_orderbook(self, orderbook: OrderbookL5) -> None:
        """Update latest orderbook cache."""
        self._latest_orderbooks[orderbook.symbol] = orderbook

    def update_funding(self, funding: FundingRate) -> None:
        """Update latest funding rate cache."""
        self._latest_funding[funding.symbol] = funding

    async def run_forever(self) -> None:
        """
        Run the adapter in a loop, handling reconnections.

        This is typically run as an asyncio task.
        """
        max_retries = 5
        retry_delay = 1  # Start with 1 second

        for attempt in range(max_retries):
            try:
                await self.start()

                # Keep running until disconnected
                while self.is_connected:
                    await asyncio.sleep(1)

                self.logger.warning("adapter_disconnected", attempt=attempt + 1)

            except Exception as e:
                self.logger.error(
                    "adapter_error",
                    error=str(e),
                    attempt=attempt + 1,
                    retry_delay=retry_delay,
                )

                if attempt < max_retries - 1:
                    await asyncio.sleep(retry_delay)
                    retry_delay = min(retry_delay * 2, 60)  # Exponential backoff, max 60s
                else:
                    self.logger.error("adapter_max_retries_exceeded")
                    raise

    def __repr__(self) -> str:
        """String representation."""
        return f"<{self.__class__.__name__}(exchange={self.exchange_id}, symbols={self.symbols})>"
