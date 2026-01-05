"""Domain models using Pydantic."""

from datetime import datetime
from decimal import Decimal
from typing import List, Optional

from pydantic import BaseModel, Field

from arb_engine.core.types import Exchange, OrderSide, SignalStatus, Symbol


class OrderbookLevel(BaseModel):
    """Single orderbook level (price and size)."""

    price: Decimal
    size: Decimal


class PriceTick(BaseModel):
    """Price tick data from exchange."""

    exchange: Exchange
    symbol: Symbol
    price: Decimal
    volume_24h: Optional[Decimal] = None
    exchange_timestamp: datetime  # Exchange's timestamp
    received_timestamp: datetime  # When we received it (local time)

    class Config:
        frozen = True  # Immutable


class OrderbookL5(BaseModel):
    """Top 5 levels of orderbook (L5)."""

    exchange: Exchange
    symbol: Symbol
    bids: List[OrderbookLevel] = Field(max_length=5)  # Top 5 bids
    asks: List[OrderbookLevel] = Field(max_length=5)  # Top 5 asks
    exchange_timestamp: datetime
    received_timestamp: datetime

    @property
    def best_bid(self) -> Optional[Decimal]:
        """Get best bid price."""
        return self.bids[0].price if self.bids else None

    @property
    def best_ask(self) -> Optional[Decimal]:
        """Get best ask price."""
        return self.asks[0].price if self.asks else None

    @property
    def mid_price(self) -> Optional[Decimal]:
        """Calculate mid price."""
        if self.best_bid and self.best_ask:
            return (self.best_bid + self.best_ask) / Decimal("2")
        return None

    @property
    def spread(self) -> Optional[Decimal]:
        """Calculate spread."""
        if self.best_bid and self.best_ask:
            return self.best_ask - self.best_bid
        return None

    @property
    def spread_bps(self) -> Optional[Decimal]:
        """Calculate spread in basis points."""
        if self.spread and self.mid_price and self.mid_price > 0:
            return (self.spread / self.mid_price) * Decimal("10000")
        return None

    class Config:
        frozen = True


class FundingRate(BaseModel):
    """Funding rate for perpetual contracts."""

    exchange: Exchange
    symbol: Symbol
    funding_rate: Decimal  # Current funding rate
    next_funding_time: Optional[datetime] = None
    predicted_rate: Optional[Decimal] = None
    exchange_timestamp: datetime
    received_timestamp: datetime

    class Config:
        frozen = True


class Fee(BaseModel):
    """Trading fee structure."""

    exchange: Exchange
    maker_fee: Decimal  # Maker fee (fraction, e.g., 0.0001 = 0.01%)
    taker_fee: Decimal  # Taker fee (fraction)
    withdrawal_fee: Optional[Decimal] = None


class ArbSignal(BaseModel):
    """Arbitrage opportunity signal."""

    signal_id: str  # Unique identifier
    timestamp: datetime
    symbol: Symbol  # Standardized symbol

    # Exchanges involved
    buy_exchange: Exchange
    sell_exchange: Exchange

    # Prices
    buy_price: Decimal  # Effective buy price (after slippage estimate)
    sell_price: Decimal  # Effective sell price (after slippage estimate)

    # Spread
    gross_spread: Decimal  # sell - buy
    gross_spread_bps: Decimal  # In basis points

    # After fees
    net_spread: Decimal  # After taker fees
    net_spread_bps: Decimal

    # Execution estimates
    estimated_size: Decimal  # Size we could trade (based on L5 depth)
    estimated_slippage: Decimal  # Estimated slippage

    # Fees
    buy_fee: Decimal
    sell_fee: Decimal

    # Funding (if applicable)
    funding_cost: Optional[Decimal] = None

    # Status
    status: SignalStatus = SignalStatus.DETECTED

    # Convergence tracking
    converged_at: Optional[datetime] = None
    convergence_time_seconds: Optional[float] = None
    final_spread_bps: Optional[Decimal] = None

    class Config:
        frozen = False  # Mutable for status updates


class MarketSnapshot(BaseModel):
    """Complete market snapshot at a point in time."""

    timestamp: datetime
    symbol: Symbol
    price_ticks: List[PriceTick]
    orderbooks: List[OrderbookL5]
    funding_rates: List[FundingRate]

    class Config:
        frozen = True
