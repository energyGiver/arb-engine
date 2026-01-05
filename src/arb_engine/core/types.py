"""Common type definitions for arb-engine."""

from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Literal


class ExchangeType(str, Enum):
    """Exchange type classification."""

    CEX = "cex"
    PERP = "perp"


class OrderSide(str, Enum):
    """Order side."""

    BID = "bid"
    ASK = "ask"


class OrderType(str, Enum):
    """Order type."""

    LIMIT = "limit"
    MARKET = "market"


class PositionSide(str, Enum):
    """Position side for futures."""

    LONG = "long"
    SHORT = "short"


class SignalStatus(str, Enum):
    """Arbitrage signal status."""

    DETECTED = "detected"
    IN_PROGRESS = "in_progress"
    CONVERGED = "converged"
    FAILED = "failed"
    EXPIRED = "expired"


# Type aliases
Symbol = str  # Standardized symbol (e.g., "BTC/USDT", "ETH-PERP")
Exchange = str  # Exchange identifier (e.g., "binance", "hyperliquid")
Price = Decimal
Quantity = Decimal
Timestamp = datetime
