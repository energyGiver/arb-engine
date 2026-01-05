"""Custom exceptions for arb-engine."""


class ArbEngineError(Exception):
    """Base exception for arb-engine."""

    pass


class ConfigurationError(ArbEngineError):
    """Raised when configuration is invalid or missing."""

    pass


class ExchangeError(ArbEngineError):
    """Base exception for exchange-related errors."""

    pass


class ConnectionError(ExchangeError):
    """Raised when connection to exchange fails."""

    pass


class AuthenticationError(ExchangeError):
    """Raised when authentication fails."""

    pass


class RateLimitError(ExchangeError):
    """Raised when rate limit is exceeded."""

    pass


class DataError(ArbEngineError):
    """Raised when data is invalid or missing."""

    pass


class StorageError(ArbEngineError):
    """Raised when storage operation fails."""

    pass


class NormalizationError(ArbEngineError):
    """Raised when data normalization fails."""

    pass


class WebSocketError(ExchangeError):
    """Raised when WebSocket operation fails."""

    pass


class StaleDataError(DataError):
    """Raised when data is too old (stale)."""

    def __init__(self, exchange: str, symbol: str, last_update: float):
        self.exchange = exchange
        self.symbol = symbol
        self.last_update = last_update
        super().__init__(
            f"Stale data from {exchange} for {symbol}. Last update: {last_update}s ago"
        )
