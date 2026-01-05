"""Symbol mapping and normalization utilities."""

from typing import Dict, Optional

from arb_engine.core.config import get_config_loader
from arb_engine.core.errors import NormalizationError
from arb_engine.core.logging import get_logger
from arb_engine.core.types import Symbol

logger = get_logger(__name__)


class SymbolMapper:
    """
    Map exchange-specific symbols to standardized format.

    Standard formats:
    - Spot: "BTC/USDT", "ETH/USDT"
    - Perp: "BTC-PERP", "ETH-PERP"
    """

    def __init__(self):
        """Initialize symbol mapper with config."""
        self._mapping: Dict[str, Symbol] = {}
        self._reverse_mapping: Dict[Symbol, Dict[str, str]] = {}
        self._load_config()

    def _load_config(self) -> None:
        """Load symbol mappings from config."""
        try:
            config_loader = get_config_loader()
            exchanges_config = config_loader.load_exchanges()

            # Load global mappings
            if "symbol_mapping" in exchanges_config:
                self._mapping.update(exchanges_config["symbol_mapping"])

            logger.info("symbol_mapper_loaded", mapping_count=len(self._mapping))
        except Exception as e:
            logger.error("symbol_mapper_load_failed", error=str(e))
            # Initialize with minimal hardcoded mappings as fallback
            self._init_fallback_mappings()

    def _init_fallback_mappings(self) -> None:
        """Initialize fallback symbol mappings."""
        self._mapping = {
            # BTC variants
            "BTCUSDT": "BTC/USDT",
            "BTC-USDT": "BTC/USDT",
            "BTC/USDT": "BTC/USDT",
            "BTC-PERP": "BTC-PERP",
            "BTCPERP": "BTC-PERP",
            # ETH variants
            "ETHUSDT": "ETH/USDT",
            "ETH-USDT": "ETH/USDT",
            "ETH/USDT": "ETH/USDT",
            "ETH-PERP": "ETH-PERP",
            "ETHPERP": "ETH-PERP",
        }
        logger.warning("using_fallback_symbol_mappings")

    def normalize(self, exchange_symbol: str) -> Symbol:
        """
        Normalize exchange-specific symbol to standard format.

        Args:
            exchange_symbol: Exchange-specific symbol (e.g., "BTCUSDT")

        Returns:
            Standardized symbol (e.g., "BTC/USDT")

        Raises:
            NormalizationError: If symbol cannot be normalized
        """
        # Direct lookup
        if exchange_symbol in self._mapping:
            return self._mapping[exchange_symbol]

        # Try uppercase
        upper_symbol = exchange_symbol.upper()
        if upper_symbol in self._mapping:
            return self._mapping[upper_symbol]

        # Try common transformations
        normalized = self._try_transform(exchange_symbol)
        if normalized:
            return normalized

        # Failed to normalize
        logger.error("symbol_normalization_failed", exchange_symbol=exchange_symbol)
        raise NormalizationError(f"Cannot normalize symbol: {exchange_symbol}")

    def _try_transform(self, symbol: str) -> Optional[Symbol]:
        """
        Try common symbol transformations.

        Args:
            symbol: Original symbol

        Returns:
            Normalized symbol or None
        """
        upper = symbol.upper()

        # Check for USDT spot pairs
        if upper.endswith("USDT"):
            base = upper[:-4]  # Remove USDT
            return f"{base}/USDT"

        # Check for PERP
        if upper.endswith("PERP"):
            if "-" in upper:
                base = upper.split("-")[0]
            else:
                base = upper[:-4]  # Remove PERP
            return f"{base}-PERP"

        return None

    def denormalize(self, standard_symbol: Symbol, exchange: str) -> str:
        """
        Convert standard symbol back to exchange-specific format.

        Args:
            standard_symbol: Standard symbol (e.g., "BTC/USDT")
            exchange: Exchange identifier

        Returns:
            Exchange-specific symbol

        Note: This is a best-effort conversion. May not work for all exchanges.
        """
        # Common conversions by exchange
        if exchange == "binance":
            # Binance uses BTCUSDT format
            return standard_symbol.replace("/", "").replace("-PERP", "PERP")

        elif exchange == "bybit":
            # Bybit uses BTC-USDT for spot, BTCPERP for futures
            if "-PERP" in standard_symbol:
                return standard_symbol.replace("-PERP", "PERP")
            else:
                return standard_symbol.replace("/", "-")

        elif exchange == "okx":
            # OKX uses BTC-USDT format
            return standard_symbol.replace("/", "-")

        elif exchange in ("hyperliquid", "lighter"):
            # PerpDEX typically use BTC-PERP format
            return standard_symbol

        # Default: return as-is
        return standard_symbol

    def add_mapping(self, exchange_symbol: str, standard_symbol: Symbol) -> None:
        """
        Add a new symbol mapping.

        Args:
            exchange_symbol: Exchange-specific symbol
            standard_symbol: Standard symbol
        """
        self._mapping[exchange_symbol] = standard_symbol
        logger.debug("symbol_mapping_added", from_symbol=exchange_symbol, to_symbol=standard_symbol)


# Global symbol mapper instance
_symbol_mapper: Optional[SymbolMapper] = None


def get_symbol_mapper() -> SymbolMapper:
    """
    Get or create global symbol mapper instance.

    Returns:
        SymbolMapper instance
    """
    global _symbol_mapper
    if _symbol_mapper is None:
        _symbol_mapper = SymbolMapper()
    return _symbol_mapper
