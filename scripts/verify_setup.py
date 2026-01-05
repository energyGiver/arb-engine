"""Verify that the basic project setup is working."""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from decimal import Decimal
from datetime import datetime

def test_imports():
    """Test that all modules can be imported."""
    print("Testing imports...")

    try:
        from arb_engine.core import config, errors, logging, types, clock
        print("✓ Core modules imported successfully")

        from arb_engine.domain import models, schema
        print("✓ Domain modules imported successfully")

        from arb_engine.storage import duckdb, writers
        print("✓ Storage modules imported successfully")

        return True
    except Exception as e:
        print(f"✗ Import failed: {e}")
        return False


def test_logging():
    """Test logging setup."""
    print("\nTesting logging...")

    try:
        from arb_engine.core.logging import setup_logging, get_logger

        setup_logging(log_level="INFO")
        logger = get_logger("test")
        logger.info("test_message", key="value", number=42)
        print("✓ Logging works")
        return True
    except Exception as e:
        print(f"✗ Logging failed: {e}")
        return False


def test_config():
    """Test config loading."""
    print("\nTesting config...")

    try:
        from arb_engine.core.config import get_settings, get_config_loader

        settings = get_settings()
        print(f"  Environment: {settings.environment}")
        print(f"  Log level: {settings.log_level}")
        print(f"  DuckDB path: {settings.duckdb_path}")

        loader = get_config_loader()
        exchanges = loader.load_exchanges()
        fees = loader.load_fees()

        print(f"  Loaded {len(exchanges.get('cex', {}))} CEX configs")
        print(f"  Loaded {len(exchanges.get('perps', {}))} PerpDEX configs")
        print(f"  Loaded fees for {len(fees.get('cex', {}))} CEXs")
        print("✓ Config loading works")
        return True
    except Exception as e:
        print(f"✗ Config failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_models():
    """Test domain models."""
    print("\nTesting domain models...")

    try:
        from arb_engine.domain.models import PriceTick, OrderbookL5, OrderbookLevel
        from arb_engine.core.clock import get_clock

        clock = get_clock()
        now = clock.now()

        # Test PriceTick
        tick = PriceTick(
            exchange="binance",
            symbol="BTC/USDT",
            price=Decimal("50000.00"),
            volume_24h=Decimal("1000.0"),
            exchange_timestamp=now,
            received_timestamp=now,
        )
        print(f"  Created PriceTick: {tick.exchange} {tick.symbol} @ ${tick.price}")

        # Test OrderbookL5
        orderbook = OrderbookL5(
            exchange="binance",
            symbol="BTC/USDT",
            bids=[
                OrderbookLevel(price=Decimal("49999"), size=Decimal("1.0")),
                OrderbookLevel(price=Decimal("49998"), size=Decimal("2.0")),
            ],
            asks=[
                OrderbookLevel(price=Decimal("50001"), size=Decimal("1.0")),
                OrderbookLevel(price=Decimal("50002"), size=Decimal("2.0")),
            ],
            exchange_timestamp=now,
            received_timestamp=now,
        )
        print(f"  Created Orderbook: spread={orderbook.spread} ({orderbook.spread_bps} bps)")
        print("✓ Domain models work")
        return True
    except Exception as e:
        print(f"✗ Models failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_storage():
    """Test storage layer."""
    print("\nTesting storage...")

    try:
        from arb_engine.storage.duckdb import DuckDBConnection
        from arb_engine.storage.writers import DataWriter

        # Use temporary database
        db = DuckDBConnection(db_path="data/duckdb/test.db")
        print("  ✓ DuckDB connection established")

        # Check tables
        result = db.execute("SHOW TABLES").fetchall()
        table_names = [row[0] for row in result]
        print(f"  ✓ Tables created: {', '.join(table_names)}")

        db.close()
        print("✓ Storage works")
        return True
    except Exception as e:
        print(f"✗ Storage failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Run all verification tests."""
    print("=" * 60)
    print("ARB-ENGINE SETUP VERIFICATION")
    print("=" * 60)

    results = {
        "Imports": test_imports(),
        "Logging": test_logging(),
        "Config": test_config(),
        "Models": test_models(),
        "Storage": test_storage(),
    }

    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)

    for name, passed in results.items():
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"{name:20s} {status}")

    all_passed = all(results.values())
    print("\n" + ("=" * 60))
    if all_passed:
        print("ALL TESTS PASSED ✓")
    else:
        print("SOME TESTS FAILED ✗")
    print("=" * 60)

    return 0 if all_passed else 1


if __name__ == "__main__":
    sys.exit(main())
