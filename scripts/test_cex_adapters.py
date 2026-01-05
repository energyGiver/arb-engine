"""Test CEX adapters connectivity."""

import asyncio
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from arb_engine.adapters.cex.binance_ws import BinanceAdapter
from arb_engine.adapters.cex.bybit_ws import BybitAdapter
from arb_engine.adapters.cex.okx_ws import OKXAdapter
from arb_engine.core.logging import setup_logging


async def test_adapter(adapter, duration=10):
    """
    Test an adapter by connecting and collecting data.

    Args:
        adapter: Adapter instance
        duration: How long to run (seconds)
    """
    print(f"\n{'='*60}")
    print(f"Testing {adapter.exchange_id.upper()} Adapter")
    print(f"{'='*60}")

    try:
        # Connect and subscribe
        await adapter.connect()
        await adapter.subscribe_tickers(adapter.symbols)
        await adapter.subscribe_orderbook(adapter.symbols)

        print(f"✓ Connected to {adapter.exchange_id}")
        print(f"  Subscribed to: {', '.join(adapter.symbols)}")
        print(f"  Waiting {duration}s for data...\n")

        # Collect data for specified duration
        for i in range(duration):
            await asyncio.sleep(1)

            # Check if we have data
            for symbol in adapter.symbols:
                tick = adapter.get_latest_tick(symbol)
                orderbook = adapter.get_latest_orderbook(symbol)

                if tick:
                    print(
                        f"  [{i+1:2d}s] {symbol:10s} | "
                        f"Price: ${float(tick.price):>10,.2f} | "
                        f"Vol: {float(tick.volume_24h):>12,.2f}"
                    )

                if orderbook:
                    spread_bps = float(orderbook.spread_bps) if orderbook.spread_bps else 0
                    print(
                        f"        {'':10s} | "
                        f"Bid: ${float(orderbook.best_bid):>10,.2f} | "
                        f"Ask: ${float(orderbook.best_ask):>10,.2f} | "
                        f"Spread: {spread_bps:>6.2f} bps"
                    )

        # Disconnect
        await adapter.disconnect()
        print(f"\n✓ {adapter.exchange_id.upper()} test completed successfully")

        return True

    except Exception as e:
        print(f"\n✗ {adapter.exchange_id.upper()} test failed: {e}")
        import traceback

        traceback.print_exc()
        return False


async def main():
    """Run all adapter tests."""
    print("CEX ADAPTER CONNECTIVITY TEST")
    print("Testing Binance, Bybit, and OKX WebSocket adapters")
    print()

    # Setup logging
    setup_logging(log_level="WARNING")  # Less verbose for test

    symbols = ["BTC/USDT", "ETH/USDT"]

    # Create adapters
    adapters = [
        BinanceAdapter(symbols=symbols),
        BybitAdapter(symbols=symbols),
        OKXAdapter(symbols=symbols),
    ]

    # Test each adapter sequentially
    results = {}
    for adapter in adapters:
        success = await test_adapter(adapter, duration=5)
        results[adapter.exchange_id] = success

    # Summary
    print("\n" + "=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)

    for exchange, success in results.items():
        status = "✓ PASS" if success else "✗ FAIL"
        print(f"{exchange.upper():20s} {status}")

    all_passed = all(results.values())
    print("\n" + "=" * 60)
    if all_passed:
        print("ALL TESTS PASSED ✓")
    else:
        print("SOME TESTS FAILED ✗")
    print("=" * 60)

    return 0 if all_passed else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
