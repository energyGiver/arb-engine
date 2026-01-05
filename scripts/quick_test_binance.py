"""Quick test for Binance adapter."""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from arb_engine.adapters.cex.binance_ws import BinanceAdapter
from arb_engine.core.config import get_settings
from arb_engine.core.logging import setup_logging


async def quick_test():
    """Quick test to verify data reception."""
    setup_logging(log_level="WARNING")  # Only show warnings/errors

    settings = get_settings()

    adapter = BinanceAdapter(
        symbols=["BTC/USDT", "ETH/USDT"],
        api_key=settings.binance_api_key,
        api_secret=settings.binance_api_secret,
    )

    print("Binance Adapter Quick Test")
    print("-" * 40)

    try:
        await adapter.connect()
        print("✓ Connected")

        # Note: Binance subscriptions happen in connect()
        print("✓ Subscribed (automatic)")

        print("\nWaiting 5 seconds for data...")
        await asyncio.sleep(5)

        print("\nResults:")
        success = True
        for symbol in adapter.symbols:
            tick = adapter.get_latest_tick(symbol)
            book = adapter.get_latest_orderbook(symbol)

            print(f"\n{symbol}:")
            if tick:
                print(f"  ✓ Price: ${tick.price}")
                if tick.volume_24h:
                    print(f"  ✓ 24h Volume: {tick.volume_24h}")
            else:
                print(f"  ✗ No price data")
                success = False

            if book and book.best_bid and book.best_ask:
                print(f"  ✓ Bid: ${book.best_bid} | Ask: ${book.best_ask}")
                print(f"  ✓ Spread: {book.spread_bps:.2f} bps")
            else:
                print(f"  ✗ No orderbook data")
                success = False

        print("\n" + "-" * 40)
        if success:
            print("✓ TEST PASSED - All data received!")
        else:
            print("✗ TEST FAILED - Missing data")

    except Exception as e:
        print(f"\n✗ ERROR: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await adapter.disconnect()


if __name__ == "__main__":
    asyncio.run(quick_test())
