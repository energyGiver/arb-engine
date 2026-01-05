"""Test script for Lighter adapter."""

import asyncio
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from arb_engine.adapters.perps.lighter import LighterAdapter
from arb_engine.core.config import get_settings
from arb_engine.core.logging import setup_logging


async def test_lighter_adapter():
    """Test Lighter adapter connection and data reception."""
    # Setup logging
    setup_logging(log_level="DEBUG", json_logs=False)

    # Load settings
    settings = get_settings()

    print("=" * 60)
    print("Testing Lighter Adapter")
    print("=" * 60)

    # Create adapter
    adapter = LighterAdapter(
        symbols=["BTC-PERP", "ETH-PERP"],
        api_key_private_key=settings.lighter_api_key_private_key,
        account_index=settings.lighter_account_index,
        api_key_index=settings.lighter_api_key_index,
    )

    print(f"\n1. Adapter created: {adapter}")
    print(f"   Exchange: {adapter.exchange_id}")
    print(f"   Symbols: {adapter.symbols}")
    print(f"   WS Endpoint: {adapter.ws_endpoint}")

    try:
        # Connect
        print("\n2. Connecting to Lighter WebSocket...")
        await adapter.connect()
        print("   ✓ Connected successfully!")

        # Subscribe to data
        print("\n3. Subscribing to tickers and orderbook...")
        await adapter.subscribe_tickers(adapter.symbols)
        await adapter.subscribe_orderbook(adapter.symbols)
        print("   ✓ Subscriptions sent!")

        # Wait and collect data
        print("\n4. Waiting 10 seconds for data...")
        for i in range(10):
            await asyncio.sleep(1)
            print(f"   {i+1}s elapsed...")

            # Check for received data
            for symbol in adapter.symbols:
                tick = adapter.get_latest_tick(symbol)
                orderbook = adapter.get_latest_orderbook(symbol)

                if tick:
                    print(f"   [{symbol}] Last Price: ${tick.price}")

                if orderbook:
                    print(
                        f"   [{symbol}] Bid: ${orderbook.best_bid} | Ask: ${orderbook.best_ask} | Spread: {orderbook.spread_bps:.2f} bps"
                    )

        # Final summary
        print("\n5. Data Summary:")
        for symbol in adapter.symbols:
            tick = adapter.get_latest_tick(symbol)
            orderbook = adapter.get_latest_orderbook(symbol)

            print(f"\n   {symbol}:")
            if tick:
                print(f"     ✓ Tick data received")
                print(f"       Price: ${tick.price}")
                print(f"       Exchange TS: {tick.exchange_timestamp}")
                print(f"       Received TS: {tick.received_timestamp}")
            else:
                print(f"     ✗ No tick data received")

            if orderbook:
                print(f"     ✓ Orderbook data received")
                print(f"       Best Bid: ${orderbook.best_bid}")
                print(f"       Best Ask: ${orderbook.best_ask}")
                print(f"       Mid Price: ${orderbook.mid_price}")
                print(f"       Spread: {orderbook.spread_bps:.2f} bps")
                print(f"       Bids: {len(orderbook.bids)} levels")
                print(f"       Asks: {len(orderbook.asks)} levels")
            else:
                print(f"     ✗ No orderbook data received")

        print("\n" + "=" * 60)
        print("Test completed successfully!")
        print("=" * 60)

    except Exception as e:
        print(f"\n✗ Error occurred: {e}")
        import traceback

        traceback.print_exc()

    finally:
        # Disconnect
        print("\n6. Disconnecting...")
        await adapter.disconnect()
        print("   ✓ Disconnected")


if __name__ == "__main__":
    asyncio.run(test_lighter_adapter())
