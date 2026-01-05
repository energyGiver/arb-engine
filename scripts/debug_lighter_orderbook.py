"""Debug script to see Lighter orderbook structure."""

import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import websockets


async def debug_orderbook():
    """Show orderbook structure."""
    print("Connecting to Lighter WebSocket...")

    async with websockets.connect("wss://mainnet.zklighter.elliot.ai/stream") as ws:
        print("✓ Connected\n")

        # Subscribe to BTC-PERP orderbook
        await ws.send(json.dumps({"type": "subscribe", "channel": "order_book/0"}))
        print("✓ Subscribed to orderbook\n")

        # Get orderbook snapshot
        async for msg in ws:
            data = json.loads(msg)

            if data.get("type") == "connected":
                continue

            if "order_book" in data:
                ob = data["order_book"]
                print("Orderbook structure:")
                print(f"  Keys: {list(ob.keys())}")

                if "asks" in ob:
                    print(f"\n  Asks ({len(ob['asks'])} levels):")
                    for i, ask in enumerate(ob["asks"][:3]):
                        print(f"    [{i}] {ask}")

                if "bids" in ob:
                    print(f"\n  Bids ({len(ob['bids'])} levels):")
                    for i, bid in enumerate(ob["bids"][:3]):
                        print(f"    [{i}] {bid}")

                break


if __name__ == "__main__":
    asyncio.run(debug_orderbook())
