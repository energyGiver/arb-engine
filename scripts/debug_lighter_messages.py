"""Debug script to see actual Lighter message format."""

import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import websockets


async def debug_messages():
    """Connect and print first few messages."""
    print("Connecting to Lighter WebSocket...")

    async with websockets.connect("wss://mainnet.zklighter.elliot.ai/stream") as ws:
        print("✓ Connected\n")

        # Subscribe to BTC-PERP trade and orderbook
        await ws.send(json.dumps({"type": "subscribe", "channel": "trade/0"}))
        await ws.send(json.dumps({"type": "subscribe", "channel": "order_book/0"}))
        print("✓ Subscribed\n")

        # Receive first 5 messages
        for i in range(5):
            msg = await ws.recv()
            data = json.loads(msg)

            print(f"Message {i+1}:")
            print(f"  Type: {data.get('type')}")
            print(f"  Channel: {data.get('channel')}")

            # Show structure without overwhelming detail
            if 'trades' in data:
                print(f"  Has 'trades' key with {len(data.get('trades', []))} items")
                if data['trades']:
                    print(f"  First trade keys: {list(data['trades'][0].keys())}")

            if 'orders' in data:
                print(f"  Has 'orders' key")

            if 'order_book' in data:
                print(f"  Has 'order_book' key")

            # Show all top-level keys
            print(f"  Top-level keys: {list(data.keys())}")
            print()


if __name__ == "__main__":
    asyncio.run(debug_messages())
