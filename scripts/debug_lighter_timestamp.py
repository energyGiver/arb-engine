"""Debug script to check Lighter timestamp format."""

import asyncio
import json
import websockets


async def debug_timestamp():
    """Check timestamp values."""
    async with websockets.connect("wss://mainnet.zklighter.elliot.ai/stream") as ws:
        await ws.send(json.dumps({"type": "subscribe", "channel": "trade/0"}))
        await ws.send(json.dumps({"type": "subscribe", "channel": "order_book/0"}))

        for i in range(5):
            msg = await ws.recv()
            data = json.loads(msg)

            if data.get("type") == "connected":
                continue

            print(f"\nMessage {i+1}:")
            print(f"  Type: {data.get('type')}")

            # Check timestamp in message root
            if "timestamp" in data:
                ts = data["timestamp"]
                print(f"  Root timestamp: {ts}")
                print(f"    Type: {type(ts)}")
                # Try different conversions
                print(f"    / 1000 = {ts / 1000 if isinstance(ts, (int, float)) else 'N/A'}")
                print(f"    / 1000000 = {ts / 1000000 if isinstance(ts, (int, float)) else 'N/A'}")

            # Check timestamp in trades
            if "trades" in data and data["trades"]:
                trade = data["trades"][0]
                if "timestamp" in trade:
                    ts = trade["timestamp"]
                    print(f"  Trade timestamp: {ts}")
                    print(f"    Type: {type(ts)}")
                    print(f"    / 1000 = {ts / 1000 if isinstance(ts, (int, float)) else 'N/A'}")
                    print(f"    / 1000000 = {ts / 1000000 if isinstance(ts, (int, float)) else 'N/A'}")


if __name__ == "__main__":
    asyncio.run(debug_timestamp())
