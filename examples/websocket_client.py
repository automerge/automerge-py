#!/usr/bin/env python3
"""WebSocket Client Example

Start ``python examples/websocket_server.py`` in another terminal, then run
``python examples/websocket_client.py``. The client synchronizes documents and
reconnects automatically until interrupted.
"""

import asyncio

from automerge.repo import InMemoryStorage, Repo
from automerge.transports import WebSocketClientTransport


async def main():
    """Run a reconnecting WebSocket client."""
    repo = await Repo.load(InMemoryStorage())
    url = "ws://localhost:8080"

    async def connect():
        return await WebSocketClientTransport.connect(url)

    async with repo:
        print(f"Connecting to WebSocket server at {url}...")
        dialer = await repo.add_dialer(url, connect=connect)
        async with dialer:
            await dialer.wait_connected()
            print("Connected!")

            handle = await repo.create()
            with handle.change() as doc:
                doc["from"] = "client"
                doc["clicks"] = 0
            print(f"\nCreated document: {handle.url}")

            for _ in range(5):
                await asyncio.sleep(2)
                current = handle.doc()["clicks"]
                with handle.change() as doc:
                    new_value = current + 1
                    doc["clicks"] = new_value
                    print(f"Incremented clicks to {new_value}")

            print("\nChanges complete. Press Ctrl+C to disconnect.")
            await asyncio.Event().wait()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nExiting...")
