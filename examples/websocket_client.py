#!/usr/bin/env python3
"""WebSocket Client Example

This example shows how to connect to a WebSocket server and sync Automerge
documents.

Usage:
    # First, start the server in another terminal:
    python examples/websocket_server.py

    # Then run this client:
    python examples/websocket_client.py

The client will connect to the server and sync documents.
"""

import asyncio
from automerge.repo import Repo, InMemoryStorage
from automerge.transports import WebSocketClientTransport


async def main():
    """Run the WebSocket client."""
    # Create a repository with in-memory storage
    storage = InMemoryStorage()
    repo = await Repo.load(storage)

    async with repo:
        print("Connecting to WebSocket server at ws://localhost:8080...")

        # Connect to the server
        transport = await WebSocketClientTransport.connect("ws://localhost:8080")

        # Start the connection (this will run until disconnected)
        conn_task = asyncio.create_task(repo.connect(transport))

        # Give time for handshake
        await asyncio.sleep(1)
        print("Connected!")

        # Create a new document
        handle = await repo.create()

        def init_doc(doc):
            doc["from"] = "client"
            doc["clicks"] = 0

        await handle.change(init_doc)

        print(f"\nCreated document: {handle.url}")

        # Make some changes
        for i in range(5):
            await asyncio.sleep(2)

            def increment(doc):
                current = doc["clicks"]
                new_value = current + 1
                doc["clicks"] = new_value
                print(f"Incremented clicks to {new_value}")

            await handle.change(increment)

        print("\nChanges complete. Press Ctrl+C to disconnect.")

        # Wait for user to stop
        try:
            await conn_task
        except KeyboardInterrupt:
            print("\n\nDisconnecting...")
            await transport.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nExiting...")
