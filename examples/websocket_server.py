#!/usr/bin/env python3
"""WebSocket Server Example

This example shows how to run a WebSocket server that shares Automerge
documents with connected clients.

Usage:
    python examples/websocket_server.py

The server will start on localhost:8080 and accept connections from clients.
"""

import asyncio
from automerge.repo import Repo, InMemoryStorage
from automerge.transports import WebSocketServer


async def main():
    """Run the WebSocket server."""
    # Create a repository with in-memory storage
    storage = InMemoryStorage()
    repo = await Repo.load(storage)

    async with repo:
        print("Starting WebSocket server on ws://localhost:8080")

        # Create an initial document
        handle = await repo.create()

        def init_doc(doc):
            doc["message"] = "Hello from server!"
            doc["counter"] = 0

        await handle.change(init_doc)

        print(f"Created document: {handle.url}")
        print(f"Document ID: {handle.document_id}")

        # Start the WebSocket server
        async with WebSocketServer(repo, "localhost", 8080):
            print("\nServer is running. Press Ctrl+C to stop.")
            print("Clients can connect to: ws://localhost:8080")

            # Keep the server running
            try:
                while True:
                    await asyncio.sleep(1)

                    # Show active connections
                    connections = repo._hub.connections()
                    if connections:
                        print(
                            f"\rActive connections: {len(connections)}",
                            end="",
                            flush=True,
                        )

            except KeyboardInterrupt:
                print("\n\nShutting down server...")


if __name__ == "__main__":
    asyncio.run(main())
