"""Integration tests for WebSocket transport.

These tests verify that document synchronization works correctly over WebSocket
connections between repos.
"""

import asyncio

# Skip all tests in this file if websockets is not installed
import importlib

import pytest

if importlib.util.find_spec("websockets") is None:
    pytest.skip("websockets not installed", allow_module_level=True)

from automerge.repo import InMemoryStorage, Repo
from automerge.transports import (
    WebSocketClientTransport,
    WebSocketServer,
)


@pytest.mark.asyncio
async def test_websocket_document_sync():
    """Test basic document synchronization over WebSocket."""
    # Create two repos with separate storage
    storage_a = InMemoryStorage()
    storage_b = InMemoryStorage()

    repo_a = await Repo.load(storage_a)
    repo_b = await Repo.load(storage_b)

    async with repo_a, repo_b:
        # Start WebSocket server with repo_b
        async with WebSocketServer(repo_b, "localhost", 8768):
            # Give server time to start
            await asyncio.sleep(0.1)

            # Connect repo_a as client
            transport = await WebSocketClientTransport.connect("ws://localhost:8768")
            _conn_task = asyncio.create_task(repo_a.connect(transport))

            # Give time for handshake to complete
            await asyncio.sleep(0.5)

            # Create a document in repo_a (client)
            handle_a = await repo_a.create()

            # Set some content
            def init_doc(doc):
                doc["title"] = "WebSocket Test"
                doc["count"] = 42

            await handle_a.change(init_doc)

            # Give time for sync
            await asyncio.sleep(0.5)

            # Verify the document synced to repo_b (server)
            url = handle_a.url
            handle_b = await repo_b.find(url)

            assert handle_b is not None, "Document should have synced to server"
            assert handle_b.document_id == handle_a.document_id

            # Verify content matches
            doc_a = handle_a.doc()
            doc_b = handle_b.doc()

            title_a = doc_a["title"]
            title_b = doc_b["title"]
            assert title_a == title_b == "WebSocket Test"

            count_a = doc_a["count"]
            count_b = doc_b["count"]
            assert count_a == count_b == 42

            # Close connection
            await transport.close()
            await asyncio.sleep(0.1)


@pytest.mark.asyncio
async def test_websocket_bidirectional_sync():
    """Test document synchronization in both directions over WebSocket."""
    storage_a = InMemoryStorage()
    storage_b = InMemoryStorage()

    repo_a = await Repo.load(storage_a)
    repo_b = await Repo.load(storage_b)

    async with repo_a, repo_b:
        # Start WebSocket server with repo_b
        async with WebSocketServer(repo_b, "localhost", 8769):
            await asyncio.sleep(0.1)

            # Connect repo_a as client
            transport = await WebSocketClientTransport.connect("ws://localhost:8769")
            _conn_task = asyncio.create_task(repo_a.connect(transport))
            await asyncio.sleep(0.5)

            # Create document in client (repo_a)
            handle_a1 = await repo_a.create()

            def init_doc_a(doc):
                doc["source"] = "client"

            await handle_a1.change(init_doc_a)
            await asyncio.sleep(0.5)

            # Create document in server (repo_b)
            handle_b1 = await repo_b.create()

            def init_doc_b(doc):
                doc["source"] = "server"

            await handle_b1.change(init_doc_b)
            await asyncio.sleep(0.5)

            # Verify client doc synced to server
            handle_a1_on_b = await repo_b.find(handle_a1.url)
            assert handle_a1_on_b is not None, "Client doc should sync to server"

            # Verify
            doc_a1_on_b = handle_a1_on_b.doc()
            source = doc_a1_on_b["source"]
            assert source == "client"

            # Verify server doc synced to client
            handle_b1_on_a = await repo_a.find(handle_b1.url)
            assert handle_b1_on_a is not None, "Server doc should sync to client"

            doc_b1_on_a = handle_b1_on_a.doc()
            source = doc_b1_on_a["source"]
            assert source == "server"

            await transport.close()
            await asyncio.sleep(0.1)


@pytest.mark.asyncio
async def test_websocket_multiple_clients():
    """Test multiple clients connecting to one server."""
    storage_server = InMemoryStorage()
    storage_client1 = InMemoryStorage()
    storage_client2 = InMemoryStorage()

    repo_server = await Repo.load(storage_server)
    repo_client1 = await Repo.load(storage_client1)
    repo_client2 = await Repo.load(storage_client2)

    async with repo_server, repo_client1, repo_client2:
        # Start WebSocket server
        async with WebSocketServer(repo_server, "localhost", 8770):
            await asyncio.sleep(0.1)

            # Connect two clients
            transport1 = await WebSocketClientTransport.connect("ws://localhost:8770")
            transport2 = await WebSocketClientTransport.connect("ws://localhost:8770")

            _conn_task1 = asyncio.create_task(repo_client1.connect(transport1))
            _conn_task2 = asyncio.create_task(repo_client2.connect(transport2))
            await asyncio.sleep(0.5)

            # Create document in client1
            handle1 = await repo_client1.create()

            def init_doc(doc):
                doc["from"] = "client1"

            await handle1.change(init_doc)
            await asyncio.sleep(0.5)

            # Verify it synced to server
            handle_on_server = await repo_server.find(handle1.url)
            assert handle_on_server is not None, "Doc should sync to server"

            # Verify it synced to client2
            handle_on_client2 = await repo_client2.find(handle1.url)
            assert handle_on_client2 is not None, "Doc should sync to client2"

            # Verify
            doc_on_client2 = handle_on_client2.doc()
            from_field = doc_on_client2["from"]
            assert from_field == "client1"

            await transport1.close()
            await transport2.close()
            await asyncio.sleep(0.1)


@pytest.mark.asyncio
async def test_websocket_document_changes_sync():
    """Test that document changes propagate over WebSocket."""
    storage_a = InMemoryStorage()
    storage_b = InMemoryStorage()

    repo_a = await Repo.load(storage_a)
    repo_b = await Repo.load(storage_b)

    async with repo_a, repo_b:
        async with WebSocketServer(repo_b, "localhost", 8771):
            await asyncio.sleep(0.1)

            transport = await WebSocketClientTransport.connect("ws://localhost:8771")
            _conn_task = asyncio.create_task(repo_a.connect(transport))
            await asyncio.sleep(0.5)

            # Create document with initial content
            handle_a = await repo_a.create()

            def init_doc(doc):
                doc["counter"] = 0

            await handle_a.change(init_doc)
            await asyncio.sleep(0.5)

            # Get handle on repo_b
            handle_b = await repo_b.find(handle_a.url)
            assert handle_b is not None

            # Make multiple changes on repo_a
            for i in range(1, 4):

                def increment(doc):
                    current = doc["counter"]
                    doc["counter"] = current + 1

                await handle_a.change(increment)
                await asyncio.sleep(0.3)

            # Verify final value on repo_b
            doc_b = handle_b.doc()
            counter_b = doc_b["counter"]
            assert counter_b == 3, "Counter should be 3 after three increments"

            await transport.close()
            await asyncio.sleep(0.1)


@pytest.mark.asyncio
async def test_websocket_connection_failure():
    """Test handling of connection failure."""
    storage = InMemoryStorage()
    repo = await Repo.load(storage)

    async with repo:
        # Try to connect to non-existent server
        try:
            _transport = await asyncio.wait_for(
                WebSocketClientTransport.connect("ws://localhost:9999"),
                timeout=2.0,
            )
            assert False, "Should have failed to connect"
        except (ConnectionRefusedError, OSError, asyncio.TimeoutError):
            # Expected - connection should fail
            pass


@pytest.mark.asyncio
async def test_websocket_connection_lost_during_operation():
    """Test handling of connection lost during operation."""
    storage_a = InMemoryStorage()
    storage_b = InMemoryStorage()

    repo_a = await Repo.load(storage_a)
    repo_b = await Repo.load(storage_b)

    async with repo_a, repo_b:
        # Start server
        server = WebSocketServer(repo_b, "localhost", 8772)
        await server.start()
        await asyncio.sleep(0.1)

        try:
            # Connect client
            transport = await WebSocketClientTransport.connect("ws://localhost:8772")
            _conn_task = asyncio.create_task(repo_a.connect(transport))
            await asyncio.sleep(0.5)

            # Create document
            handle = await repo_a.create()

            def init_doc(doc):
                doc["test"] = "value"

            await handle.change(init_doc)
            await asyncio.sleep(0.3)

            # Force close the server (simulates connection loss)
            await server.stop()
            await asyncio.sleep(0.3)

            # Connection task should complete (with error reason)
            # The repo should handle this gracefully
            # Note: conn_task may already be done at this point

        finally:
            # Clean up
            if server._server is not None:
                await server.stop()


@pytest.mark.asyncio
async def test_websocket_server_shutdown_with_clients():
    """Test graceful server shutdown while clients are connected."""
    storage_server = InMemoryStorage()
    storage_client = InMemoryStorage()

    repo_server = await Repo.load(storage_server)
    repo_client = await Repo.load(storage_client)

    async with repo_server, repo_client:
        # Start server
        server = WebSocketServer(repo_server, "localhost", 8773)
        await server.start()
        await asyncio.sleep(0.1)

        # Connect client
        transport = await WebSocketClientTransport.connect("ws://localhost:8773")
        _conn_task = asyncio.create_task(repo_client.connect(transport))
        await asyncio.sleep(0.3)

        # Verify connection is established
        connections = repo_server._hub.connections()
        assert len(connections) > 0, "Should have active connection"

        # Stop server gracefully
        await server.stop()
        await asyncio.sleep(0.3)

        # Server should have cleaned up
        assert server._server is None
        assert len(server._connections) == 0


@pytest.mark.asyncio
async def test_websocket_binary_message_validation():
    """Test that non-binary messages are rejected."""
    storage_a = InMemoryStorage()
    storage_b = InMemoryStorage()

    repo_a = await Repo.load(storage_a)
    repo_b = await Repo.load(storage_b)

    async with repo_a, repo_b:
        async with WebSocketServer(repo_b, "localhost", 8774):
            await asyncio.sleep(0.1)

            transport = await WebSocketClientTransport.connect("ws://localhost:8774")

            # Test sending non-bytes raises TypeError
            try:
                await transport.send("not bytes")  # type: ignore
                assert False, "Should have raised TypeError"
            except TypeError as e:
                assert "must be bytes" in str(e)

            await transport.close()
            await asyncio.sleep(0.1)
