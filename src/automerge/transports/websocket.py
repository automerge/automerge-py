"""WebSocket transport implementations for automerge-repo.

This module provides WebSocket-based transports for syncing Automerge documents
over WebSocket connections. It requires the `websockets` package to be installed.

Installation:
    pip install automerge[websocket]

Usage:
    # Client side
    transport = await WebSocketClientTransport.connect("ws://localhost:8080")
    await repo.connect(transport)

    # Server side
    async def handle_connection(websocket, path):
        transport = WebSocketServerTransport(websocket)
        await repo.accept(transport)

    # Or use the WebSocketServer helper
    async with WebSocketServer(repo, "localhost", 8080):
        # Server is now running and accepting connections
        await asyncio.sleep(3600)  # Keep server running
"""

# Import guard - provide helpful error message if websockets not installed
try:
    from websockets.client import WebSocketClientProtocol, connect
    from websockets.exceptions import ConnectionClosed
    from websockets.server import WebSocketServerProtocol, serve
except ImportError as e:
    raise ImportError(
        "WebSocket transport requires the 'websockets' package. "
        "Install it with: pip install automerge[websocket]"
    ) from e

import asyncio


class WebSocketClientTransport:
    """Client-side WebSocket transport.

    This transport wraps a WebSocket client connection for use with automerge-repo.
    Use the connect() classmethod to establish a connection.

    Example:
        >>> transport = await WebSocketClientTransport.connect("ws://localhost:8080")
        >>> await repo.connect(transport)
    """

    def __init__(self, websocket: WebSocketClientProtocol):
        """Initialize a WebSocket client transport.

        Args:
            websocket: The connected WebSocket client protocol instance

        Note:
            Prefer using the connect() classmethod rather than calling this directly.
        """
        self._websocket = websocket
        self._closed = False

    @classmethod
    async def connect(cls, uri: str) -> "WebSocketClientTransport":
        """Connect to a WebSocket server.

        Args:
            uri: The WebSocket URI to connect to (e.g., "ws://localhost:8080")

        Returns:
            A connected WebSocketClientTransport instance

        Example:
            >>> transport = await WebSocketClientTransport.connect("ws://localhost:8080")
            >>> await repo.connect(transport)
        """
        websocket = await connect(uri)
        return cls(websocket)

    async def send(self, msg: bytes):
        """Send a message to the peer.

        Args:
            msg: The message bytes to send

        Raises:
            RuntimeError: If the transport is closed
            TypeError: If the message is not bytes
        """
        if self._closed:
            raise RuntimeError("Transport is closed")

        if not isinstance(msg, bytes):
            raise TypeError(f"Message must be bytes, got {type(msg)}")

        await self._websocket.send(msg)

    async def recv(self) -> bytes:
        """Receive a message from the peer.

        Returns:
            The received message bytes

        Raises:
            RuntimeError: If the transport is closed
            TypeError: If the received message is not bytes
        """
        if self._closed:
            raise RuntimeError("Transport is closed")

        try:
            msg = await self._websocket.recv()
        except ConnectionClosed:
            # Connection closed - return empty bytes to signal EOF
            self._closed = True
            raise RuntimeError("Connection closed")

        if not isinstance(msg, bytes):
            raise TypeError(
                f"Expected binary message, got {type(msg)}. "
                "Ensure the WebSocket is configured for binary messages."
            )

        return msg

    async def close(self):
        """Close the WebSocket connection."""
        if not self._closed:
            self._closed = True
            await self._websocket.close()

    async def wait_closed(self):
        """Wait for the WebSocket connection to be fully closed."""
        await self._websocket.wait_closed()


class WebSocketServerTransport:
    """Server-side WebSocket transport.

    This transport wraps a WebSocket server connection for use with automerge-repo.
    Typically used within a WebSocket server handler function.

    Example:
        >>> async def handle_connection(websocket, path):
        ...     transport = WebSocketServerTransport(websocket)
        ...     await repo.accept(transport)
    """

    def __init__(self, websocket: WebSocketServerProtocol):
        """Initialize a WebSocket server transport.

        Args:
            websocket: The WebSocket server protocol instance from the connection handler
        """
        self._websocket = websocket
        self._closed = False

    async def send(self, msg: bytes):
        """Send a message to the peer.

        Args:
            msg: The message bytes to send

        Raises:
            RuntimeError: If the transport is closed
            TypeError: If the message is not bytes
        """
        if self._closed:
            raise RuntimeError("Transport is closed")

        if not isinstance(msg, bytes):
            raise TypeError(f"Message must be bytes, got {type(msg)}")

        await self._websocket.send(msg)

    async def recv(self) -> bytes:
        """Receive a message from the peer.

        Returns:
            The received message bytes. Returns empty bytes on EOF/connection close.

        Raises:
            RuntimeError: If the transport is closed
            TypeError: If the received message is not bytes
        """
        if self._closed:
            raise RuntimeError("Transport is closed")

        try:
            msg = await self._websocket.recv()
        except ConnectionClosed:
            # Connection closed - return empty bytes to signal EOF
            self._closed = True
            raise RuntimeError("Connection closed")

        if not isinstance(msg, bytes):
            raise TypeError(
                f"Expected binary message, got {type(msg)}. "
                "Ensure the WebSocket is configured for binary messages."
            )

        return msg

    async def close(self):
        """Close the WebSocket connection."""
        if not self._closed:
            self._closed = True
            await self._websocket.close()

    async def wait_closed(self):
        """Wait for the WebSocket connection to be fully closed."""
        await self._websocket.wait_closed()


class WebSocketServer:
    """Helper for running a WebSocket server for an automerge-repo.

    This class manages a WebSocket server that automatically accepts connections
    and handles them with the provided repository.

    Example:
        >>> repo = await Repo.load(storage)
        >>> async with repo:
        ...     async with WebSocketServer(repo, "localhost", 8080):
        ...         # Server is now running
        ...         await asyncio.sleep(3600)  # Keep server running
    """

    def __init__(self, repo, host: str = "localhost", port: int = 8080):
        """Initialize a WebSocket server.

        Args:
            repo: The Repo instance to use for handling connections
            host: The host to bind to (default: "localhost")
            port: The port to bind to (default: 8080)
        """
        # Import here to avoid circular dependency
        from automerge.repo import Repo

        self._repo: Repo = repo
        self._host = host
        self._port = port
        self._server = None
        self._connections: set[asyncio.Task] = set()

    async def _handle_connection(self, websocket, path):
        """Handle a WebSocket connection.

        Args:
            websocket: The WebSocket connection
            path: The request path (unused)
        """
        # Create transport and accept connection
        transport = WebSocketServerTransport(websocket)

        # Create task for this connection
        task = asyncio.create_task(self._repo.accept(transport))
        self._connections.add(task)

        try:
            # Wait for connection to finish
            await task
        finally:
            # Clean up
            self._connections.discard(task)

    async def start(self):
        """Start the WebSocket server.

        This starts the server in the background. Use stop() to shut it down,
        or use the context manager interface instead.
        """
        if self._server is not None:
            raise RuntimeError("Server is already running")

        self._server = await serve(self._handle_connection, self._host, self._port)

    async def stop(self):
        """Stop the WebSocket server.

        This shuts down the server and waits for all active connections to close.
        """
        if self._server is None:
            return

        # Close the server (stop accepting new connections)
        self._server.close()
        await self._server.wait_closed()
        self._server = None

        # Wait for all active connections to finish
        if self._connections:
            await asyncio.gather(*self._connections, return_exceptions=True)
        self._connections.clear()

    async def __aenter__(self):
        """Enter the async context manager - start the server."""
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Exit the async context manager - stop the server."""
        await self.stop()
        return False
