"""Transport implementations for automerge-repo.

This module provides transport implementations for syncing Automerge documents
between repos. Transports handle the low-level communication between peers.

Available transports:
- InMemoryTransport: For testing and local communication (always available)
- WebSocketClientTransport: Client-side WebSocket transport (requires websockets package)
- WebSocketServerTransport: Server-side WebSocket transport (requires websockets package)
- WebSocketServer: Helper for running a WebSocket server (requires websockets package)
"""

# Always available - no dependencies
from .memory import InMemoryTransport

__all__ = ["InMemoryTransport"]

# WebSocket transports - optional dependency
try:
    from .websocket import (
        WebSocketClientTransport,  # noqa
        WebSocketServer,  # noqa
        WebSocketServerTransport,  # noqa
    )

    __all__.extend(
        [
            "WebSocketClientTransport",
            "WebSocketServerTransport",
            "WebSocketServer",
        ]
    )
except ImportError:
    # websockets package not installed - that's okay, just don't export these
    pass
