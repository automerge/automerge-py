"""In-memory transport for testing and local communication.

This module provides InMemoryTransport, which allows two repos to communicate
within the same process using asyncio queues. It's useful for testing and
development scenarios.
"""

import asyncio
from typing import Tuple


class InMemoryTransport:
    """In-memory transport for testing.

    This transport allows two repos to communicate within the same process
    using asyncio queues. Create a pair of transports using create_pair().
    """

    def __init__(self, send_queue: asyncio.Queue, recv_queue: asyncio.Queue):
        """Initialize an in-memory transport.

        Args:
            send_queue: Queue to send messages to
            recv_queue: Queue to receive messages from
        """
        self._send_queue = send_queue
        self._recv_queue = recv_queue
        self._closed = False

    @classmethod
    def create_pair(cls) -> Tuple["InMemoryTransport", "InMemoryTransport"]:
        """Create a pair of connected transports.

        Returns:
            A tuple of (transport_a, transport_b) where messages sent on
            transport_a are received on transport_b and vice versa.
        """
        queue_a_to_b = asyncio.Queue()
        queue_b_to_a = asyncio.Queue()

        transport_a = cls(queue_a_to_b, queue_b_to_a)
        transport_b = cls(queue_b_to_a, queue_a_to_b)

        return transport_a, transport_b

    async def send(self, msg: bytes):
        """Send a message to the peer.

        Args:
            msg: The message bytes to send

        Raises:
            RuntimeError: If the transport is closed
        """
        if self._closed:
            raise RuntimeError("Transport is closed")
        await self._send_queue.put(msg)

    async def recv(self) -> bytes:
        """Receive a message from the peer.

        Returns:
            The received message bytes

        Raises:
            RuntimeError: If the transport is closed
        """
        if self._closed:
            raise RuntimeError("Transport is closed")
        return await self._recv_queue.get()

    async def close(self):
        """Close the transport.

        After closing, send() and recv() will raise RuntimeError.
        """
        self._closed = True
