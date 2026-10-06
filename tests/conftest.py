import asyncio

import pytest

from automerge.repo import Backoff
from automerge.transports import InMemoryTransport


@pytest.fixture
def memory_connection():
    """Connect two running repositories using the public connector APIs."""

    async def establish(client, server):
        url = "memory://server"
        listener = await server.add_listener(url)

        async def connect():
            outgoing, incoming = InMemoryTransport.create_pair()
            try:
                await listener.accept(incoming)
            except BaseException:
                await outgoing.close()
                await incoming.close()
                raise
            return outgoing

        dialer = await client.add_dialer(
            url, connect=connect, backoff=Backoff(max_retries=0)
        )
        await asyncio.wait_for(dialer.wait_connected(), timeout=5)
        return dialer, listener

    return establish
