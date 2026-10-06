"""Regression coverage for samod's connector and document-search API changes."""

import asyncio
from uuid import uuid4

import pytest

from automerge._automerge import ConnectionStateConnected
from automerge.repo import AutomergeUrl, DocumentId, InMemoryStorage, Repo
from automerge.transports import InMemoryTransport


class GatedStorage(InMemoryStorage):
    def __init__(self):
        super().__init__()
        self.block_reads = False
        self.read_started = asyncio.Event()
        self.allow_reads = asyncio.Event()

    async def load_range(self, prefix):
        if self.block_reads:
            self.read_started.set()
            await self.allow_reads.wait()
        return await super().load_range(prefix)


@pytest.mark.asyncio
async def test_concurrent_find_waits_for_stored_document():
    storage = GatedStorage()
    original = await Repo.load(storage)
    async with original:
        handle = await original.create()
        url = handle.url
        with handle.change() as doc:
            doc["counter"] = 42

    restored = await Repo.load(storage)
    async with restored:
        storage.block_reads = True
        searches = [asyncio.create_task(restored.find(url)) for _ in range(2)]
        try:
            await asyncio.wait_for(storage.read_started.wait(), timeout=2)
            assert all(not search.done() for search in searches)
            storage.allow_reads.set()
            handles = await asyncio.wait_for(asyncio.gather(*searches), timeout=2)
            for handle in handles:
                assert handle is not None
                assert handle.doc()["counter"] == 42
        finally:
            storage.allow_reads.set()
            for search in searches:
                search.cancel()
            await asyncio.gather(*searches, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("with_peer", [False, True])
async def test_find_unavailable_document(with_peer):
    repo = await Repo.load()
    peer = await Repo.load()
    missing = AutomergeUrl.from_document_id(DocumentId.from_bytes(uuid4().bytes))
    connections = []
    try:
        async with repo, peer:
            if with_peer:
                outgoing, incoming = InMemoryTransport.create_pair()
                connections = [
                    asyncio.create_task(repo.connect(outgoing)),
                    asyncio.create_task(peer.accept(incoming)),
                ]

                async def wait_for_handshake():
                    while not all(
                        any(
                            isinstance(connection.state, ConnectionStateConnected)
                            for connection in current._hub.connections()
                        )
                        for current in (repo, peer)
                    ):
                        await asyncio.sleep(0.01)

                await asyncio.wait_for(wait_for_handshake(), timeout=2)

            assert await asyncio.wait_for(repo.find(missing), timeout=2) is None

            if with_peer:
                for connection in connections:
                    connection.cancel()
                await asyncio.gather(*connections, return_exceptions=True)

                async def wait_for_disconnect():
                    while repo._hub.connections() or peer._hub.connections():
                        await asyncio.sleep(0.01)

                await asyncio.wait_for(wait_for_disconnect(), timeout=2)
                # Disconnected single-use transports must not leave a pending
                # dialer that causes subsequent document searches to hang.
                assert await asyncio.wait_for(repo.find(missing), timeout=2) is None
    finally:
        for connection in connections:
            connection.cancel()
        await asyncio.gather(*connections, return_exceptions=True)
