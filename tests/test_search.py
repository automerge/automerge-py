import asyncio
from uuid import uuid4

import pytest

from automerge import _automerge
from automerge._automerge import CommandResultSearchForDoc, HubEvent
from automerge.repo import (
    AutomergeUrl,
    ConnectorClosedError,
    DocSearchPhase,
    DocumentId,
    InMemoryStorage,
    PeerRequestState,
    Repo,
    Search,
    SearchClosedError,
)


class GatedStorage(InMemoryStorage):
    def __init__(self):
        super().__init__()
        self.block_reads = False
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def load_range(self, prefix):
        if self.block_reads:
            self.started.set()
            await self.release.wait()
        return await super().load_range(prefix)


async def stored_document():
    storage = GatedStorage()
    repo = await Repo.load(storage)
    async with repo:
        doc = await repo.create()
        with doc.change() as d:
            d["counter"] = 42
        url = doc.url
    return storage, url


async def wait_ready(stream):
    async for state in stream:
        if state.phase == DocSearchPhase.Ready:
            return state
    raise AssertionError("Search closed before readiness")


def missing_url():
    return AutomergeUrl.from_document_id(DocumentId.from_bytes(uuid4().bytes))


@pytest.mark.asyncio
async def test_search_requires_running_repo_and_valid_input():
    repo = await Repo.load()
    with pytest.raises(ConnectorClosedError):
        await repo.search_for_doc(missing_url())
    async with repo:
        with pytest.raises(ValueError):
            await repo.search_for_doc("not an automerge URL")
        with pytest.raises(TypeError):
            await repo.search_for_doc(None)
        assert not repo._searches


@pytest.mark.asyncio
async def test_native_search_command_returns_a_snapshot_without_compatibility():
    storage, url = await stored_document()
    repo = await Repo.load(storage)
    async with repo:
        storage.block_reads = True
        try:
            command = HubEvent.search_for_doc(url.document_id())
            result = await asyncio.wait_for(repo._dispatch_command(command), timeout=5)
            assert isinstance(result, CommandResultSearchForDoc)
            assert result.search_state.phase == DocSearchPhase.Loading
            assert not result.search_state.is_currently_unavailable()
            assert not hasattr(_automerge, "CommandResultFindDocument")
            assert not hasattr(HubEvent, "find_document")
            assert not hasattr(HubEvent, "create_connection")
        finally:
            storage.release.set()


@pytest.mark.asyncio
@pytest.mark.parametrize("input_kind", ["url", "string", "document_id"])
async def test_ready_search_and_find_delegate(input_kind):
    repo = await Repo.load()
    async with repo:
        original = await repo.create()
        with original.change() as d:
            d["counter"] = 42
        argument = {
            "url": original.url,
            "string": str(original.url),
            "document_id": original.url.document_id(),
        }[input_kind]
        search = await repo.search_for_doc(argument)
        assert isinstance(search, Search)
        async with search:
            restored = await asyncio.wait_for(search.wait_result(), timeout=5)
            assert restored.doc()["counter"] == 42
            assert search.url.document_id() == original.url.document_id()
            assert search.state.phase == DocSearchPhase.Ready
        assert search.closed
        assert not repo._searches
        calls = []
        native_search = repo.search_for_doc

        async def tracked(url):
            calls.append(url)
            return await native_search(url)

        repo.search_for_doc = tracked
        assert (await repo.find(original.url)).doc()["counter"] == 42
        assert calls == [original.url]
        assert not repo._searches


@pytest.mark.asyncio
async def test_loading_snapshots_and_multicast_updates_are_independent():
    storage, url = await stored_document()
    repo = await Repo.load(storage)
    async with repo:
        storage.block_reads = True
        search = await repo.search_for_doc(url)
        streams = [search.updates(), search.updates()]
        try:
            await asyncio.wait_for(storage.started.wait(), timeout=5)
            initial = await anext(streams[0])
            assert await anext(streams[1]) == initial
            assert initial.phase == DocSearchPhase.Loading
            with pytest.raises(AttributeError):
                initial.phase = DocSearchPhase.Ready
            pending = initial.pending_connections
            pending.append("memory://not-real")
            assert initial.pending_connections == []
            waiting = [asyncio.create_task(wait_ready(stream)) for stream in streams]
            storage.release.set()
            results = await asyncio.wait_for(asyncio.gather(*waiting), timeout=5)
            assert all(state.phase == DocSearchPhase.Ready for state in results)
            assert initial.phase == DocSearchPhase.Loading
            assert initial != search.state
        finally:
            storage.release.set()
            for stream in streams:
                await stream.aclose()
            await search.close()
        assert not search._subscribers
        assert not repo._searches


@pytest.mark.asyncio
async def test_concurrent_searches_closing_one_does_not_close_another():
    storage, url = await stored_document()
    repo = await Repo.load(storage)
    async with repo:
        storage.block_reads = True
        first = await repo.search_for_doc(url)
        second = await repo.search_for_doc(url)
        try:
            assert len(repo._searches[url.document_id()]) == 2
            waiting = asyncio.create_task(first.wait_result())
            await asyncio.sleep(0)
            await asyncio.gather(first.close(), first.close())
            with pytest.raises(SearchClosedError):
                await waiting
            assert not second.closed
            assert repo._searches[url.document_id()] == {second}
            storage.release.set()
            result = await asyncio.wait_for(second.wait_result(), timeout=5)
            assert result.doc()["counter"] == 42
        finally:
            storage.release.set()
            await asyncio.gather(first.close(), second.close())
        assert not repo._searches


@pytest.mark.asyncio
async def test_cancelling_waits_and_streams_does_not_cancel_search():
    storage, url = await stored_document()
    repo = await Repo.load(storage)
    async with repo:
        storage.block_reads = True
        search = await repo.search_for_doc(url)
        stream = search.updates()
        try:
            await anext(stream)
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(search.wait_result(), timeout=0.01)
            next_state = asyncio.create_task(anext(stream))
            await asyncio.sleep(0)
            next_state.cancel()
            with pytest.raises(asyncio.CancelledError):
                await next_state
            assert not search.closed
            assert not search._subscribers
            storage.release.set()
            assert (await search.wait_result()).doc()["counter"] == 42
        finally:
            storage.release.set()
            await stream.aclose()
            await search.close()


@pytest.mark.asyncio
async def test_timing_out_find_unsubscribes_only_its_own_handle(monkeypatch):
    storage, url = await stored_document()
    repo = await Repo.load(storage)
    async with repo:
        storage.block_reads = True
        live = await repo.search_for_doc(url)
        registered = asyncio.Event()
        native_search = repo.search_for_doc

        async def tracked_search(url):
            search = await native_search(url)
            registered.set()
            return search

        monkeypatch.setattr(repo, "search_for_doc", tracked_search)
        finding = asyncio.create_task(repo.find(url))
        try:
            # Time out the result wait, not registration: a short timer can fire
            # before registration finishes, especially on Windows.
            await asyncio.wait_for(registered.wait(), timeout=5)
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(finding, timeout=0.01)
            assert repo._searches[url.document_id()] == {live}
            storage.release.set()
            result = await asyncio.wait_for(live.wait_result(), timeout=5)
            assert result.doc()["counter"] == 42
        finally:
            storage.release.set()
            finding.cancel()
            await asyncio.gather(finding, return_exceptions=True)
            await live.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["search_for_doc", "find"])
async def test_cancellation_after_registration_before_receiving_handle(
    monkeypatch, method
):
    repo = await Repo.load()
    async with repo:
        url = missing_url()
        live = await repo.search_for_doc(url)
        dispatch = repo._dispatch_command
        created = []
        loop = asyncio.get_running_loop()

        async def cancel_after_result(command, *, on_result=None):
            def registered(result):
                search = on_result(result)
                created.append(search)
                # The hub sets the command future's result synchronously after
                # this callback. Cancel before the waiting caller resumes.
                loop.call_soon(registering.cancel)
                return search

            return await dispatch(command, on_result=registered)

        monkeypatch.setattr(repo, "_dispatch_command", cancel_after_result)
        registering = asyncio.create_task(getattr(repo, method)(url))
        try:
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(registering, timeout=5)
            assert len(created) == 1
            assert created[0].closed
            assert repo._searches[url.document_id()] == {live}
            assert not live.closed
        finally:
            registering.cancel()
            await asyncio.gather(registering, return_exceptions=True)
            await live.close()


@pytest.mark.asyncio
async def test_unavailable_search_can_later_become_ready(memory_connection):
    repo, source = await Repo.load(), await Repo.load()
    connections = []
    async with repo, source:
        doc = await source.create()
        with doc.change() as d:
            d["counter"] = 42
        search = await repo.search_for_doc(doc.url)
        stream = search.updates()
        try:
            assert await asyncio.wait_for(search.wait_result(), timeout=5) is None
            unavailable = search.state
            assert unavailable.is_currently_unavailable()
            assert unavailable.phase == DocSearchPhase.Searching
            states = []

            async def watch():
                async for state in stream:
                    states.append(state)
                    if state.phase == DocSearchPhase.Ready:
                        return

            watching = asyncio.create_task(watch())
            connections = await memory_connection(source, repo)
            await asyncio.wait_for(watching, timeout=5)
            result = await search.wait_result()
            assert result.doc()["counter"] == 42
            assert unavailable.is_currently_unavailable()
            assert any(
                state in (PeerRequestState.Requested, PeerRequestState.Syncing)
                for snapshot in states
                for state in snapshot.peers.values()
            )
            for snapshot in states:
                original_peers = snapshot.peers.copy()
                snapshot.peers.clear()
                assert snapshot.peers == original_peers
        finally:
            await stream.aclose()
            await search.close()
            await asyncio.gather(*(connector.close() for connector in connections))


@pytest.mark.asyncio
async def test_cancelled_registration_unsubscribes():
    repo = await Repo.load()
    async with repo:
        registering = asyncio.create_task(repo.search_for_doc(missing_url()))
        await asyncio.sleep(0)
        registering.cancel()
        with pytest.raises(asyncio.CancelledError):
            await registering
        await repo._dispatch_event(HubEvent.tick())
        for _ in range(100):
            if not repo._searches:
                break
            await asyncio.sleep(0.001)
        assert not repo._searches


@pytest.mark.asyncio
async def test_close_and_repo_stop_wake_waiters_and_end_streams():
    storage, url = await stored_document()
    repo = await Repo.load(storage)
    await repo.start()
    storage.block_reads = True
    search = await repo.search_for_doc(url)
    stream = search.updates()
    await anext(stream)
    waiting = asyncio.create_task(search.wait_result())
    next_state = asyncio.create_task(anext(stream))
    stopping = asyncio.create_task(repo.stop())
    try:
        with pytest.raises(SearchClosedError):
            await asyncio.wait_for(waiting, timeout=5)
        with pytest.raises(StopAsyncIteration):
            await asyncio.wait_for(next_state, timeout=5)
    finally:
        storage.release.set()
        await asyncio.wait_for(stopping, timeout=5)
        await stream.aclose()
    assert search.closed
    assert not search._subscribers
    assert not repo._searches
    assert search.state.phase == DocSearchPhase.Loading
    await search.close()
    with pytest.raises(SearchClosedError):
        await search.wait_result()
    with pytest.raises(SearchClosedError):
        async with search:
            pass
