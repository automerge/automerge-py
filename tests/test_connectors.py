import asyncio

import pytest

from automerge._automerge import HubEvent
from automerge.repo import (
    Backoff,
    ConnFinishedReason,
    ConnectorClosedError,
    DialFailedError,
    PeerId,
    PermanentDialError,
    Repo,
)


class MemoryTransport:
    """Paired transports that signal EOF and count cleanup calls."""

    def __init__(self, incoming, outgoing):
        self.incoming = incoming
        self.outgoing = outgoing
        self.closed = False
        self.close_calls = 0

    @classmethod
    def pair(cls):
        a, b = asyncio.Queue(), asyncio.Queue()
        return cls(a, b), cls(b, a)

    async def send(self, message):
        if self.closed:
            raise ConnectionError("closed")
        await self.outgoing.put(message)

    async def recv(self):
        message = await self.incoming.get()
        if message is None:
            raise ConnectionError("EOF")
        return message

    async def close(self):
        self.close_calls += 1
        if not self.closed:
            self.closed = True
            await self.outgoing.put(None)


async def wait_until(predicate):
    async def wait():
        while not predicate():
            await asyncio.sleep(0.001)

    await asyncio.wait_for(wait(), timeout=5)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"initial_delay": -1},
        {"initial_delay": float("nan")},
        {"max_delay": float("inf")},
        {"max_delay": 0.01},
        {"initial_delay": True},
        {"max_retries": -1},
        {"max_retries": 0.5},
        {"max_retries": True},
        {"max_retries": 2**32},
    ],
)
def test_invalid_backoff(kwargs):
    with pytest.raises(ValueError):
        Backoff(**kwargs)


@pytest.mark.asyncio
async def test_registration_requires_running_repo_and_valid_inputs():
    repo = await Repo.load()
    with pytest.raises(ConnectorClosedError):
        await repo.add_listener("memory://server")
    async with repo:
        with pytest.raises(ValueError):
            await repo.add_listener("not a URL")
        with pytest.raises(TypeError):
            await repo.add_dialer("memory://server", connect=None)
        with pytest.raises(TypeError):
            await repo.add_dialer("memory://server", connect=lambda: None, backoff=1)
        with pytest.raises(TypeError):
            await repo.add_dialer(
                "memory://server", connect=lambda: None, expected_peer_id="peer"
            )
        assert not repo._listeners
        assert not repo._dialers


@pytest.mark.asyncio
async def test_listener_accepts_promptly_and_close_is_idempotent():
    repo = await Repo.load()
    async with repo:
        listener = await repo.add_listener("memory://server")
        _, incoming = MemoryTransport.pair()
        connection = await asyncio.wait_for(listener.accept(incoming), timeout=5)
        assert connection.peer_id is None
        assert connection in listener.connections
        async with connection:
            pass
        await asyncio.gather(connection.close(), connection.close())
        assert connection.closed
        assert incoming.close_calls == 1
        assert not listener.connections
        async with listener:
            pass
        await listener.close()
        with pytest.raises(ConnectorClosedError):
            await listener.accept(incoming)
        assert not repo._listeners


@pytest.mark.asyncio
async def test_dialer_connects_syncs_and_reconnects():
    repo, peer = await Repo.load(), await Repo.load()
    transports = []
    async with repo, peer:
        listener = await peer.add_listener("memory://server")

        async def connect():
            outgoing, incoming = MemoryTransport.pair()
            transports.extend((outgoing, incoming))
            await listener.accept(incoming, expected_peer_id=repo._peer_id)
            return outgoing

        dialer = await repo.add_dialer(
            "memory://server",
            connect=connect,
            expected_peer_id=peer._peer_id,
            backoff=Backoff(0.001, 0.01, 3),
        )
        async with dialer:
            first = await asyncio.wait_for(dialer.wait_connected(), timeout=5)
            assert first.peer_id == peer._peer_id
            assert await dialer.wait_connected() is first
            doc = await repo.create()
            with doc.change() as d:
                d["counter"] = 42
            restored = await asyncio.wait_for(peer.find(doc.url), timeout=5)
            assert restored.doc()["counter"] == 42
            await first.close()
            second = await asyncio.wait_for(dialer.wait_connected(), timeout=5)
            assert second is not first
            assert first.closed
            assert len(transports) == 4
        await listener.close()
        assert dialer.closed
        assert second.closed
        assert second.error is None
        assert await second.wait_closed() is ConnFinishedReason.WeDisconnected
        assert not repo._dialers
        assert all(transport.close_calls == 1 for transport in transports)


@pytest.mark.asyncio
@pytest.mark.parametrize("retries", [0, 2])
async def test_dialer_exhausts_retry_budget(retries):
    repo = await Repo.load()
    calls = 0

    async def connect():
        nonlocal calls
        calls += 1
        raise OSError("temporarily unavailable")

    async with repo:
        dialer = await repo.add_dialer(
            "memory://server", connect=connect, backoff=Backoff(0, 0, retries)
        )
        with pytest.raises(DialFailedError) as failed:
            await asyncio.wait_for(dialer.wait_connected(), timeout=5)
        assert isinstance(failed.value.__cause__, OSError)
        assert calls == retries + 1
        await dialer.close()


@pytest.mark.asyncio
async def test_permanent_factory_error_does_not_retry():
    repo = await Repo.load()
    calls = 0

    async def connect():
        nonlocal calls
        calls += 1
        raise PermanentDialError("bad credentials")

    async with repo:
        dialer = await repo.add_dialer("memory://server", connect=connect)
        with pytest.raises(DialFailedError) as failed:
            await asyncio.wait_for(dialer.wait_connected(), timeout=5)
        assert isinstance(failed.value.__cause__, PermanentDialError)
        await repo._dispatch_event(HubEvent.tick())
        assert calls == 1


@pytest.mark.asyncio
async def test_wait_timeout_leaves_dialer_running():
    repo, peer = await Repo.load(), await Repo.load()
    gate = asyncio.Event()
    async with repo, peer:
        listener = await peer.add_listener("memory://server")

        async def connect():
            await gate.wait()
            outgoing, incoming = MemoryTransport.pair()
            await listener.accept(incoming)
            return outgoing

        dialer = await repo.add_dialer("memory://server", connect=connect)
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(dialer.wait_connected(), timeout=0.01)
        assert not dialer.closed
        gate.set()
        conn = await asyncio.wait_for(dialer.wait_connected(), timeout=5)
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(conn.wait_closed(), timeout=0.01)
        await conn.close()
        await conn.wait_closed()


@pytest.mark.asyncio
async def test_close_cancels_pending_factory_and_wakes_waiters():
    repo = await Repo.load()
    entered, cancelled = asyncio.Event(), asyncio.Event()

    async def connect():
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    async with repo:
        dialer = await repo.add_dialer("memory://server", connect=connect)
        await asyncio.wait_for(entered.wait(), timeout=5)
        waiter = asyncio.create_task(dialer.wait_connected())
        await asyncio.gather(dialer.close(), dialer.close())
        assert cancelled.is_set()
        with pytest.raises(ConnectorClosedError):
            await waiter
        assert not repo._dialers


@pytest.mark.asyncio
@pytest.mark.parametrize("outgoing_identity", [False, True])
async def test_expected_peer_identity_is_enforced(outgoing_identity):
    repo, peer = await Repo.load(), await Repo.load()
    transports = []
    async with repo, peer:
        listener = await peer.add_listener("memory://server")
        wrong = PeerId.from_string("not-the-authenticated-peer")
        secret = await repo.create()
        with secret.change() as doc:
            doc["secret"] = 42

        async def connect():
            outgoing, incoming = MemoryTransport.pair()
            transports.extend((outgoing, incoming))
            await listener.accept(
                incoming,
                expected_peer_id=repo._peer_id if outgoing_identity else wrong,
            )
            return outgoing

        dialer = await repo.add_dialer(
            "memory://server",
            connect=connect,
            expected_peer_id=wrong if outgoing_identity else peer._peer_id,
            backoff=Backoff(max_retries=0),
        )
        with pytest.raises(DialFailedError):
            await asyncio.wait_for(dialer.wait_connected(), timeout=5)
        assert dialer.connection is None
        assert await asyncio.wait_for(peer.find(secret.url), timeout=5) is None
        await dialer.close()
        await listener.close()
        assert all(transport.close_calls == 1 for transport in transports)


@pytest.mark.asyncio
async def test_listener_groups_multiple_connections_and_repo_owns_resources():
    repo, peer = await Repo.load(), await Repo.load()
    transports = []
    async with repo, peer:
        listener = await peer.add_listener("memory://server")

        async def connect():
            outgoing, incoming = MemoryTransport.pair()
            transports.extend((outgoing, incoming))
            await listener.accept(incoming)
            return outgoing

        dialers = [
            await repo.add_dialer("memory://server", connect=connect) for _ in range(2)
        ]
        connections = await asyncio.wait_for(
            asyncio.gather(*(dialer.wait_connected() for dialer in dialers)), timeout=5
        )
        assert len(listener.connections) == 2
        await repo.stop()
        reasons = await asyncio.gather(
            *(connection.wait_closed() for connection in connections)
        )
        assert reasons == [ConnFinishedReason.Shutdown] * len(connections)
        await peer.stop()
        assert all(dialer.closed for dialer in dialers)
        assert all(connection.closed for connection in connections)
        assert listener.closed
        assert all(transport.close_calls == 1 for transport in transports)
        assert not repo._connections and not peer._connections
        assert not repo._send_tasks and not peer._recv_tasks


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["dialer", "listener"])
async def test_cancelled_registration_does_not_leak(kind):
    repo = await Repo.load()
    calls = 0

    async def connect():
        nonlocal calls
        calls += 1
        await asyncio.Event().wait()

    async with repo:
        if kind == "dialer":
            registration = repo.add_dialer("memory://server", connect=connect)
        else:
            registration = repo.add_listener("memory://server")
        task = asyncio.create_task(registration)
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        await repo._dispatch_event(HubEvent.tick())
        await wait_until(lambda: not repo._dialers and not repo._listeners)
        assert calls == 0


@pytest.mark.asyncio
async def test_cancellation_during_close_does_not_cancel_cleanup():
    repo = await Repo.load()
    began, release = asyncio.Event(), asyncio.Event()

    class SlowTransport(MemoryTransport):
        async def close(self):
            began.set()
            await release.wait()
            await super().close()

    async with repo:
        listener = await repo.add_listener("memory://server")
        transport = SlowTransport(asyncio.Queue(), asyncio.Queue())
        conn = await listener.accept(transport)
        closing = asyncio.create_task(listener.close())
        try:
            await asyncio.wait_for(began.wait(), timeout=5)
            closing.cancel()
            with pytest.raises(asyncio.CancelledError):
                await closing
        finally:
            release.set()
            await asyncio.wait_for(listener.close(), timeout=5)
        assert conn.closed
        assert transport.close_calls == 1


@pytest.mark.asyncio
async def test_transient_failure_recovers_and_resets_retry_budget():
    repo, peer = await Repo.load(), await Repo.load()
    calls = 0
    async with repo, peer:
        listener = await peer.add_listener("memory://server")

        async def connect():
            nonlocal calls
            calls += 1
            if calls == 1:
                raise OSError("retry me")
            outgoing, incoming = MemoryTransport.pair()
            await listener.accept(incoming)
            return outgoing

        dialer = await repo.add_dialer(
            "memory://server", connect=connect, backoff=Backoff(0, 0, 1)
        )
        first = await asyncio.wait_for(dialer.wait_connected(), timeout=5)
        assert calls == 2
        await first.close()
        second = await asyncio.wait_for(dialer.wait_connected(), timeout=5)
        assert second is not first
        assert calls == 3


@pytest.mark.asyncio
async def test_invalid_factory_result_reports_failure():
    repo = await Repo.load()

    async def connect():
        return None

    async with repo:
        dialer = await repo.add_dialer(
            "memory://server", connect=connect, backoff=Backoff(max_retries=0)
        )
        with pytest.raises(DialFailedError) as failed:
            await asyncio.wait_for(dialer.wait_connected(), timeout=5)
        assert isinstance(failed.value.__cause__, TypeError)


@pytest.mark.asyncio
async def test_cancelled_accept_closes_submitted_transport():
    repo = await Repo.load()
    async with repo:
        listener = await repo.add_listener("memory://server")
        _, incoming = MemoryTransport.pair()
        accepting = asyncio.create_task(listener.accept(incoming))
        await asyncio.sleep(0)
        accepting.cancel()
        with pytest.raises(asyncio.CancelledError):
            await accepting
        await repo._dispatch_event(HubEvent.tick())
        await wait_until(lambda: incoming.closed and not repo._connections)
        assert incoming.close_calls == 1
        assert not listener.connections
        assert not listener.closed


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["dialer", "listener"])
async def test_stop_drains_inflight_registration(kind):
    for _ in range(10):
        repo = await Repo.load()
        await repo.start()

        async def connect():
            await asyncio.Event().wait()

        if kind == "dialer":
            registering = repo.add_dialer("memory://server", connect=connect)
        else:
            registering = repo.add_listener("memory://server")
        registration = asyncio.create_task(registering)
        await asyncio.sleep(0)
        stopping = asyncio.create_task(repo.stop())
        resource, _ = await asyncio.wait_for(
            asyncio.gather(registration, stopping), timeout=5
        )
        assert resource.closed
        assert not repo._dialers and not repo._listeners
        assert not repo._pending_io_tasks
        with pytest.raises(ConnectorClosedError):
            await repo.start()


@pytest.mark.parametrize("method", ["connect", "accept"])
def test_repository_connection_convenience_methods_removed(method):
    assert not hasattr(Repo, method)
