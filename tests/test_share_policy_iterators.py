"""Policy stream ordering, ownership and cleanup against real document actors."""

import asyncio
import time

import pytest

from automerge._automerge import CheckSharePolicyAction, IoResult
from automerge.repo import Repo, ShareConfig
from test_share_policy import eventually, peer_state, wait_config


def configs(repo, doc):
    return [
        state.share_config
        for connection in repo._hub.connections()
        if (state := connection.doc_state(doc.document_id)) is not None
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "decision", [None, ShareConfig.none(), ShareConfig.read_only()]
)
async def test_exhaustion_retains_decision_and_is_shared(decision, memory_connection):
    calls, closed = [], []

    async def policy(document_id, peer_id):
        calls.append((document_id, peer_id))
        try:
            if decision is not None:
                yield decision
        finally:
            await asyncio.sleep(0)
            closed.append(True)

    owner = await Repo.load(share_policy=policy)
    reader = await Repo.load()
    expected = decision if decision is not None else ShareConfig.none()
    async with owner, reader:
        doc = await owner.create()
        await memory_connection(reader, owner)
        await wait_config(owner, doc, expected)
        await eventually(lambda: closed == [True])
        await memory_connection(reader, owner)
        await eventually(lambda: configs(owner, doc) == [expected, expected])
        assert calls == [(str(doc.document_id), str(reader.peer_id))]
        assert not owner._policies._tasks
        assert len(owner._policies._subscriptions) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, True, "read-write", "exception", "cancel"])
async def test_failure_after_grant_revokes_and_closes(
    failure, memory_connection, caplog
):
    fail, closed = asyncio.Event(), asyncio.Event()

    async def policy(document_id, peer_id):
        try:
            yield ShareConfig.read_write()
            await fail.wait()
            if failure == "exception":
                raise RuntimeError("ACL disconnected")
            if failure == "cancel":
                raise asyncio.CancelledError()
            yield failure
        finally:
            await asyncio.sleep(0)
            closed.set()

    owner = await Repo.load(share_policy=policy)
    reader = await Repo.load()
    async with owner, reader:
        doc = await owner.create()
        await memory_connection(reader, owner)
        copy = await asyncio.wait_for(reader.find(doc.url), 5)
        assert copy is not None
        fail.set()
        await wait_config(owner, doc, ShareConfig.none())
        await asyncio.wait_for(closed.wait(), 5)
        assert "Error in share policy" in caplog.text
        with doc.change() as d:
            d["private"] = True
        await asyncio.sleep(0.05)
        assert "private" not in list(copy.doc())


@pytest.mark.asyncio
async def test_one_shot_coroutine_is_rejected(memory_connection, caplog):
    async def old_policy(document_id, peer_id):
        return ShareConfig.read_write()

    owner = await Repo.load(share_policy=old_policy)
    reader = await Repo.load()
    async with owner, reader:
        doc = await owner.create()
        await memory_connection(reader, owner)
        await wait_config(owner, doc, ShareConfig.none())
        assert "must return AsyncIterator" in caplog.text


@pytest.mark.asyncio
async def test_shared_pending_subscription_and_connection_lifetime(memory_connection):
    calls, closed = [], []
    queue = asyncio.Queue()

    async def policy(document_id, peer_id):
        calls.append((document_id, peer_id))
        try:
            while True:
                yield await queue.get()
        finally:
            await asyncio.sleep(0)
            closed.append(True)

    owner = await Repo.load(share_policy=policy)
    reader = await Repo.load()
    async with owner, reader:
        doc = await owner.create()
        first = await memory_connection(reader, owner)
        second = await memory_connection(reader, owner)
        await eventually(lambda: configs(owner, doc) == [None, None])
        assert len(calls) == 1
        queue.put_nowait(ShareConfig.read_only())
        await eventually(lambda: configs(owner, doc) == [ShareConfig.read_only()] * 2)
        await asyncio.gather(*(resource.close() for resource in first))
        await eventually(lambda: len(configs(owner, doc)) == 1)
        assert not closed
        queue.put_nowait(ShareConfig.none())
        await wait_config(owner, doc, ShareConfig.none())
        third = await memory_connection(reader, owner)
        await eventually(lambda: configs(owner, doc) == [ShareConfig.none()] * 2)
        assert len(calls) == 1
        await asyncio.gather(*(resource.close() for resource in (*second, *third)))
        await eventually(lambda: closed == [True])
        assert not owner._policies._subscriptions
        await memory_connection(reader, owner)
        await eventually(lambda: len(calls) == 2)
        assert peer_state(owner, doc).share_config is None
        queue.put_nowait(ShareConfig.read_write())
        await wait_config(owner, doc, ShareConfig.read_write())
    assert closed == [True, True]


@pytest.mark.asyncio
@pytest.mark.parametrize("late_error", [False, True])
async def test_cancelled_stream_cannot_change_replacement(
    late_error, memory_connection, caplog
):
    calls = []
    cancelled, release, closed = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def policy(document_id, peer_id):
        calls.append(True)
        if len(calls) > 1:
            yield ShareConfig.read_only()
            return
        try:
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cancelled.set()
                # Simulate a policy backend returning a result after cancellation.
                await release.wait()
                if late_error:
                    raise RuntimeError("late error")
                yield ShareConfig.read_write()
        finally:
            closed.set()

    owner = await Repo.load(share_policy=policy)
    reader = await Repo.load()
    async with owner, reader:
        doc = await owner.create()
        resources = await memory_connection(reader, owner)
        await eventually(lambda: len(calls) == 1)
        old = next(iter(owner._policies._subscriptions.values()))
        await asyncio.gather(*(resource.close() for resource in resources))
        await asyncio.wait_for(cancelled.wait(), 5)
        await memory_connection(reader, owner)
        await wait_config(owner, doc, ShareConfig.read_only())
        release.set()
        await asyncio.wait_for(closed.wait(), 5)
        await eventually(lambda: not owner._policies._tasks)

        # There is no queue between the generation check and native application.
        owner._policies._publish(old, ShareConfig.none())
        assert peer_state(owner, doc).share_config == ShareConfig.read_only()
        assert len(calls) == 2
        if not late_error:
            assert "Error in share policy" not in caplog.text


@pytest.mark.asyncio
@pytest.mark.parametrize("initial", [False, True])
async def test_shutdown_cancels_waiting_iterator_and_awaits_aclose(
    initial, memory_connection
):
    waiting, closing, release, closed = (asyncio.Event() for _ in range(4))

    class Iterator:
        def __init__(self):
            self.first = initial

        def __aiter__(self):
            return self

        async def __anext__(self):
            if self.first:
                self.first = False
                return ShareConfig.read_write()
            waiting.set()
            await asyncio.Event().wait()

        async def aclose(self):
            closing.set()
            await release.wait()
            closed.set()

    owner = await Repo.load(share_policy=lambda doc, peer: Iterator())
    reader = await Repo.load()
    async with owner, reader:
        await owner.create()
        await memory_connection(reader, owner)
        await asyncio.wait_for(waiting.wait(), 5)
        stopping = asyncio.create_task(owner.stop())
        try:
            await asyncio.wait_for(closing.wait(), 5)
            assert not stopping.done()
        finally:
            release.set()
            await asyncio.wait_for(stopping, 5)
        assert closed.is_set()
        assert not owner._policies._subscriptions
        assert not owner._policies._tasks
        assert not owner._pending_io_tasks


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid_item", [False, True])
async def test_cleanup_in_progress_is_awaited_on_shutdown(
    invalid_item, memory_connection, caplog
):
    closing, release, closed = (asyncio.Event() for _ in range(3))

    class Iterator:
        def __init__(self):
            self.values = iter([ShareConfig.read_write(), None] if invalid_item else [])

        def __aiter__(self):
            return self

        async def __anext__(self):
            try:
                return next(self.values)
            except StopIteration:
                raise StopAsyncIteration

        async def aclose(self):
            closing.set()
            await release.wait()
            closed.set()

    owner = await Repo.load(share_policy=lambda doc, peer: Iterator())
    reader = await Repo.load()
    async with owner, reader:
        doc = await owner.create()
        await memory_connection(reader, owner)
        await asyncio.wait_for(closing.wait(), 5)
        # Invalid values revoke before potentially slow cleanup, not after it.
        await wait_config(owner, doc, ShareConfig.none())
        if invalid_item:
            assert "must yield ShareConfig" in caplog.text
        stopping = asyncio.create_task(owner.stop())
        try:
            await asyncio.sleep(0.01)
            assert not stopping.done()
        finally:
            release.set()
            await asyncio.wait_for(stopping, 5)
        assert closed.is_set()


@pytest.mark.asyncio
async def test_cleanup_error_denies_access(memory_connection, caplog):
    class Iterator:
        def __init__(self):
            self.first = True

        def __aiter__(self):
            return self

        async def __anext__(self):
            if self.first:
                self.first = False
                return ShareConfig.read_write()
            raise StopAsyncIteration

        async def aclose(self):
            raise RuntimeError("cleanup failed")

    owner = await Repo.load(share_policy=lambda doc, peer: Iterator())
    reader = await Repo.load()
    async with owner, reader:
        doc = await owner.create()
        await memory_connection(reader, owner)
        await wait_config(owner, doc, ShareConfig.none())
        assert "cleanup failed" in caplog.text


@pytest.mark.asyncio
async def test_samod_discards_initial_checks_after_stream_updates(memory_connection):
    ready, release = asyncio.Event(), asyncio.Event()
    checks = []

    async def policy(document_id, peer_id):
        ready.set()
        await release.wait()
        yield ShareConfig.read_write()
        yield ShareConfig.none()

    owner = await Repo.load(share_policy=policy)
    dispatch = owner._dispatch_actor_result

    def capture(actor_id, result):
        for io_task in result.io_tasks:
            if isinstance(io_task.action, CheckSharePolicyAction):
                checks.append((actor_id, io_task.task_id))
        dispatch(actor_id, result)

    owner._dispatch_actor_result = capture
    reader = await Repo.load()
    async with owner, reader:
        doc = await owner.create()
        await memory_connection(reader, owner)
        await asyncio.wait_for(ready.wait(), 5)
        release.set()
        await wait_config(owner, doc, ShareConfig.none())

        # Updates resolve samod's initial checks. Even a delayed grant returned
        # through the IO path must not overwrite the stream's latest decision.
        assert checks
        for actor_id, task_id in checks:
            result = owner._doc_actors[actor_id].handle_io_complete(
                time.time(),
                IoResult.from_check_share_policy_result(
                    task_id, ShareConfig.read_write()
                ),
            )
            assert not result.outgoing_messages
            owner._dispatch_actor_result(actor_id, result)
        assert peer_state(owner, doc).share_config == ShareConfig.none()


@pytest.mark.asyncio
async def test_stream_without_awaits_does_not_starve_shutdown(memory_connection):
    closed = asyncio.Event()

    async def policy(document_id, peer_id):
        try:
            while True:
                yield ShareConfig.read_only()
        finally:
            closed.set()

    owner = await Repo.load(share_policy=policy)
    reader = await Repo.load()
    async with owner, reader:
        doc = await owner.create()
        await memory_connection(reader, owner)
        await wait_config(owner, doc, ShareConfig.read_only())
        await asyncio.wait_for(owner.stop(), 5)
        assert closed.is_set()
        assert not owner._policies._tasks


@pytest.mark.asyncio
async def test_document_actor_teardown_closes_iterator(memory_connection):
    waiting, closed = asyncio.Event(), asyncio.Event()

    async def policy(document_id, peer_id):
        try:
            yield ShareConfig.read_write()
            waiting.set()
            await asyncio.Event().wait()
        finally:
            await asyncio.sleep(0)
            closed.set()

    owner = await Repo.load(share_policy=policy)
    reader = await Repo.load()
    async with owner, reader:
        doc = await owner.create()
        await memory_connection(reader, owner)
        await asyncio.wait_for(waiting.wait(), 5)
        subscription = next(iter(owner._policies._subscriptions.values()))
        actor_task = owner._doc_actor_tasks[doc._actor_id]
        actor_task.cancel()
        await asyncio.gather(actor_task, return_exceptions=True)
        assert closed.is_set()
        assert not owner._policies._subscriptions
        assert not owner._policies._tasks
        assert doc._actor_id not in owner._doc_actors
        owner._policies._publish(subscription, ShareConfig.none())
        # The transport remains connected; actor lifetime, not transport lifetime,
        # controls this subscription.
        assert owner._connections
