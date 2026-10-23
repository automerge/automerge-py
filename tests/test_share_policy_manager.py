"""Policy subscription ownership without a repository or document actors."""

import asyncio

import pytest

from automerge._automerge import DocumentActorId, DocumentId, PeerId
from automerge.share_policy import ShareConfig, _SharePolicyManager


@pytest.fixture
def identity():
    return (
        DocumentActorId.new(),
        DocumentId.from_string("550e8400-e29b-41d4-a716-446655440000"),
        PeerId.random(),
    )


@pytest.mark.asyncio
async def test_decisions_and_cached_registration_are_applied_synchronously(identity):
    actor_id, document_id, peer_id = identity
    calls, applied = [], []
    decisions = [ShareConfig.read_only(), ShareConfig.read_write()]

    async def policy(document, peer):
        calls.append((document, peer))
        for config in decisions:
            yield config

    manager = _SharePolicyManager(policy, lambda *args: applied.append(args))
    try:
        manager.register(*identity)
        manager.register(*identity)
        await asyncio.wait_for(asyncio.gather(*manager._tasks), 5)
        assert calls == [(str(document_id), str(peer_id))]
        assert applied == [(actor_id, peer_id, config) for config in decisions]

        # Reconciliation uses peer identity, not Python object identity.
        manager.reconcile(actor_id, [PeerId.from_string(str(peer_id))])
        manager.register(*identity)
        assert applied[-1] == (actor_id, peer_id, decisions[-1])
        assert len(applied) == 3  # Cached application happened without an await.
        assert len(calls) == 1
    finally:
        await manager.close()


@pytest.mark.asyncio
async def test_shutdown_invalidates_before_cleanup_and_is_terminal(identity):
    started, cancelled, release, closed = (asyncio.Event() for _ in range(4))
    applied = []

    async def policy(document, peer):
        try:
            yield ShareConfig.read_only()
            started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cancelled.set()
                await release.wait()
                yield ShareConfig.read_write()
        finally:
            closed.set()

    manager = _SharePolicyManager(policy, lambda *args: applied.append(args))
    try:
        manager.register(*identity)
        await asyncio.wait_for(started.wait(), 5)
        subscription = next(iter(manager._subscriptions.values()))
        manager.begin_close()
        manager.begin_close()  # Must not cancel the same task twice.
        manager._publish(subscription, ShareConfig.none())
        manager.register(*identity)
        assert not manager._subscriptions
        assert len(applied) == 1

        stopping = asyncio.create_task(manager.close())
        await asyncio.wait_for(cancelled.wait(), 5)
        assert not stopping.done()
        release.set()
        await asyncio.wait_for(stopping, 5)
        assert closed.is_set()
        assert not manager._tasks
        assert len(applied) == 1  # The cancellation-resistant yield was ignored.
        manager.register(*identity)
        assert not manager._subscriptions
    finally:
        release.set()
        await manager.close()


@pytest.mark.asyncio
async def test_actor_teardown_leaves_other_actors_and_replacements_active(identity):
    actor_id, document_id, peer_id = identity
    other_actor = DocumentActorId.new()
    started, closed = asyncio.Queue(), []
    applied = []

    async def policy(document, peer):
        try:
            yield ShareConfig.read_only()
            started.put_nowait(True)
            await asyncio.Event().wait()
        finally:
            closed.append(True)

    manager = _SharePolicyManager(policy, lambda *args: applied.append(args))
    try:
        manager.register(*identity)
        manager.register(other_actor, document_id, peer_id)
        for _ in range(2):
            await asyncio.wait_for(started.get(), 5)
        old = manager._subscriptions[(actor_id, str(peer_id))]
        manager.begin_close(actor_id)
        manager._publish(old, ShareConfig.none())
        assert len(applied) == 2
        await asyncio.wait_for(manager.close(actor_id), 5)
        assert closed == [True]
        assert len(manager._subscriptions) == 1
        assert len(manager._tasks) == 1

        manager.register(*identity)
        await asyncio.wait_for(started.get(), 5)
        manager._publish(old, ShareConfig.none())
        assert len(applied) == 3
        assert applied[-1] == (actor_id, peer_id, ShareConfig.read_only())
        assert len(manager._subscriptions) == 2
    finally:
        await manager.close()


@pytest.mark.asyncio
async def test_close_awaits_cleanup_already_started_by_reconciliation(identity):
    actor_id, _, _ = identity
    started, closing, release, closed = (asyncio.Event() for _ in range(4))
    applied = []

    async def policy(document, peer):
        try:
            yield ShareConfig.read_only()
            started.set()
            await asyncio.Event().wait()
        finally:
            closing.set()
            await release.wait()
            closed.set()

    manager = _SharePolicyManager(policy, lambda *args: applied.append(args))
    try:
        manager.register(*identity)
        await asyncio.wait_for(started.wait(), 5)
        subscription = next(iter(manager._subscriptions.values()))
        manager.reconcile(actor_id, [])
        manager._publish(subscription, ShareConfig.none())
        assert not manager._subscriptions
        assert len(applied) == 1
        await asyncio.wait_for(closing.wait(), 5)

        stopping = asyncio.create_task(manager.close(actor_id))
        await asyncio.sleep(0)
        assert not stopping.done()
        release.set()
        await asyncio.wait_for(stopping, 5)
        assert closed.is_set()
        assert not manager._tasks
    finally:
        release.set()
        await manager.close()
