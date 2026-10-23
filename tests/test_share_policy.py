"""Access-control regressions against real connected samod document actors."""

import asyncio

import pytest

from automerge._automerge import (
    CheckSharePolicyResultPayload,
    DocumentIoResultCheckSharePolicy,
    IoResult,
    IoTaskId,
)
from automerge.repo import (
    DocSearchPhase,
    PeerRequestState,
    Repo,
    ShareAccess,
    ShareConfig,
)


async def eventually(predicate):
    async def wait():
        while not predicate():
            await asyncio.sleep(0.01)

    await asyncio.wait_for(wait(), timeout=5)


def peer_state(repo, doc):
    for connection in repo._hub.connections():
        state = connection.doc_state(doc.document_id)
        if state is not None:
            return state
    return None


async def wait_config(repo, doc, config):
    await eventually(
        lambda: (
            (state := peer_state(repo, doc)) is not None
            and state.share_config == config
        )
    )


def test_policy_types_reexported_from_repo():
    from automerge import repo, share_policy

    for name in (
        "ShareAccess",
        "ShareConfig",
        "SharePolicy",
        "SharePolicyFunc",
        "SharePolicyType",
    ):
        assert getattr(repo, name) is getattr(share_policy, name)


def test_share_config_and_io_bindings():
    assert ShareConfig() == ShareConfig.read_write()
    assert ShareConfig.none().access == ShareAccess.None_
    assert not ShareConfig.none().announce
    assert ShareConfig.read_only().access == ShareAccess.ReadOnly
    assert ShareConfig.read_only().announce
    config = ShareConfig.read_only().with_announce(False)
    assert config == ShareConfig(ShareAccess.ReadOnly, False)
    with pytest.raises(AttributeError):
        config.announce = True
    with pytest.raises(TypeError):
        ShareConfig("read-only")
    task_id = IoTaskId.from_u32(7)
    for result in (
        IoResult.from_check_share_policy_result(task_id, config),
        IoResult.from_document_result(
            task_id, DocumentIoResultCheckSharePolicy(config)
        ),
    ):
        assert result.task_id == task_id
        assert isinstance(result.payload, CheckSharePolicyResultPayload)
        assert result.payload.config == config


@pytest.mark.asyncio
async def test_policy_forms():
    async def collect(repo):
        return [config async for config in repo._policies.stream("doc", "peer")]

    assert await collect(await Repo.load()) == [ShareConfig.read_write()]
    assert await collect(await Repo.load(share_policy=ShareConfig.none())) == [
        ShareConfig.none()
    ]
    calls = []

    async def callback(document_id, peer_id):
        calls.append((document_id, peer_id))
        yield ShareConfig.read_only()

    class Policy:
        share_config = staticmethod(callback)

    for policy in (callback, Policy()):
        assert await collect(await Repo.load(share_policy=policy)) == [
            ShareConfig.read_only()
        ]
    assert calls == [("doc", "peer"), ("doc", "peer")]

    with pytest.raises(TypeError, match="share_policy"):
        await Repo.load(share_policy=False)


@pytest.mark.asyncio
@pytest.mark.parametrize("bad_result", [None, True, "read-write", "exception"])
async def test_policy_errors_fail_closed(bad_result, caplog, memory_connection):
    async def policy(document_id, peer_id):
        if bad_result == "exception":
            raise RuntimeError("unavailable ACL service")
        yield bad_result

    repo = await Repo.load(share_policy=policy)
    reader = await Repo.load()
    async with repo, reader:
        doc = await repo.create()
        await memory_connection(reader, repo)
        await wait_config(repo, doc, ShareConfig.none())
        assert await asyncio.wait_for(reader.find(doc.url), 5) is None
        assert "Error in share policy" in caplog.text


class PolicyFeed:
    def __init__(self, initial):
        self.queue = asyncio.Queue()
        self.queue.put_nowait(initial)

    async def __call__(self, document_id, peer_id):
        while True:
            yield await self.queue.get()

    def send(self, config):
        self.queue.put_nowait(config)


@pytest.mark.asyncio
@pytest.mark.parametrize("access", ["hidden", "denied", "error"])
async def test_explicit_request_distinguishes_no_announce_from_no_access(
    access, memory_connection
):
    async def broken(document_id, peer_id):
        raise RuntimeError("ACL unavailable")
        yield  # Make this a failing async generator.

    config = {
        "hidden": ShareConfig.read_write().with_announce(False),
        "denied": ShareConfig.none().with_announce(True),
        "error": broken,
    }[access]
    owner = await Repo.load(share_policy=config)
    reader = await Repo.load()
    async with owner, reader:
        doc = await owner.create()
        with doc.change() as d:
            d["secret"] = 42
        await memory_connection(reader, owner)
        received = await asyncio.wait_for(reader.find(doc.url), timeout=5)
        if access == "hidden":
            assert received.doc()["secret"] == 42
        else:
            assert received is None


@pytest.mark.asyncio
async def test_read_only_sync_and_live_permission_changes(memory_connection):
    policy = PolicyFeed(ShareConfig.read_only())
    owner = await Repo.load(share_policy=policy)
    reader = await Repo.load()
    async with owner, reader:
        doc = await owner.create()
        with doc.change() as d:
            d["owner"] = 1
        await memory_connection(reader, owner)
        copy = await asyncio.wait_for(reader.find(doc.url), timeout=5)
        assert copy.doc()["owner"] == 1
        await wait_config(owner, doc, ShareConfig.read_only())
        await eventually(lambda: peer_state(reader, copy).peer_read_only)

        with copy.change() as d:
            d["remote_write"] = "must not arrive yet"
        with doc.change() as d:
            d["owner"] = 2
        await eventually(lambda: copy.doc()["owner"] == 2)
        assert "remote_write" not in list(doc.doc())

        # Read-only access still allows ephemeral traffic in both directions.
        owner_messages, reader_messages = [], []
        doc.on("ephemeral", owner_messages.append)
        copy.on("ephemeral", reader_messages.append)
        doc.broadcast(b"owner cursor")
        copy.broadcast(b"reader cursor")
        await eventually(lambda: owner_messages and reader_messages)
        assert owner_messages == [b"reader cursor"]
        assert reader_messages == [b"owner cursor"]

        # Granting writes resumes synchronization, including the peer's edits.
        policy.send(ShareConfig.read_write())
        await eventually(lambda: "remote_write" in list(doc.doc()))
        await eventually(lambda: not peer_state(reader, copy).peer_read_only)

        # Denial stops both directions without disconnecting the transport.
        policy.send(ShareConfig.none())
        await wait_config(owner, doc, ShareConfig.none())
        with doc.change() as d:
            d["after_denial"] = True
        with copy.change() as d:
            d["denied_write"] = True
        doc.broadcast(b"denied outgoing")
        copy.broadcast(b"denied incoming")
        await asyncio.sleep(0.15)
        assert "after_denial" not in list(copy.doc())
        assert "denied_write" not in list(doc.doc())
        assert owner_messages == [b"reader cursor"]
        assert reader_messages == [b"owner cursor"]

        policy.send(ShareConfig.read_write())
        await eventually(
            lambda: (
                "after_denial" in list(copy.doc()) and "denied_write" in list(doc.doc())
            )
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("config", [ShareConfig.none(), ShareConfig.read_only()])
async def test_local_policy_blocks_fetch_and_search_can_recover(
    config, memory_connection
):
    owner = await Repo.load()
    policy = PolicyFeed(config)
    reader = await Repo.load(share_policy=policy)
    async with owner, reader:
        doc = await owner.create()
        with doc.change() as d:
            d["value"] = 42
        await memory_connection(reader, owner)
        async with await reader.search_for_doc(doc.url) as search:
            assert await asyncio.wait_for(search.wait_result(), timeout=5) is None
            assert PeerRequestState.BlockedByPolicy in search.state.peers.values()
            policy.send(ShareConfig.read_write())
            await eventually(lambda: search.state.phase == DocSearchPhase.Ready)
            copy = await search.wait_result()
            assert copy.doc()["value"] == 42


@pytest.mark.asyncio
async def test_pending_initial_check_and_ordered_updates(memory_connection):
    started, release = asyncio.Event(), asyncio.Event()
    decisions = [ShareConfig.read_only(), ShareConfig.read_write(), ShareConfig.none()]
    applied = []

    async def policy(document_id, peer_id):
        started.set()
        await release.wait()
        for config in decisions:
            yield config

    owner = await Repo.load(share_policy=policy)
    original = owner._policies._publish

    def record(subscription, config):
        original(subscription, config)
        applied.append(subscription.current)

    owner._policies._publish = record
    reader = await Repo.load()
    async with owner, reader:
        doc = await owner.create()
        await memory_connection(reader, owner)
        await asyncio.wait_for(started.wait(), timeout=5)
        await eventually(lambda: peer_state(owner, doc) is not None)
        assert peer_state(owner, doc).share_config is None
        assert not applied
        release.set()
        await wait_config(owner, doc, ShareConfig.none())
        assert applied == decisions


@pytest.mark.asyncio
async def test_policy_is_per_document_and_peer_and_rechecked_on_reconnect(
    memory_connection,
):
    calls = []
    allowed = set()

    async def policy(document_id, peer_id):
        calls.append((document_id, peer_id))
        yield (
            ShareConfig.read_write()
            if (document_id, peer_id) in allowed
            else ShareConfig.none()
        )

    owner = await Repo.load(share_policy=policy)
    reader = await Repo.load()
    stranger = await Repo.load()
    async with owner, reader, stranger:
        shared, private = await owner.create(), await owner.create()
        allowed.add((str(shared.document_id), str(reader.peer_id)))
        dialer, listener = await memory_connection(reader, owner)
        await memory_connection(stranger, owner)
        assert await asyncio.wait_for(reader.find(shared.url), 5) is not None
        assert await asyncio.wait_for(reader.find(private.url), 5) is None
        assert await asyncio.wait_for(stranger.find(shared.url), 5) is None
        assert (str(private.document_id), str(reader.peer_id)) in calls
        assert (str(shared.document_id), str(stranger.peer_id)) in calls

        # Once the last connection closes, a reconnect evaluates a fresh stream.
        await dialer.close()
        await listener.close()
        before = calls.count((str(shared.document_id), str(reader.peer_id)))
        await memory_connection(reader, owner)
        await eventually(
            lambda: calls.count((str(shared.document_id), str(reader.peer_id))) > before
        )
        with shared.change() as d:
            d["reconnected"] = True
        copy = await reader.find(shared.url)
        await eventually(lambda: "reconnected" in list(copy.doc()))
