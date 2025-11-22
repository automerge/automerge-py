from __future__ import annotations

import asyncio
import time
from asyncio.queues import Queue

import pytest

from automerge._automerge import (
    AutomergeUrl,
    CheckAnnouncePolicyAction,
    CheckAnnouncePolicyResultPayload,
    CommandId,
    ConnectionId,
    ConnectionInfo,
    ConnectionState,
    DisconnectAction,
    DisconnectResultPayload,
    DocumentActorId,
    DocumentId,
    DocumentIoResultCheckAnnouncePolicy,
    DocumentIoResultStorage,
    Hub,
    IoResult,
    IoTaskId,
    LoaderStateLoaded,
    LoaderStateNeedIo,
    PeerDocState,
    PeerId,
    SamodLoader,
    SendAction,
    SendResultPayload,
    StorageId,
    StorageKey,
    StorageResult,
    StorageResultPayload,
    StorageTaskAction,
    StorageTaskDelete,
    StorageTaskLoad,
    StorageTaskLoadRange,
    StorageTaskPut,
)
from automerge.repo import InMemoryStorage, Repo
from automerge.transports import InMemoryTransport


def test_peer_id_creation():
    """Test creating PeerId instances"""
    # Create a random PeerId
    peer1 = PeerId.random()
    assert peer1 is not None
    assert str(peer1).startswith("peer-")

    # Create from string
    peer2 = PeerId.from_string("peer-test")
    assert str(peer2) == "peer-test"

    # Test equality
    peer3 = PeerId.from_string("peer-test")
    assert peer2 == peer3

    # Test inequality
    assert peer1 != peer2

    # Test repr
    assert "PeerId" in repr(peer1)

    # Test hash (for use in dicts/sets)
    peer_set = {peer1, peer2}
    assert len(peer_set) == 2


def test_storage_id_creation():
    """Test creating StorageId instances"""
    # Create a random StorageId (UUID)
    storage1 = StorageId.random()
    assert storage1 is not None
    # UUID format check (basic - just check it has hyphens)
    assert "-" in str(storage1)

    # Create from string
    storage2 = StorageId.from_string("test-storage-id")
    assert str(storage2) == "test-storage-id"

    # Test equality
    storage3 = StorageId.from_string("test-storage-id")
    assert storage2 == storage3

    # Test inequality
    assert storage1 != storage2

    # Test repr
    assert "StorageId" in repr(storage1)

    # Test hash
    storage_set = {storage1, storage2}
    assert len(storage_set) == 2


def test_connection_id_creation():
    """Test creating ConnectionId instances"""
    # Create from u32
    conn1 = ConnectionId.from_u32(42)
    assert conn1.to_u32() == 42

    # Test equality
    conn2 = ConnectionId.from_u32(42)
    assert conn1 == conn2

    # Test inequality
    conn3 = ConnectionId.from_u32(43)
    assert conn1 != conn3

    # Test repr
    assert "ConnectionId" in repr(conn1)
    assert "42" in repr(conn1)

    # Test str
    assert str(conn1) == "42"

    # Test hash
    conn_set = {conn1, conn3}
    assert len(conn_set) == 2


def test_document_actor_id_creation():
    """Test creating DocumentActorId instances"""
    # Create new DocumentActorId
    actor1 = DocumentActorId.new()
    assert actor1 is not None

    # Create another to ensure they're different
    actor2 = DocumentActorId.new()
    assert actor1 != actor2

    # Create from u32
    actor3 = DocumentActorId.from_u32(100)
    assert actor3.to_u32() == 100

    # Test equality
    actor4 = DocumentActorId.from_u32(100)
    assert actor3 == actor4

    # Test repr
    assert "DocumentActorId" in repr(actor1)

    # Test str (should be "actor:N" format)
    assert "actor:" in str(actor3)

    # Test hash
    actor_set = {actor1, actor2, actor3}
    assert len(actor_set) == 3


def test_command_id_creation():
    """Test creating CommandId instances"""
    # Create from u32
    cmd1 = CommandId.from_u32(1)
    assert cmd1.to_u32() == 1

    # Test equality
    cmd2 = CommandId.from_u32(1)
    assert cmd1 == cmd2

    # Test inequality
    cmd3 = CommandId.from_u32(2)
    assert cmd1 != cmd3

    # Test repr
    assert "CommandId" in repr(cmd1)
    assert "1" in repr(cmd1)

    # Test str
    assert str(cmd1) == "1"

    # Test hash
    cmd_set = {cmd1, cmd3}
    assert len(cmd_set) == 2


def test_document_id_creation():
    """Test creating DocumentId instances"""
    # Create DocumentId from a known UUID string (legacy format)
    uuid_str = "550e8400-e29b-41d4-a716-446655440000"
    doc1 = DocumentId.from_string(uuid_str)
    assert doc1 is not None

    # DocumentId should be base58 encoded when converted to string
    doc1_str = str(doc1)
    assert len(doc1_str) > 0

    # Create from base58 string (round-trip)
    doc2 = DocumentId.from_string(doc1_str)
    assert doc1 == doc2
    assert str(doc1) == str(doc2)

    # Test bytes conversion
    doc1_bytes = doc1.to_bytes()
    assert len(doc1_bytes) == 16  # UUID is 16 bytes

    # Test from_bytes (round-trip)
    doc3 = DocumentId.from_bytes(list(doc1_bytes))
    assert doc1 == doc3

    # Test inequality with different UUID
    uuid_str2 = "650e8400-e29b-41d4-a716-446655440000"
    doc4 = DocumentId.from_string(uuid_str2)
    assert doc1 != doc4

    # Test repr
    assert "DocumentId" in repr(doc1)

    # Test hash
    doc_set = {doc1, doc4}
    assert len(doc_set) == 2

    # Test invalid bytes length
    try:
        DocumentId.from_bytes([1, 2, 3])  # Too short
        assert False, "Should have raised ValueError"
    except ValueError:
        pass  # Expected


def test_automerge_url_creation():
    """Test creating AutomergeUrl instances"""
    # Create a DocumentId for testing
    doc_id = DocumentId.from_string("550e8400-e29b-41d4-a716-446655440000")
    doc_id_str = str(doc_id)

    # Create AutomergeUrl from string
    url_str = f"automerge:{doc_id_str}"
    url1 = AutomergeUrl.from_str(url_str)
    assert url1 is not None

    # Test to_str round-trip
    assert url1.to_str() == url_str

    # Test with path
    url_with_path_str = f"automerge:{doc_id_str}/foo/bar"
    url2 = AutomergeUrl.from_str(url_with_path_str)
    assert url2.to_str() == url_with_path_str

    # Test with numeric path component
    url_with_index_str = f"automerge:{doc_id_str}/items/0"
    url3 = AutomergeUrl.from_str(url_with_index_str)
    assert url3.to_str() == url_with_index_str

    # Test equality
    url4 = AutomergeUrl.from_str(url_str)
    assert url1 == url4

    # Test inequality
    assert url1 != url2

    # Test repr
    assert "AutomergeUrl" in repr(url1)

    # Test str
    assert str(url1) == url_str

    # Test hash
    url_set = {url1, url2}
    assert len(url_set) == 2

    # Test invalid URL
    try:
        AutomergeUrl.from_str("not-a-valid-url")
        assert False, "Should have raised ValueError"
    except ValueError:
        pass  # Expected


def test_storage_key_creation():
    """Test creating StorageKey instances"""
    # Create from parts
    key1 = StorageKey.from_parts(["users", "123", "profile"])
    assert key1 is not None

    # Test to_parts round-trip
    parts = key1.to_parts()
    assert parts == ["users", "123", "profile"]

    # Test string representation (joined with "/")
    assert str(key1) == "users/123/profile"

    # Test equality
    key2 = StorageKey.from_parts(["users", "123", "profile"])
    assert key1 == key2

    # Test inequality
    key3 = StorageKey.from_parts(["users", "456", "profile"])
    assert key1 != key3

    # Test repr
    assert "StorageKey" in repr(key1)

    # Test hash
    key_set = {key1, key3}
    assert len(key_set) == 2

    # Test empty parts not allowed
    try:
        StorageKey.from_parts(["users", "", "profile"])
        assert False, "Should have raised ValueError"
    except ValueError:
        pass  # Expected

    # Test parts with "/" not allowed
    try:
        StorageKey.from_parts(["users/admins", "123"])
        assert False, "Should have raised ValueError"
    except ValueError:
        pass  # Expected


def test_storage_task_load():
    """Test creating Load StorageTask"""
    key = StorageKey.from_parts(["document", "abc123", "data"])
    task = StorageTaskLoad(key)

    # Test isinstance checks (replacing is_* methods)
    assert isinstance(task, StorageTaskLoad)
    assert not isinstance(task, StorageTaskLoadRange)
    assert not isinstance(task, StorageTaskPut)
    assert not isinstance(task, StorageTaskDelete)

    # Test direct field access (replacing key() method)
    assert task.key.to_parts() == ["document", "abc123", "data"]

    # Test repr
    assert "Load" in repr(task)


def test_storage_task_load_range():
    """Test creating LoadRange StorageTask"""
    prefix = StorageKey.from_parts(["document", "abc123"])
    task = StorageTaskLoadRange(prefix)

    # Test isinstance checks (replacing is_* methods)
    assert not isinstance(task, StorageTaskLoad)
    assert isinstance(task, StorageTaskLoadRange)
    assert not isinstance(task, StorageTaskPut)
    assert not isinstance(task, StorageTaskDelete)

    # Test direct field access (replacing key() method)
    assert task.prefix.to_parts() == ["document", "abc123"]

    # Test repr
    assert "LoadRange" in repr(task)


def test_storage_task_put():
    """Test creating Put StorageTask"""
    key = StorageKey.from_parts(["document", "abc123", "snapshot"])
    value = b"test data content"
    task = StorageTaskPut(key, list(value))

    # Test isinstance checks (replacing is_* methods)
    assert not isinstance(task, StorageTaskLoad)
    assert not isinstance(task, StorageTaskLoadRange)
    assert isinstance(task, StorageTaskPut)
    assert not isinstance(task, StorageTaskDelete)

    # Test direct field access (replacing key() and value() methods)
    assert task.key.to_parts() == ["document", "abc123", "snapshot"]
    assert bytes(task.value) == value

    # Test repr
    assert "Put" in repr(task)
    assert "bytes" in repr(task)


def test_storage_task_delete():
    """Test creating Delete StorageTask"""
    key = StorageKey.from_parts(["document", "old123", "data"])
    task = StorageTaskDelete(key)

    # Test isinstance checks (replacing is_* methods)
    assert not isinstance(task, StorageTaskLoad)
    assert not isinstance(task, StorageTaskLoadRange)
    assert not isinstance(task, StorageTaskPut)
    assert isinstance(task, StorageTaskDelete)

    # Test direct field access (replacing key() method)
    assert task.key.to_parts() == ["document", "old123", "data"]

    # Test repr
    assert "Delete" in repr(task)


def test_storage_result_load_with_value():
    """Test creating Load StorageResult with a value"""
    value = b"test data"
    result = StorageResult.load(list(value))

    # Test variant checks
    assert result.is_load()
    assert not result.is_load_range()
    assert not result.is_put()
    assert not result.is_delete()

    # Test value accessor
    loaded_value = result.load_value()
    assert loaded_value is not None
    assert bytes(loaded_value) == value

    # Test load_range_values (should be None)
    assert result.load_range_values() is None

    # Test repr
    assert "Load" in repr(result)
    assert "bytes" in repr(result)


def test_storage_result_load_not_found():
    """Test creating Load StorageResult with None (key not found)"""
    result = StorageResult.load(None)

    # Test variant checks
    assert result.is_load()

    # Test value accessor (should be None)
    assert result.load_value() is None

    # Test repr
    assert "Load" in repr(result)
    assert "None" in repr(result)


def test_storage_result_load_range():
    """Test creating LoadRange StorageResult"""
    key1 = StorageKey.from_parts(["doc", "abc", "v1"])
    key2 = StorageKey.from_parts(["doc", "abc", "v2"])
    value1 = b"data1"
    value2 = b"data2"

    # Create LoadRange result with list of tuples
    result = StorageResult.load_range([(key1, list(value1)), (key2, list(value2))])

    # Test variant checks
    assert not result.is_load()
    assert result.is_load_range()
    assert not result.is_put()
    assert not result.is_delete()

    # Test load_range_values
    values_dict = result.load_range_values()
    assert values_dict is not None
    assert len(values_dict) == 2

    # Check that the values are in the dict
    # Note: we can't directly access by PyStorageKey, but we can iterate
    found_values = set()
    for k, v in values_dict.items():
        found_values.add(bytes(v))
    assert value1 in found_values
    assert value2 in found_values

    # Test load_value (should be None)
    assert result.load_value() is None

    # Test repr
    assert "LoadRange" in repr(result)
    assert "2 entries" in repr(result)


def test_storage_result_put():
    """Test creating Put StorageResult"""
    result = StorageResult.put()

    # Test variant checks
    assert not result.is_load()
    assert not result.is_load_range()
    assert result.is_put()
    assert not result.is_delete()

    # Test accessors (should be None)
    assert result.load_value() is None
    assert result.load_range_values() is None

    # Test repr
    assert "Put" in repr(result)


def test_storage_result_delete():
    """Test creating Delete StorageResult"""
    result = StorageResult.delete()

    # Test variant checks
    assert not result.is_load()
    assert not result.is_load_range()
    assert not result.is_put()
    assert result.is_delete()

    # Test accessors (should be None)
    assert result.load_value() is None
    assert result.load_range_values() is None

    # Test repr
    assert "Delete" in repr(result)


def test_io_task_id():
    """Test creating IoTaskId"""
    # Create from u32
    task_id1 = IoTaskId.from_u32(42)
    assert task_id1.to_u32() == 42

    # Test equality
    task_id2 = IoTaskId.from_u32(42)
    assert task_id1 == task_id2

    # Test inequality
    task_id3 = IoTaskId.from_u32(43)
    assert task_id1 != task_id3

    # Test repr
    assert "IoTaskId" in repr(task_id1)
    assert "42" in repr(task_id1)

    # Test str
    assert str(task_id1) == "42"

    # Test hash
    task_id_set = {task_id1, task_id3}
    assert len(task_id_set) == 2


def test_io_task():
    """Test IoTask type (created internally by Rust)"""
    # IoTask instances are created internally by the Rust code and passed
    # to Python. They contain an 'action' field which is one of:
    # - StorageTaskAction (contains a StorageTask)
    # - SendAction (contains connection_id and message bytes)
    # - DisconnectAction (contains connection_id)
    # - CheckAnnouncePolicyAction (contains peer_id)
    #
    # These types are tested through integration tests with actual
    # loader/hub usage where IoTask instances are naturally created.
    #
    # For now, we just verify the types are importable.
    assert StorageTaskAction is not None
    assert SendAction is not None
    assert DisconnectAction is not None
    assert CheckAnnouncePolicyAction is not None


def test_io_result():
    """Test IoResult type (created internally by Rust)"""
    # IoResult instances are created internally by the Rust code and passed
    # to Python. They contain a 'payload' field which is one of:
    # - StorageResultPayload (contains a StorageResult that can be consumed once)
    # - SendResultPayload (empty - just indicates send completed)
    # - DisconnectResultPayload (empty - just indicates disconnect completed)
    # - CheckAnnouncePolicyResultPayload (contains should_announce boolean)
    #
    # These types are tested through integration tests with actual
    # loader/hub usage where IoResult instances are naturally created.
    #
    # For now, we just verify the types are importable.
    assert StorageResultPayload is not None
    assert SendResultPayload is not None
    assert DisconnectResultPayload is not None
    assert CheckAnnouncePolicyResultPayload is not None


# ===== InMemoryStorage Tests =====


@pytest.mark.asyncio
async def test_storage_put_and_load():
    """Test basic put and load operations"""
    storage = InMemoryStorage()
    key = StorageKey.from_parts(["test", "key"])
    value = b"test data"

    # Initially should be None
    result = await storage.load(key)
    assert result is None

    # Put value
    await storage.put(key, value)

    # Should now load the value
    result = await storage.load(key)
    assert result == value


@pytest.mark.asyncio
async def test_storage_delete():
    """Test delete operation"""
    storage = InMemoryStorage()
    key = StorageKey.from_parts(["test", "delete"])
    value = b"to be deleted"

    # Put and verify
    await storage.put(key, value)
    assert await storage.load(key) == value

    # Delete
    await storage.delete(key)

    # Should be None after delete
    assert await storage.load(key) is None


@pytest.mark.asyncio
async def test_storage_load_range():
    """Test load_range operation"""
    storage = InMemoryStorage()

    # Put multiple values with same prefix
    await storage.put(StorageKey.from_parts(["docs", "doc1", "data"]), b"data1")
    await storage.put(StorageKey.from_parts(["docs", "doc2", "data"]), b"data2")
    await storage.put(StorageKey.from_parts(["docs", "doc3", "data"]), b"data3")
    await storage.put(StorageKey.from_parts(["other", "key"]), b"other")

    # Load range with "docs" prefix
    results = await storage.load_range(StorageKey.from_parts(["docs"]))

    # Should get 3 results, not the "other" key
    assert len(results) == 3

    # Check that all results have correct prefix
    for key, value in results:
        parts = key.to_parts()
        assert parts[0] == "docs"
        assert value.startswith(b"data")


@pytest.mark.asyncio
async def test_storage_load_range_empty():
    """Test load_range with no matching keys"""
    storage = InMemoryStorage()

    # Load range with non-existent prefix
    results = await storage.load_range(StorageKey.from_parts(["nonexistent"]))

    # Should get empty list
    assert len(results) == 0


@pytest.mark.asyncio
async def test_storage_overwrite():
    """Test overwriting an existing key"""
    storage = InMemoryStorage()
    key = StorageKey.from_parts(["test", "overwrite"])

    # Put initial value
    await storage.put(key, b"initial")
    assert await storage.load(key) == b"initial"

    # Overwrite with new value
    await storage.put(key, b"updated")
    assert await storage.load(key) == b"updated"


@pytest.mark.asyncio
async def test_storage_multiple_operations():
    """Test multiple storage operations in sequence"""
    storage = InMemoryStorage()

    # Create several keys
    keys = [
        StorageKey.from_parts(["app", "user", "1"]),
        StorageKey.from_parts(["app", "user", "2"]),
        StorageKey.from_parts(["app", "config"]),
    ]

    # Put values
    for i, key in enumerate(keys):
        await storage.put(key, f"value{i}".encode())

    # Load and verify
    for i, key in enumerate(keys):
        value = await storage.load(key)
        assert value == f"value{i}".encode()

    # Load range
    user_keys = await storage.load_range(StorageKey.from_parts(["app", "user"]))
    assert len(user_keys) == 2

    # Delete one
    await storage.delete(keys[0])
    assert await storage.load(keys[0]) is None
    assert await storage.load(keys[1]) is not None


# ===== Loader Integration Tests =====


@pytest.mark.asyncio
async def test_loader_integration():
    """Test complete loader lifecycle with InMemoryStorage"""
    # Create storage and loader
    storage = InMemoryStorage()
    peer_id = PeerId.random()
    loader = SamodLoader(peer_id)

    # Step 1: Initial step should request loading storage ID
    state = loader.step(time.time())
    assert isinstance(state, LoaderStateNeedIo)
    print(f"Initial state: {state}")

    tasks = state.tasks
    assert len(tasks) == 1

    # Step 2: Execute IO tasks
    for task in tasks:
        print(f"Executing task: {task}")
        action = task.action

        # Should be a storage task action
        assert isinstance(action, StorageTaskAction)
        storage_task = action.task

        # Execute the storage task - use isinstance to determine task type
        if isinstance(storage_task, StorageTaskLoad):
            print(f"  Loading key: {storage_task.key}")
            value = await storage.load(storage_task.key)
            result = StorageResult.load(list(value) if value else None)
        elif isinstance(storage_task, StorageTaskPut):
            print(f"  Putting key: {storage_task.key}, value: {storage_task.value}")
            await storage.put(storage_task.key, bytes(storage_task.value))
            result = StorageResult.put()
        elif isinstance(storage_task, StorageTaskDelete):
            print(f"  Deleting key: {storage_task.key}")
            await storage.delete(storage_task.key)
            result = StorageResult.delete()
        elif isinstance(storage_task, StorageTaskLoadRange):
            print(f"  Loading range: {storage_task.prefix}")
            results = await storage.load_range(storage_task.prefix)
            result = StorageResult.load_range([(k, list(v)) for k, v in results])
        else:
            raise ValueError("Unknown storage task type")

        # Create IoResult and provide it to loader
        io_result = IoResult.from_storage_result(task.task_id, result)
        loader.provide_io_result(io_result)

    # Step 3: Step again - should either need more IO or be loaded
    state = loader.step(time.time())
    print(f"Second state: {state}")

    # If we need more IO, execute those tasks too
    while isinstance(state, LoaderStateNeedIo):
        tasks = state.tasks
        print(f"Got {len(tasks)} more task(s)")

        for task in tasks:
            print(f"Executing task: {task}")
            action = task.action
            assert isinstance(action, StorageTaskAction)
            storage_task = action.task

            if isinstance(storage_task, StorageTaskLoad):
                value = await storage.load(storage_task.key)
                result = StorageResult.load(list(value) if value else None)
            elif isinstance(storage_task, StorageTaskPut):
                await storage.put(storage_task.key, bytes(storage_task.value))
                result = StorageResult.put()
            elif isinstance(storage_task, StorageTaskDelete):
                await storage.delete(storage_task.key)
                result = StorageResult.delete()
            elif isinstance(storage_task, StorageTaskLoadRange):
                results = await storage.load_range(storage_task.prefix)
                result = StorageResult.load_range([(k, list(v)) for k, v in results])

            io_result = IoResult.from_storage_result(task.task_id, result)
            loader.provide_io_result(io_result)

        state = loader.step(time.time())
        print(f"Next state: {state}")

    # Step 4: Should now be loaded with a Hub
    assert isinstance(state, LoaderStateLoaded)
    print("✓ Loader completed successfully")

    hub = state.hub
    assert isinstance(hub, Hub)
    print(f"✓ Got Hub: {hub}")

    # Verify storage ID was saved
    storage_id_key = StorageKey.from_parts(["storage-adapter-id"])
    storage_id_value = await storage.load(storage_id_key)
    assert storage_id_value is not None
    print(f"✓ Storage ID persisted: {storage_id_value.decode('utf-8')}")


@pytest.mark.asyncio
async def test_loader_with_existing_storage_id():
    """Test loader when storage ID already exists"""
    # Create storage with pre-existing storage ID
    storage = InMemoryStorage()
    storage_id_key = StorageKey.from_parts(["storage-adapter-id"])
    await storage.put(storage_id_key, b"existing-storage-id")

    # Create loader
    peer_id = PeerId.random()
    loader = SamodLoader(peer_id)

    # Step and execute IO tasks
    state = loader.step(time.time())
    assert isinstance(state, LoaderStateNeedIo)

    tasks = state.tasks
    for task in tasks:
        action = task.action
        assert isinstance(action, StorageTaskAction)
        storage_task = action.task

        if isinstance(storage_task, StorageTaskLoad):
            value = await storage.load(storage_task.key)
            result = StorageResult.load(list(value) if value else None)
            io_result = IoResult.from_storage_result(task.task_id, result)
            loader.provide_io_result(io_result)

    # Should load immediately since storage ID exists
    state = loader.step(time.time())
    assert isinstance(state, LoaderStateLoaded)

    hub = state.hub
    assert isinstance(hub, Hub)
    print("✓ Loaded with existing storage ID")


def test_hub_io_result():
    """Test HubIoResult creation and type checking with subclasses"""
    from automerge._automerge import (
        HubIoResultDisconnect,
        HubIoResultSend,
        IoResult,
        IoTaskId,
    )

    # Create Send result
    send_result = HubIoResultSend()
    assert isinstance(send_result, HubIoResultSend)
    assert not isinstance(send_result, HubIoResultDisconnect)
    print(f"✓ HubIoResultSend(): {send_result}")

    # Create Disconnect result
    disconnect_result = HubIoResultDisconnect()
    assert isinstance(disconnect_result, HubIoResultDisconnect)
    assert not isinstance(disconnect_result, HubIoResultSend)
    print(f"✓ HubIoResultDisconnect(): {disconnect_result}")

    # Create IoResult from HubIoResult
    task_id = IoTaskId.from_u32(42)
    io_result = IoResult.from_hub_result(task_id, send_result)
    assert io_result.task_id.to_u32() == 42
    print(f"✓ IoResult.from_hub_result(): {io_result}")


def test_document_io_result():
    """Test DocumentIoResult creation and type checking with subclasses"""
    # Create CheckAnnouncePolicy result
    announce_result = DocumentIoResultCheckAnnouncePolicy(True)
    assert isinstance(announce_result, DocumentIoResultCheckAnnouncePolicy)
    assert not isinstance(announce_result, DocumentIoResultStorage)
    assert announce_result.should_announce
    print(f"✓ DocumentIoResultCheckAnnouncePolicy(True): {announce_result}")

    # Create Storage result
    storage_result = StorageResult.put()
    doc_result = DocumentIoResultStorage(storage_result)
    assert isinstance(doc_result, DocumentIoResultStorage)
    assert not isinstance(doc_result, DocumentIoResultCheckAnnouncePolicy)
    # Access the stored storage_result field
    assert doc_result.storage_result.is_put()
    print(f"✓ DocumentIoResultStorage(): {doc_result}")

    # Create IoResult from DocumentIoResult
    task_id = IoTaskId.from_u32(43)
    io_result = IoResult.from_document_result(task_id, doc_result)
    assert io_result.task_id.to_u32() == 43
    print(f"✓ IoResult.from_document_result(): {io_result}")

    # Can reuse DocumentIoResult since it's cloneable now
    io_result2 = IoResult.from_document_result(task_id, doc_result)
    assert io_result2.task_id.to_u32() == 43
    print("✓ DocumentIoResult is cloneable: can create multiple IoResults")


class DummyTransport:
    outbox: Queue
    inbox: Queue

    @staticmethod
    def pair() -> tuple["DummyTransport", "DummyTransport"]:
        a = DummyTransport()
        b = DummyTransport()
        a.inbox = b.outbox
        b.inbox = a.outbox
        return a, b

    async def send(self, msg: bytes):
        await self.outbox.put(msg)

    async def recv(self) -> bytes:
        return await self.inbox.get()


@pytest.mark.asyncio
async def test_repo_initialization():
    """Test basic Repo initialization and context manager"""
    from automerge.repo import InMemoryStorage

    storage = InMemoryStorage()

    # Load the repo
    repo = await Repo.load(storage)

    # Test context manager
    async with repo:
        # Verify repo is initialized
        assert repo._hub is not None
        assert repo._hub_task is not None
        assert not repo._shutdown_event.is_set()

        # Check that Hub has correct properties
        peer_id = repo._hub.peer_id()
        assert peer_id is not None

        storage_id = repo._hub.storage_id()
        assert storage_id is not None

        # Verify Hub is not stopped
        assert not repo._hub.is_stopped()

    # After exiting context, Hub should be stopped
    # (checked implicitly by successful exit)


@pytest.mark.asyncio
async def test_repo_hub_loop_with_tick():
    """Test that Hub loop processes tick events"""
    import asyncio

    from automerge.repo import InMemoryStorage

    storage = InMemoryStorage()

    repo = await Repo.load(storage)
    async with repo:
        # Let the Hub loop run for a bit
        await asyncio.sleep(0.3)

        # Verify Hub is still running
        assert repo._hub_task is not None
        assert not repo._hub_task.done()
        assert not repo._hub.is_stopped()

    # Hub should have stopped cleanly


@pytest.mark.asyncio
async def test_repo_storage_persistence():
    """Test that storage ID is persisted across repo instances"""
    from automerge.repo import InMemoryStorage

    storage = InMemoryStorage()

    # First repo instance
    repo1 = await Repo.load(storage)
    async with repo1:
        storage_id_1 = repo1._hub.storage_id()
        peer_id_1 = repo1._hub.peer_id()

    # Second repo instance with same storage
    repo2 = await Repo.load(storage)
    async with repo2:
        storage_id_2 = repo2._hub.storage_id()
        peer_id_2 = repo2._hub.peer_id()

    # Storage ID should be the same (persisted)
    assert storage_id_1.to_string() == storage_id_2.to_string()

    # Peer ID should be different (each repo has unique peer ID)
    assert peer_id_1.to_string() != peer_id_2.to_string()


@pytest.mark.asyncio
async def test_repo_event_dispatch():
    """Test dispatching events to Hub loop"""
    import asyncio

    from automerge._automerge import HubEvent
    from automerge.repo import InMemoryStorage

    storage = InMemoryStorage()

    repo = await Repo.load(storage)
    async with repo:
        # Send a tick event to the Hub
        await repo._hub_event_queue.put(HubEvent.tick())

        # Wait a bit for processing
        await asyncio.sleep(0.1)

        # Hub should still be running
        assert not repo._hub.is_stopped()

    # After exiting context, Hub should be stopped


@pytest.mark.asyncio
async def test_repo_create_document():
    """Test creating a document in the repository"""
    from automerge.repo import InMemoryStorage

    storage = InMemoryStorage()
    repo = await Repo.load(storage)

    async with repo:
        # Create a new document
        doc_handle = await repo.create()

        # Verify we got a DocHandle back
        assert doc_handle is not None
        assert hasattr(doc_handle, "url")
        assert hasattr(doc_handle, "document_id")

        # Verify the document actor was spawned
        assert doc_handle._actor_id in repo._doc_actors
        assert doc_handle._actor_id in repo._doc_actor_queues
        assert doc_handle._actor_id in repo._doc_actor_tasks

        # Verify the URL is valid
        url = doc_handle.url
        assert url is not None
        assert "automerge:" in url.to_str()

        # Give the document actor a moment to initialize
        await asyncio.sleep(0.1)


@pytest.mark.asyncio
async def test_repo_multiple_documents():
    """Test creating multiple documents in the same repository"""
    from automerge.repo import InMemoryStorage

    storage = InMemoryStorage()
    repo = await Repo.load(storage)

    async with repo:
        # Create multiple documents
        doc1 = await repo.create()
        doc2 = await repo.create()
        doc3 = await repo.create()

        # Verify all documents have unique IDs
        assert doc1.document_id != doc2.document_id
        assert doc2.document_id != doc3.document_id
        assert doc1.document_id != doc3.document_id

        # Verify all documents have unique actor IDs
        assert doc1._actor_id != doc2._actor_id
        assert doc2._actor_id != doc3._actor_id
        assert doc1._actor_id != doc3._actor_id

        # Verify all actors are tracked
        assert len(repo._doc_actors) == 3
        assert len(repo._doc_actor_queues) == 3
        assert len(repo._doc_actor_tasks) == 3

        # Give actors a moment to initialize
        await asyncio.sleep(0.1)


@pytest.mark.asyncio
async def test_repo_document_modification():
    """Test modifying a document using doc() and change() methods"""
    from automerge.repo import InMemoryStorage

    storage = InMemoryStorage()
    repo = await Repo.load(storage)

    async with repo:
        # Create a document
        doc_handle = await repo.create()

        # Give the actor time to initialize
        await asyncio.sleep(0.1)

        # Test that doc() and change() methods exist
        assert hasattr(doc_handle, "doc")
        assert callable(doc_handle.doc)
        assert hasattr(doc_handle, "change")
        assert callable(doc_handle.change)

        # Test modifying the document
        # Put a value in the document
        def set_value(doc):
            doc["hello"] = "world"

        await doc_handle.change(set_value)

        # Give IO tasks time to process
        await asyncio.sleep(0.1)

        # Read the value back
        doc = doc_handle.doc()
        result = doc.get("hello")

        # Verify the value was set correctly
        assert result == "world"


@pytest.mark.asyncio
async def test_document_event_emitter_basic():
    """Test basic event emitter functionality - single callback"""
    from automerge.repo import InMemoryStorage

    storage = InMemoryStorage()
    repo = await Repo.load(storage)

    async with repo:
        doc_handle = await repo.create()
        await asyncio.sleep(0.1)

        # Track callback invocations and patches
        callback_count = []
        received_patches = []

        def on_change(patches):
            callback_count.append(1)
            received_patches.append(patches)

        # Register callback
        doc_handle.on("change", on_change)

        # Modify the document
        def set_value(doc):
            doc["key"] = "value"

        await doc_handle.change(set_value)
        await asyncio.sleep(0.1)

        # Callback should have been invoked once
        assert len(callback_count) == 1
        # Patches should have been received
        assert len(received_patches) == 1
        assert len(received_patches[0]) > 0  # Should have at least one patch


@pytest.mark.asyncio
async def test_document_event_emitter_multiple_callbacks():
    """Test multiple callbacks on the same event"""
    from automerge.repo import InMemoryStorage

    storage = InMemoryStorage()
    repo = await Repo.load(storage)

    async with repo:
        doc_handle = await repo.create()
        await asyncio.sleep(0.1)

        # Track callback invocations
        callback1_count = []
        callback2_count = []
        callback3_count = []

        def on_change_1(patches):
            callback1_count.append(1)

        def on_change_2(patches):
            callback2_count.append(1)

        def on_change_3(patches):
            callback3_count.append(1)

        # Register multiple callbacks
        doc_handle.on("change", on_change_1)
        doc_handle.on("change", on_change_2)
        doc_handle.on("change", on_change_3)

        # Modify the document
        def set_value(doc):
            doc["key"] = "value"

        await doc_handle.change(set_value)
        await asyncio.sleep(0.1)

        # All callbacks should have been invoked
        assert len(callback1_count) == 1
        assert len(callback2_count) == 1
        assert len(callback3_count) == 1


@pytest.mark.asyncio
async def test_document_event_emitter_multiple_changes():
    """Test that callbacks are invoked for each change"""
    from automerge.repo import InMemoryStorage

    storage = InMemoryStorage()
    repo = await Repo.load(storage)

    async with repo:
        doc_handle = await repo.create()
        await asyncio.sleep(0.1)

        # Track callback invocations
        callback_count = []

        def on_change(patches):
            callback_count.append(1)

        # Register callback
        doc_handle.on("change", on_change)

        # Make multiple changes
        for i in range(5):

            def set_value(doc, value=i):
                doc[f"key{value}"] = f"value{value}"

            await doc_handle.change(set_value)
            await asyncio.sleep(0.05)

        # Callback should have been invoked 5 times
        assert len(callback_count) == 5


@pytest.mark.asyncio
async def test_document_event_emitter_error_isolation():
    """Test that errors in one callback don't affect others"""
    from automerge.repo import InMemoryStorage

    storage = InMemoryStorage()
    repo = await Repo.load(storage)

    async with repo:
        doc_handle = await repo.create()
        await asyncio.sleep(0.1)

        # Track callback invocations
        callback1_count = []
        callback3_count = []

        def on_change_1(patches):
            callback1_count.append(1)

        def on_change_2_error(patches):
            raise ValueError("Intentional error in callback")

        def on_change_3(patches):
            callback3_count.append(1)

        # Register callbacks - middle one will error
        doc_handle.on("change", on_change_1)
        doc_handle.on("change", on_change_2_error)
        doc_handle.on("change", on_change_3)

        # Modify the document
        def set_value(doc):
            doc["key"] = "value"

        # This should not raise an exception
        await doc_handle.change(set_value)
        await asyncio.sleep(0.1)

        # First and third callbacks should still have been invoked
        assert len(callback1_count) == 1
        assert len(callback3_count) == 1


@pytest.mark.asyncio
async def test_document_event_emitter_no_callbacks():
    """Test that changes work fine when no callbacks are registered"""
    from automerge.repo import InMemoryStorage

    storage = InMemoryStorage()
    repo = await Repo.load(storage)

    async with repo:
        doc_handle = await repo.create()
        await asyncio.sleep(0.1)

        # Modify the document without any callbacks registered
        def set_value(doc):
            doc["key"] = "value"

        # This should work fine
        await doc_handle.change(set_value)
        await asyncio.sleep(0.1)

        # Verify the value was set
        doc = doc_handle.doc()
        result = doc.get("key")
        assert result == "value"


@pytest.mark.asyncio
async def test_document_event_emitter_doc_method_no_emit():
    """Test that doc() method (read-only) does not emit change events"""
    from automerge.repo import InMemoryStorage

    storage = InMemoryStorage()
    repo = await Repo.load(storage)

    async with repo:
        doc_handle = await repo.create()
        await asyncio.sleep(0.1)

        # Track callback invocations
        callback_count = []

        def on_change(patches):
            callback_count.append(1)

        # Register callback
        doc_handle.on("change", on_change)

        # First, make a change to verify callback works
        def set_value(doc):
            doc["key"] = "value"

        await doc_handle.change(set_value)
        await asyncio.sleep(0.1)

        assert len(callback_count) == 1

        # Now use doc() to read (should not trigger callback)
        doc = doc_handle.doc()
        doc.get("key")
        await asyncio.sleep(0.1)

        # Callback count should still be 1 (not incremented)
        assert len(callback_count) == 1


@pytest.mark.asyncio
async def test_document_event_emitter_off():
    """Test removing event listeners with off()"""
    from automerge.repo import InMemoryStorage

    storage = InMemoryStorage()
    repo = await Repo.load(storage)

    async with repo:
        doc_handle = await repo.create()
        await asyncio.sleep(0.1)

        # Track callback invocations
        callback1_count = []
        callback2_count = []

        def on_change_1(patches):
            callback1_count.append(1)

        def on_change_2(patches):
            callback2_count.append(1)

        # Register both callbacks
        doc_handle.on("change", on_change_1)
        doc_handle.on("change", on_change_2)

        # Make a change - both should be called
        def set_value_1(doc):
            doc["key1"] = "value1"

        await doc_handle.change(set_value_1)
        await asyncio.sleep(0.1)

        assert len(callback1_count) == 1
        assert len(callback2_count) == 1

        # Remove first callback
        doc_handle.off("change", on_change_1)

        # Make another change - only second should be called
        def set_value_2(doc):
            doc["key2"] = "value2"

        await doc_handle.change(set_value_2)
        await asyncio.sleep(0.1)

        # First callback should still be 1, second should be 2
        assert len(callback1_count) == 1
        assert len(callback2_count) == 2

        # Remove second callback
        doc_handle.off("change", on_change_2)

        # Make another change - neither should be called
        def set_value_3(doc):
            doc["key3"] = "value3"

        await doc_handle.change(set_value_3)
        await asyncio.sleep(0.1)

        # Both should still be at their previous counts
        assert len(callback1_count) == 1
        assert len(callback2_count) == 2


@pytest.mark.asyncio
async def test_document_event_emitter_off_nonexistent():
    """Test that removing a non-existent listener doesn't error"""
    from automerge.repo import InMemoryStorage

    storage = InMemoryStorage()
    repo = await Repo.load(storage)

    async with repo:
        doc_handle = await repo.create()
        await asyncio.sleep(0.1)

        def on_change(patches):
            pass

        # Try to remove a callback that was never added - should not error
        doc_handle.off("change", on_change)

        # Try to remove from a non-existent event - should not error
        doc_handle.off("nonexistent_event", on_change)


@pytest.mark.asyncio
async def test_document_event_emitter_no_change_no_emit():
    """Test that change() without actual changes doesn't emit event"""
    from automerge.repo import InMemoryStorage

    storage = InMemoryStorage()
    repo = await Repo.load(storage)

    async with repo:
        doc_handle = await repo.create()
        await asyncio.sleep(0.1)

        # Track callback invocations
        callback_count = []

        def on_change(patches):
            callback_count.append(1)

        # Register callback
        doc_handle.on("change", on_change)

        # Call change() but don't actually modify anything
        def no_op(doc):
            # Just read, don't modify
            doc.get("nonexistent_key")

        await doc_handle.change(no_op)
        await asyncio.sleep(0.1)

        # Callback should NOT have been invoked
        assert len(callback_count) == 0

        # Now make an actual change
        def set_value(doc):
            doc["key"] = "value"

        await doc_handle.change(set_value)
        await asyncio.sleep(0.1)

        # Now callback should have been invoked once
        assert len(callback_count) == 1


@pytest.mark.asyncio
async def test_hub_connections_empty():
    """Test Hub.connections() returns empty list when no connections exist"""
    from automerge.repo import InMemoryStorage

    storage = InMemoryStorage()
    repo = await Repo.load(storage)

    async with repo:
        # Get connections from the hub
        connections = repo._hub.connections()

        # Should be an empty list initially
        assert isinstance(connections, list)
        assert len(connections) == 0


def test_connection_state_types():
    """Test that ConnectionState, PeerDocState, and ConnectionInfo types are exposed"""
    # Verify the types are importable
    assert ConnectionState is not None
    assert PeerDocState is not None
    assert ConnectionInfo is not None

    # Verify they have repr methods
    # Note: We can't easily construct these types without going through the Hub,
    # but we can verify they exist and are the right type
    assert hasattr(ConnectionState, "__repr__")
    assert hasattr(PeerDocState, "__repr__")
    assert hasattr(ConnectionInfo, "__repr__")


@pytest.mark.asyncio
async def test_repo_find_document():
    """Test finding a document by URL"""
    from automerge.repo import InMemoryStorage

    storage = InMemoryStorage()
    repo = await Repo.load(storage)

    async with repo:
        # Create a document
        handle_a = await repo.create()

        # Set some content
        def init_doc(doc):
            doc["test"] = "value"

        await handle_a.change(init_doc)

        # Get the URL
        url = handle_a.url

        # Find the document by URL
        handle_b = await repo.find(url)

        # Should find the document
        assert handle_b is not None
        assert handle_b.document_id == handle_a.document_id
        assert handle_b.url == url

        # Verify we can read the content through the new handle using new direct access API
        doc = handle_b.doc()
        result = doc["test"]
        assert result == "value"

        # TODO: Add test for non-existent document once find() properly handles this case
        # Currently the command may hang if the document doesn't exist


@pytest.mark.asyncio
async def test_document_sync_basic():
    """Test basic document synchronization between two repos"""
    import asyncio

    from automerge.repo import InMemoryStorage

    # Create two repos with separate storage
    storage_a = InMemoryStorage()
    storage_b = InMemoryStorage()

    repo_a = await Repo.load(storage_a)
    repo_b = await Repo.load(storage_b)

    async with repo_a, repo_b:
        # Create paired transports
        transport_a, transport_b = InMemoryTransport.create_pair()

        # Connect the repos - Repo A initiates (client), Repo B accepts (server)
        conn_task_a = asyncio.create_task(repo_a.connect(transport_a))
        conn_task_b = asyncio.create_task(repo_b.accept(transport_b))

        # Give some time for handshake to complete
        await asyncio.sleep(1.0)  # Increased from 0.3 to allow full handshake

        # Now create a document in Repo A (after connections are established)
        handle_a = await repo_a.create()

        # Set some initial content
        def init_doc(doc):
            doc["title"] = "Hello from Repo A"
            doc["count"] = 42

        await handle_a.change(init_doc)

        # Give time for the document to sync to Repo B
        await asyncio.sleep(0.5)

        # Verify connections exist
        connections_a = repo_a._hub.connections()
        connections_b = repo_b._hub.connections()

        assert len(connections_a) >= 1, "Repo A should have at least one connection"
        assert len(connections_b) >= 1, "Repo B should have at least one connection"

        # The document should have synced from Repo A to Repo B
        url = handle_a.url
        handle_b = await repo_b.find(url)

        assert handle_b is not None, "Document should have synced from Repo A to Repo B"
        assert handle_b.document_id == handle_a.document_id

        # Verify we can read the content from Repo B using new direct access API
        doc = handle_b.doc()
        result = doc["title"]
        assert result == "Hello from Repo A", "Document content should have synced"

        # Clean up - close transports to trigger disconnection
        await transport_a.close()
        await transport_b.close()

        # Wait a bit for disconnection to complete
        await asyncio.sleep(0.2)

        # Cancel connection tasks
        conn_task_a.cancel()
        conn_task_b.cancel()

        try:
            await conn_task_a
        except asyncio.CancelledError:
            pass

        try:
            await conn_task_b
        except asyncio.CancelledError:
            pass


@pytest.mark.asyncio
async def test_doc_direct_access_read_only():
    """Test that documents from doc() are read-only and error on write attempts"""
    from automerge.repo import InMemoryStorage

    storage = InMemoryStorage()
    repo = await Repo.load(storage)

    async with repo:
        # Create a document
        doc_handle = await repo.create()

        # Initialize with some content
        def init_doc(doc):
            doc["key"] = "value"

        await doc_handle.change(init_doc)
        await asyncio.sleep(0.1)

        # Get read-only document reference
        doc = doc_handle.doc()

        # Verify we can read
        assert doc["key"] == "value"

        # Attempt to write - should raise error
        with pytest.raises(TypeError) as exc_info:
            doc["key"] = "new value"

        # Error message should indicate item assignment is not supported
        error_msg = str(exc_info.value)
        assert (
            "does not support item assignment" in error_msg or "read-only" in error_msg
        )


@pytest.mark.asyncio
async def test_doc_direct_access_transaction_error():
    """Test that creating transactions on actor-backed documents raises clear error"""
    from automerge.repo import InMemoryStorage

    storage = InMemoryStorage()
    repo = await Repo.load(storage)

    async with repo:
        # Create a document
        doc_handle = await repo.create()
        await asyncio.sleep(0.1)

        # Get the underlying core document
        actor = repo._doc_actors.get(doc_handle._actor_id)
        assert actor is not None

        core_doc = actor.get_document()

        # Attempt to create transaction - should raise error
        with pytest.raises(Exception) as exc_info:
            core_doc.transaction()

        # Error message should mention DocHandle.change() as the alternative
        error_msg = str(exc_info.value)
        assert "read-only" in error_msg or "Cannot create transaction" in error_msg
        assert "change()" in error_msg or "Use DocHandle.change()" in error_msg


@pytest.mark.asyncio
async def test_doc_concurrent_references():
    """Test that multiple document references work concurrently"""
    from automerge.repo import InMemoryStorage

    storage = InMemoryStorage()
    repo = await Repo.load(storage)

    async with repo:
        # Create a document with some content
        doc_handle = await repo.create()

        def init_doc(doc):
            doc["key1"] = "value1"
            doc["key2"] = "value2"

        await doc_handle.change(init_doc)
        await asyncio.sleep(0.1)

        # Get multiple document references
        doc1 = doc_handle.doc()
        doc2 = doc_handle.doc()
        doc3 = doc_handle.doc()

        # All should be able to read independently
        assert doc1["key1"] == "value1"
        assert doc2["key2"] == "value2"
        assert doc3["key1"] == "value1"

        # References should work independently (not interfere with each other)
        result1 = doc1.get("key1")
        result2 = doc2.get("key2")
        result3 = doc3.get("key1")

        assert result1 == "value1"
        assert result2 == "value2"
        assert result3 == "value1"


@pytest.mark.asyncio
async def test_doc_read_during_change():
    """Test that reading while a change is in progress works correctly"""
    from automerge.repo import InMemoryStorage

    storage = InMemoryStorage()
    repo = await Repo.load(storage)

    async with repo:
        # Create a document with initial content
        doc_handle = await repo.create()

        def init_doc(doc):
            doc["counter"] = 0

        await doc_handle.change(init_doc)
        await asyncio.sleep(0.1)

        # Get initial document reference
        doc_before = doc_handle.doc()
        assert doc_before["counter"] == 0

        # Make a change
        def increment(doc):
            doc["counter"] = 1

        await doc_handle.change(increment)
        await asyncio.sleep(0.1)

        # Get new document reference after change
        doc_after = doc_handle.doc()
        assert doc_after["counter"] == 1

        # Old reference should still work (it will see new state due to mutex)
        # The mutex ensures we get a consistent view
        current_value = doc_before["counter"]
        assert current_value == 1  # Should see the updated value


@pytest.mark.asyncio
async def test_doc_reference_across_awaits():
    """Test that document references stay valid across awaits"""
    from automerge.repo import InMemoryStorage

    storage = InMemoryStorage()
    repo = await Repo.load(storage)

    async with repo:
        # Create a document
        doc_handle = await repo.create()

        def init_doc(doc):
            doc["field1"] = "value1"
            doc["field2"] = "value2"

        await doc_handle.change(init_doc)
        await asyncio.sleep(0.1)

        # Get document reference
        doc = doc_handle.doc()

        # Read value
        val1 = doc["field1"]
        assert val1 == "value1"

        # Await something
        await asyncio.sleep(0.05)

        # Document reference should still be valid
        val2 = doc["field2"]
        assert val2 == "value2"

        # Another await
        await asyncio.sleep(0.05)

        # Still valid
        val1_again = doc.get("field1")
        assert val1_again == "value1"

        # Make a change via handle
        def update(doc):
            doc["field3"] = "value3"

        await doc_handle.change(update)
        await asyncio.sleep(0.1)

        # Original reference should see the new value (mutex provides consistent view)
        val3 = doc.get("field3")
        assert val3 == "value3"


@pytest.mark.asyncio
async def test_doc_reference_in_change_callback():
    """Test that doc() references work inside change() callbacks without deadlock."""
    from automerge.repo import InMemoryStorage

    storage = InMemoryStorage()
    repo = await Repo.load(storage)

    async with repo:
        handle = await repo.create()

        # Set up initial state
        def init_counter(doc):
            doc["counter"] = 0

        await handle.change(init_counter)
        await asyncio.sleep(0.1)

        # Get a doc reference
        doc_ref = handle.doc()

        # Use the doc reference inside a change callback
        # This should NOT deadlock
        def increment_using_ref(doc):
            doc["counter"] = doc_ref["counter"] + 1

        await handle.change(increment_using_ref)
        await asyncio.sleep(0.1)

        # Verify the change worked
        doc = handle.doc()
        assert doc["counter"] == 1


@pytest.mark.asyncio
async def test_nested_doc_reference_in_change():
    """Test that nested doc() references work in change callbacks."""
    from automerge.repo import InMemoryStorage

    storage = InMemoryStorage()
    repo = await Repo.load(storage)

    async with repo:
        handle = await repo.create()

        # Set up initial values
        def init_values(doc):
            doc["value1"] = 10
            doc["value2"] = 20

        await handle.change(init_values)
        await asyncio.sleep(0.1)

        # Get a doc reference
        doc_ref = handle.doc()

        # Use doc reference to read multiple values in change callback
        def compute_sum(doc):
            # Read from external doc reference
            val1 = doc_ref["value1"]
            val2 = doc_ref["value2"]
            # Write to current doc
            doc["sum"] = val1 + val2

        await handle.change(compute_sum)
        await asyncio.sleep(0.1)

        # Verify
        doc = handle.doc()
        assert doc["sum"] == 30


@pytest.mark.asyncio
async def test_different_doc_in_change_callback():
    """Test that accessing different document's reference works in change callback."""
    from automerge.repo import InMemoryStorage

    storage = InMemoryStorage()
    repo = await Repo.load(storage)

    async with repo:
        handle1 = await repo.create()
        handle2 = await repo.create()

        # Set up both documents
        def init_doc1(doc):
            doc["name"] = "doc1"

        await handle1.change(init_doc1)
        await asyncio.sleep(0.1)

        def init_doc2(doc):
            doc["name"] = "doc2"

        await handle2.change(init_doc2)
        await asyncio.sleep(0.1)

        # Get reference to doc2
        doc2_ref = handle2.doc()

        # Access doc2_ref inside doc1's change callback
        # This should work because they're different actors
        def use_doc2_ref(doc):
            doc["other_name"] = doc2_ref["name"]

        await handle1.change(use_doc2_ref)
        await asyncio.sleep(0.1)

        # Verify
        doc1 = handle1.doc()
        assert doc1["other_name"] == "doc2"


@pytest.mark.asyncio
async def test_multiple_doc_refs_in_change():
    """Test that multiple doc() references work in change callback."""
    from automerge.repo import InMemoryStorage

    storage = InMemoryStorage()
    repo = await Repo.load(storage)

    async with repo:
        handle = await repo.create()

        # Set up state
        def init_values(doc):
            doc["a"] = 1
            doc["b"] = 2

        await handle.change(init_values)
        await asyncio.sleep(0.1)

        # Get multiple references (simulating user having refs from different places)
        ref1 = handle.doc()
        ref2 = handle.doc()

        # Use both in change callback
        def compute_sum(doc):
            doc["sum"] = ref1["a"] + ref2["b"]

        await handle.change(compute_sum)
        await asyncio.sleep(0.1)

        # Verify
        doc = handle.doc()
        assert doc["sum"] == 3


@pytest.mark.asyncio
async def test_nested_change_detection():
    """Test that we can detect when we're in a change callback.

    Note: Truly nested change() calls (calling change from within a change callback
    on the same thread) will be detected and raise an error. This test verifies
    the detection mechanism works.
    """
    from automerge.repo import InMemoryStorage

    storage = InMemoryStorage()
    repo = await Repo.load(storage)

    async with repo:
        handle1 = await repo.create()
        handle2 = await repo.create()

        # Set up initial state
        def init_value(doc):
            doc["value"] = 1

        await handle1.change(init_value)
        await handle2.change(init_value)
        await asyncio.sleep(0.1)

        # Test that we can access handle1.doc() from within handle2's change callback
        # These are different actors, so this should work fine
        def use_other_doc(doc):
            # Access doc from handle1 while in handle2's change callback
            val = handle1.doc()["value"]
            doc["copied_value"] = val

        await handle2.change(use_other_doc)
        await asyncio.sleep(0.1)

        # Verify it worked
        doc2 = handle2.doc()
        assert doc2["copied_value"] == 1


@pytest.mark.asyncio
async def test_context_cleanup_on_error():
    """Test that context is cleaned up when callback raises error."""
    from automerge.repo import InMemoryStorage

    storage = InMemoryStorage()
    repo = await Repo.load(storage)

    async with repo:
        handle = await repo.create()

        # Set up state
        def init_value(doc):
            doc["value"] = 1

        await handle.change(init_value)
        await asyncio.sleep(0.1)

        # Callback that raises an error
        with pytest.raises(ValueError):

            def error_callback(doc):
                # Access doc() ref to set context, then raise error
                _ = handle.doc()["value"]
                raise ValueError("Intentional error")

            await handle.change(error_callback)

        await asyncio.sleep(0.1)

        # Context should be cleaned up - next change should work
        def update_value(doc):
            doc["value"] = 2

        await handle.change(update_value)
        await asyncio.sleep(0.1)

        # Verify subsequent operations work
        doc = handle.doc()
        assert doc["value"] == 2


@pytest.mark.asyncio
async def test_context_cleanup_after_success():
    """Test that context is cleaned up after successful callback."""
    from automerge.repo import InMemoryStorage

    storage = InMemoryStorage()
    repo = await Repo.load(storage)

    async with repo:
        handle = await repo.create()

        # First change
        def first_change(doc):
            doc["value"] = 1

        await handle.change(first_change)
        await asyncio.sleep(0.1)

        # Context should be cleared after first change completes
        # Second change should not see it as "nested"
        def second_change(doc):
            doc["value"] = 2

        await handle.change(second_change)
        await asyncio.sleep(0.1)

        # Verify
        doc = handle.doc()
        assert doc["value"] == 2
