import pytest

from automerge._automerge import StorageKey
from automerge.repo import FileSystemStorage, Repo


@pytest.mark.asyncio
async def test_filesystem_storage_put_and_load(tmp_path):
    """Test basic put and load operations with FileSystemStorage"""
    storage = FileSystemStorage(tmp_path / "storage")
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
async def test_filesystem_storage_delete(tmp_path):
    """Test delete operation with FileSystemStorage"""
    storage = FileSystemStorage(tmp_path / "storage")
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
async def test_filesystem_storage_load_range(tmp_path):
    """Test load_range operation with FileSystemStorage"""
    storage = FileSystemStorage(tmp_path / "storage")

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
async def test_filesystem_storage_load_range_empty(tmp_path):
    """Test load_range with no matching keys for FileSystemStorage"""
    storage = FileSystemStorage(tmp_path / "storage")

    # Load range with non-existent prefix
    results = await storage.load_range(StorageKey.from_parts(["nonexistent"]))

    # Should get empty list
    assert len(results) == 0


@pytest.mark.asyncio
async def test_filesystem_storage_overwrite(tmp_path):
    """Test overwriting an existing key with FileSystemStorage"""
    storage = FileSystemStorage(tmp_path / "storage")
    key = StorageKey.from_parts(["test", "overwrite"])

    # Put initial value
    await storage.put(key, b"initial")
    assert await storage.load(key) == b"initial"

    # Overwrite with new value
    await storage.put(key, b"updated")
    assert await storage.load(key) == b"updated"


@pytest.mark.asyncio
async def test_filesystem_storage_splay(tmp_path):
    """Test that FileSystemStorage correctly splays keys like git"""
    storage = FileSystemStorage(tmp_path / "storage")

    # Key with long first component - should be splayed
    key = StorageKey.from_parts(["abc123def", "data"])
    await storage.put(key, b"test")

    # Check the file structure: should be base/ab/c123def/data
    expected_path = tmp_path / "storage" / "ab" / "c123def" / "data"
    assert expected_path.exists()
    assert expected_path.read_bytes() == b"test"

    # Verify load works with splayed path
    result = await storage.load(key)
    assert result == b"test"


@pytest.mark.asyncio
async def test_filesystem_storage_splay_short_key_error(tmp_path):
    """Test FileSystemStorage raises error for short first component"""
    storage = FileSystemStorage(tmp_path / "storage")

    # Key with very short first component should raise
    key = StorageKey.from_parts(["a", "data"])
    with pytest.raises(ValueError, match="at least 3 characters"):
        await storage.put(key, b"short")


@pytest.mark.asyncio
async def test_filesystem_storage_splay_two_char_key_error(tmp_path):
    """Test FileSystemStorage raises error for two-char first component"""
    storage = FileSystemStorage(tmp_path / "storage")

    # Key with exactly 2 char first component should raise
    key = StorageKey.from_parts(["ab", "data"])
    with pytest.raises(ValueError, match="at least 3 characters"):
        await storage.put(key, b"two-char")


@pytest.mark.asyncio
async def test_filesystem_storage_delete_leaves_dirs(tmp_path):
    """Test that delete removes file but leaves directories (for concurrency safety)"""
    storage = FileSystemStorage(tmp_path / "storage")
    key = StorageKey.from_parts(["abc123", "nested", "deep", "file"])

    await storage.put(key, b"deep data")

    # Verify the nested structure exists
    nested_dir = tmp_path / "storage" / "ab" / "c123" / "nested" / "deep"
    assert nested_dir.exists()

    # Delete the file
    await storage.delete(key)

    # File should be gone
    assert await storage.load(key) is None

    # But directories remain (for concurrency safety)
    assert nested_dir.exists()
    assert (tmp_path / "storage").exists()


@pytest.mark.asyncio
async def test_filesystem_storage_persistence(tmp_path):
    """Test that FileSystemStorage persists data across instances"""
    storage_path = tmp_path / "storage"
    key = StorageKey.from_parts(["persist", "test"])
    value = b"persistent data"

    # Create first instance and store data
    storage1 = FileSystemStorage(storage_path)
    await storage1.put(key, value)

    # Create second instance pointing to same path
    storage2 = FileSystemStorage(storage_path)

    # Data should be accessible from second instance
    result = await storage2.load(key)
    assert result == value


@pytest.mark.asyncio
async def test_filesystem_storage_multiple_operations(tmp_path):
    """Test multiple storage operations in sequence with FileSystemStorage"""
    storage = FileSystemStorage(tmp_path / "storage")

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


@pytest.mark.asyncio
async def test_filesystem_storage_with_repo(tmp_path):
    """Test FileSystemStorage works with Repo"""
    storage = FileSystemStorage(tmp_path / "repo_storage")
    repo = await Repo.load(storage)

    async with repo:
        # Create a document
        handle = await repo.create()
        url = handle.url

        # Make a change
        await handle.change(lambda doc: doc.__setitem__("key", "value"))

        # Verify the change
        assert handle.doc()["key"] == "value"

    # Data should be persisted - create new repo with same storage
    storage2 = FileSystemStorage(tmp_path / "repo_storage")
    repo2 = await Repo.load(storage2)

    async with repo2:
        # Find the document by URL
        handle2 = await repo2.find(url)
        assert handle2 is not None
        assert handle2.doc()["key"] == "value"
