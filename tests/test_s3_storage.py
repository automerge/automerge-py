"""
Unit tests for S3Storage using moto server mode.

Uses moto's ThreadedMotoServer to run a local S3-compatible endpoint,
which works correctly with aiobotocore's async HTTP client.

Skipped automatically when the s3 extra (aiobotocore) or moto is not installed.
"""

import asyncio
import uuid

import pytest

pytest.importorskip("aiobotocore", reason="automerge[s3] extra not installed")
moto = pytest.importorskip("moto", reason="moto[server] not installed")

import pytest_asyncio
from moto.server import ThreadedMotoServer

from automerge._automerge import StorageKey


@pytest.fixture(scope="session")
def moto_server():
    """Start moto server in a background thread for the entire test session."""
    # Use port 0 to let the OS pick an available port
    server = ThreadedMotoServer(ip_address="127.0.0.1", port=0)
    server.start()
    # Get the actual port that was assigned
    host, port = server.get_host_and_port()
    yield f"http://{host}:{port}"
    server.stop()


@pytest.fixture
def endpoint_url(moto_server):
    """Return the moto server endpoint URL."""
    return moto_server


@pytest.fixture
def bucket_name():
    """Return a unique bucket name for test isolation."""
    return f"test-bucket-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def region():
    return "us-east-1"


@pytest_asyncio.fixture
async def s3_storage(endpoint_url, bucket_name, region):
    """Create an S3Storage instance connected to the moto server."""
    from automerge.storages.s3 import S3Storage

    # Create the bucket first using aiobotocore
    from aiobotocore.session import AioSession

    session = AioSession()
    async with session.create_client(
        "s3",
        region_name=region,
        endpoint_url=endpoint_url,
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
    ) as client:
        await client.create_bucket(Bucket=bucket_name)

    # Return storage instance pointing to moto server
    return S3Storage(
        bucket=bucket_name,
        region=region,
        endpoint_url=endpoint_url,
        credentials={
            "aws_access_key_id": "testing",
            "aws_secret_access_key": "testing",
        },
    )

@pytest.mark.asyncio
async def test_put_and_load(s3_storage):
    """Test basic put and load operations."""
    key = StorageKey.from_parts(["doc1", "data"])
    value = b"hello world"

    # Initially should be None
    assert await s3_storage.load(key) is None

    # Put and verify
    await s3_storage.put(key, value)
    assert await s3_storage.load(key) == value


@pytest.mark.asyncio
async def test_load_missing_key(s3_storage):
    """Test that load returns None for missing keys."""
    key = StorageKey.from_parts(["nonexistent", "key"])
    assert await s3_storage.load(key) is None


@pytest.mark.asyncio
async def test_delete(s3_storage):
    """Test delete operation."""
    key = StorageKey.from_parts(["test", "delete"])
    value = b"to be deleted"

    await s3_storage.put(key, value)
    assert await s3_storage.load(key) == value

    await s3_storage.delete(key)
    assert await s3_storage.load(key) is None


@pytest.mark.asyncio
async def test_load_range(s3_storage):
    """Test load_range operation."""
    # Put multiple values with same prefix
    await s3_storage.put(StorageKey.from_parts(["docs", "doc1", "data"]), b"data1")
    await s3_storage.put(StorageKey.from_parts(["docs", "doc2", "data"]), b"data2")
    await s3_storage.put(StorageKey.from_parts(["docs", "doc3", "data"]), b"data3")
    await s3_storage.put(StorageKey.from_parts(["other", "key"]), b"other")

    # Load range with "docs" prefix
    results = await s3_storage.load_range(StorageKey.from_parts(["docs"]))

    assert len(results) == 3
    for key, value in results:
        parts = key.to_parts()
        assert parts[0] == "docs"
        assert value.startswith(b"data")


@pytest.mark.asyncio
async def test_load_range_empty(s3_storage):
    """Test load_range with no matching keys."""
    results = await s3_storage.load_range(StorageKey.from_parts(["nonexistent"]))
    assert len(results) == 0


@pytest.mark.asyncio
async def test_overwrite(s3_storage):
    """Test overwriting an existing key."""
    key = StorageKey.from_parts(["test", "overwrite"])

    await s3_storage.put(key, b"initial")
    assert await s3_storage.load(key) == b"initial"

    await s3_storage.put(key, b"updated")
    assert await s3_storage.load(key) == b"updated"


@pytest.mark.asyncio
async def test_binary_data(s3_storage):
    """Test that S3Storage correctly handles binary data."""
    key = StorageKey.from_parts(["test", "binary"])
    binary_data = bytes(range(256))

    await s3_storage.put(key, binary_data)
    assert await s3_storage.load(key) == binary_data


@pytest.mark.asyncio
async def test_prefix_isolation(endpoint_url, region):
    """Test that prefix isolation works correctly."""
    from automerge.storages.s3 import S3Storage

    # Create a unique bucket for this test
    from aiobotocore.session import AioSession

    test_bucket = f"prefix-test-{uuid.uuid4().hex[:8]}"
    session = AioSession()
    async with session.create_client(
        "s3",
        region_name=region,
        endpoint_url=endpoint_url,
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
    ) as client:
        await client.create_bucket(Bucket=test_bucket)

    credentials = {
        "aws_access_key_id": "testing",
        "aws_secret_access_key": "testing",
    }

    storage_a = S3Storage(
        bucket=test_bucket,
        region=region,
        prefix="user-a",
        endpoint_url=endpoint_url,
        credentials=credentials,
    )
    storage_b = S3Storage(
        bucket=test_bucket,
        region=region,
        prefix="user-b",
        endpoint_url=endpoint_url,
        credentials=credentials,
    )

    key = StorageKey.from_parts(["doc", "data"])

    await storage_a.put(key, b"value-a")
    await storage_b.put(key, b"value-b")

    assert await storage_a.load(key) == b"value-a"
    assert await storage_b.load(key) == b"value-b"


@pytest.mark.asyncio
async def test_with_repo(s3_storage):
    """Test S3Storage works with Repo."""
    from automerge.repo import Repo

    repo = await Repo.load(s3_storage)

    async with repo:
        handle = await repo.create()
        await asyncio.sleep(0.05)

        with handle.change() as doc:
            doc["key"] = "value"
        await asyncio.sleep(0.05)

        assert handle.doc()["key"] == "value"
