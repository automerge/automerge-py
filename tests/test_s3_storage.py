"""
Tests for S3Storage implementation.

These tests require:
    - AWS credentials configured (env vars or ~/.aws/credentials)
    - S3_BUCKET_NAME environment variable set
    - AWS_REGION environment variable set
    - S3_TEST_PREFIX environment variable set (base prefix for test data)
    - aiobotocore installed

Tests are skipped if required environment variables are not set.

Usage:
    export S3_BUCKET_NAME=my-test-bucket
    export AWS_REGION=us-east-1
    export S3_TEST_PREFIX=my-test-folder
    pytest tests/test_s3_storage.py -v
"""

import asyncio
import os
import uuid

import pytest

# Skip all tests in this module if required env vars are not set
pytestmark = pytest.mark.skipif(
    not os.environ.get("S3_BUCKET_NAME")
    or not os.environ.get("AWS_REGION")
    or not os.environ.get("S3_TEST_PREFIX"),
    reason="S3_BUCKET_NAME, AWS_REGION, and S3_TEST_PREFIX environment variables required",
)


def get_test_prefix(test_name: str = "pytest") -> str:
    """Get a unique S3 prefix for test isolation."""
    base_prefix = os.environ["S3_TEST_PREFIX"].rstrip("/")
    test_id = str(uuid.uuid4())[:8]
    return f"{base_prefix}/{test_name}/{test_id}"


@pytest.fixture
def s3_storage():
    """Create an S3Storage instance with a unique prefix for test isolation."""
    from automerge.storages.s3 import S3Storage

    bucket = os.environ["S3_BUCKET_NAME"]
    region = os.environ["AWS_REGION"]
    prefix = get_test_prefix()

    return S3Storage(bucket=bucket, region=region, prefix=prefix)


@pytest.fixture
def storage_key():
    """Import StorageKey for use in tests."""
    from automerge._automerge import StorageKey

    return StorageKey


@pytest.mark.asyncio
async def test_s3_storage_put_and_load(s3_storage, storage_key):
    """Test basic put and load operations with S3Storage."""
    key = storage_key.from_parts(["test", "doc1", "data"])
    value = b"hello world from automerge-py S3 test"

    # Initially should be None
    result = await s3_storage.load(key)
    assert result is None

    # Put value
    await s3_storage.put(key, value)

    # Should now load the value
    result = await s3_storage.load(key)
    assert result == value

    # Cleanup
    await s3_storage.delete(key)


@pytest.mark.asyncio
async def test_s3_storage_load_missing_key(s3_storage, storage_key):
    """Test that load returns None for missing keys."""
    key = storage_key.from_parts(["nonexistent", "key", "path"])
    result = await s3_storage.load(key)
    assert result is None


@pytest.mark.asyncio
async def test_s3_storage_delete(s3_storage, storage_key):
    """Test delete operation with S3Storage."""
    key = storage_key.from_parts(["test", "delete", "me"])
    value = b"to be deleted"

    # Put and verify
    await s3_storage.put(key, value)
    assert await s3_storage.load(key) == value

    # Delete
    await s3_storage.delete(key)

    # Should be None after delete
    assert await s3_storage.load(key) is None


@pytest.mark.asyncio
async def test_s3_storage_load_range(s3_storage, storage_key):
    """Test load_range operation with S3Storage."""
    # Put multiple values with same prefix
    await s3_storage.put(storage_key.from_parts(["docs", "doc1", "data"]), b"data1")
    await s3_storage.put(storage_key.from_parts(["docs", "doc2", "data"]), b"data2")
    await s3_storage.put(storage_key.from_parts(["docs", "doc3", "data"]), b"data3")
    await s3_storage.put(storage_key.from_parts(["other", "key"]), b"other")

    # Load range with "docs" prefix
    results = await s3_storage.load_range(storage_key.from_parts(["docs"]))

    # Should get 3 results, not the "other" key
    assert len(results) == 3

    # Check that all results have correct prefix
    for key, value in results:
        parts = key.to_parts()
        assert parts[0] == "docs"
        assert value.startswith(b"data")

    # Cleanup
    await s3_storage.delete(storage_key.from_parts(["docs", "doc1", "data"]))
    await s3_storage.delete(storage_key.from_parts(["docs", "doc2", "data"]))
    await s3_storage.delete(storage_key.from_parts(["docs", "doc3", "data"]))
    await s3_storage.delete(storage_key.from_parts(["other", "key"]))


@pytest.mark.asyncio
async def test_s3_storage_load_range_empty(s3_storage, storage_key):
    """Test load_range with no matching keys."""
    results = await s3_storage.load_range(storage_key.from_parts(["nonexistent"]))
    assert len(results) == 0


@pytest.mark.asyncio
async def test_s3_storage_overwrite(s3_storage, storage_key):
    """Test overwriting an existing key."""
    key = storage_key.from_parts(["test", "overwrite", "key"])

    # Put initial value
    await s3_storage.put(key, b"initial")
    assert await s3_storage.load(key) == b"initial"

    # Overwrite with new value
    await s3_storage.put(key, b"updated")
    assert await s3_storage.load(key) == b"updated"

    # Cleanup
    await s3_storage.delete(key)


@pytest.mark.asyncio
async def test_s3_storage_binary_data(s3_storage, storage_key):
    """Test that S3Storage correctly handles binary data."""
    key = storage_key.from_parts(["test", "binary", "doc"])
    # All possible byte values
    binary_data = bytes(range(256))

    await s3_storage.put(key, binary_data)
    result = await s3_storage.load(key)

    assert result == binary_data

    # Cleanup
    await s3_storage.delete(key)


@pytest.mark.asyncio
async def test_s3_storage_with_repo(s3_storage):
    """Test S3Storage works with Repo."""
    from automerge.repo import Repo

    repo = await Repo.load(s3_storage)

    async with repo:
        # Create a document
        handle = await repo.create()
        await asyncio.sleep(0.1)  # Allow time for storage operations

        # Make a change
        await handle.change(lambda doc: doc.__setitem__("key", "value"))
        await asyncio.sleep(0.1)

        # Verify the change
        assert handle.doc()["key"] == "value"


@pytest.mark.asyncio
async def test_s3_storage_with_credentials():
    """Test S3Storage with explicit credentials."""
    from automerge.storages.s3 import S3Storage

    bucket = os.environ["S3_BUCKET_NAME"]
    region = os.environ["AWS_REGION"]

    # Get credentials from environment (simulating what the app would pass)
    credentials = {
        "aws_access_key_id": os.environ.get("AWS_ACCESS_KEY_ID"),
        "aws_secret_access_key": os.environ.get("AWS_SECRET_ACCESS_KEY"),
        "aws_session_token": os.environ.get("AWS_SESSION_TOKEN"),
    }

    # Skip if explicit credentials not available
    if not credentials["aws_access_key_id"] or not credentials["aws_secret_access_key"]:
        pytest.skip("AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY not set")

    storage = S3Storage(
        bucket=bucket,
        region=region,
        prefix=get_test_prefix("credentials"),
        credentials=credentials,
    )

    from automerge._automerge import StorageKey

    key = StorageKey.from_parts(["cred", "test"])
    await storage.put(key, b"credentials work")
    result = await storage.load(key)
    assert result == b"credentials work"

    # Cleanup
    await storage.delete(key)


# Allow running as standalone script for manual testing
if __name__ == "__main__":
    import sys

    async def main():
        """Run tests manually."""
        bucket = os.environ.get("S3_BUCKET_NAME")
        region = os.environ.get("AWS_REGION")
        base_prefix = os.environ.get("S3_TEST_PREFIX")

        if not bucket or not region or not base_prefix:
            print("❌ Error: S3_BUCKET_NAME, AWS_REGION, and S3_TEST_PREFIX environment variables required")
            return 1

        print("=" * 60)
        print("S3Storage Manual Test Suite")
        print("=" * 60)

        from automerge._automerge import StorageKey
        from automerge.storages.s3 import S3Storage

        test_id = str(uuid.uuid4())[:8]
        prefix = f"{base_prefix.rstrip('/')}/manual/{test_id}"

        print(f"\nBucket: {bucket}")
        print(f"Region: {region}")
        print(f"Prefix: {prefix}")
        print("-" * 60)

        storage = S3Storage(bucket=bucket, region=region, prefix=prefix)

        try:
            # Test 1: put and load
            print("\n1. Testing put() and load()...")
            key = StorageKey.from_parts(["test", "doc1", "data"])
            await storage.put(key, b"test data")
            result = await storage.load(key)
            assert result == b"test data"
            print("   ✅ Passed")
            await storage.delete(key)

            # Test 2: load_range
            print("\n2. Testing load_range()...")
            await storage.put(StorageKey.from_parts(["range", "a"]), b"a")
            await storage.put(StorageKey.from_parts(["range", "b"]), b"b")
            results = await storage.load_range(StorageKey.from_parts(["range"]))
            assert len(results) == 2
            print("   ✅ Passed")
            await storage.delete(StorageKey.from_parts(["range", "a"]))
            await storage.delete(StorageKey.from_parts(["range", "b"]))

            # Test 3: Repo integration
            print("\n3. Testing Repo integration...")
            from automerge.repo import Repo

            repo = await Repo.load(storage)
            async with repo:
                handle = await repo.create()
                await asyncio.sleep(0.1)
                await handle.change(lambda doc: doc.__setitem__("test", 123))
                await asyncio.sleep(0.1)
                assert handle.doc()["test"] == 123
            print("   ✅ Passed")

            print("\n" + "=" * 60)
            print("🎉 All tests passed!")
            print("=" * 60)
            return 0

        except Exception as e:
            print(f"\n❌ Test failed: {e}")
            import traceback

            traceback.print_exc()
            return 1

    sys.exit(asyncio.run(main()))

