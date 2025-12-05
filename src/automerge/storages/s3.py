"""
S3 storage implementation for automerge-py.

This module provides an S3-backed storage adapter that implements the Storage
protocol required by automerge-py's Repo.load(). It uses aiobotocore for async
S3 operations.

Uses per-request client creation for automatic resource cleanup - no explicit
close() call needed. Each operation manages its own S3 client connection.

Requirements:
    pip install automerge[s3]

Example:
    >>> from automerge.storages.s3 import S3Storage
    >>> from automerge.repo import Repo
    >>>
    >>> storage = S3Storage(
    ...     bucket="my-bucket",
    ...     region="us-east-1",
    ...     prefix="users/user-123"
    ... )
    >>> repo = await Repo.load(storage)
    >>> async with repo:
    ...     handle = await repo.create()
    ...     await handle.change(lambda doc: doc.__setitem__("key", "value"))
"""

import asyncio
from typing import List, Optional, Tuple

from aiobotocore.session import AioSession
from botocore.exceptions import ClientError

from automerge._automerge import StorageKey


class S3Storage:
    """
    S3 storage implementation for automerge-py.

    Implements the Storage protocol required by Repo.load() to persist
    automerge documents in Amazon S3.

    Uses per-request client creation for automatic resource cleanup.
    No explicit close() call is needed - each operation manages its own
    S3 client connection, similar to how the AWS SDK v3 for JavaScript works.

    Attributes:
        bucket: The S3 bucket name
        region: The AWS region
        prefix: Key prefix for all stored objects
        credentials: Optional AWS credentials dict
        max_concurrent_requests: Concurrency limit for batch operations

    Example:
        >>> storage = S3Storage(
        ...     bucket="my-bucket",
        ...     region="us-east-1",
        ...     prefix="users/user-123",
        ... )
        >>> repo = await Repo.load(storage)
    """

    def __init__(
        self,
        bucket: str,
        region: str,
        prefix: str = "",
        credentials: Optional[dict] = None,
        max_concurrent_requests: int = 50,
    ):
        """
        Initialize S3 storage.

        Args:
            bucket: S3 bucket name
            region: AWS region (e.g., "us-east-1")
            prefix: Key prefix for all stored objects (e.g., "users/user-123").
                    This allows multiple users/repos to share a bucket.
            credentials: Optional dict with AWS credentials:
                - aws_access_key_id: AWS access key ID
                - aws_secret_access_key: AWS secret access key
                - aws_session_token: Optional session token for temporary credentials

                If not provided, uses the default AWS credential chain
                (environment variables, ~/.aws/credentials, IAM role, etc.)
            max_concurrent_requests: Maximum number of concurrent S3 requests
                for batch operations like load_range(). Default: 50.
        """
        self.bucket = bucket
        self.region = region
        self.prefix = prefix.rstrip("/") if prefix else ""
        self.credentials = credentials
        self.max_concurrent_requests = max_concurrent_requests
        self._session = AioSession()

    def _create_client(self):
        """
        Create a new S3 client context manager.

        Each call returns a fresh client context that should be used with
        `async with`. The client is automatically cleaned up when the
        context exits.

        Returns:
            An async context manager that yields an S3 client

        Example:
            async with self._create_client() as s3:
                await s3.get_object(Bucket=self.bucket, Key=key)
        """
        if self.credentials:
            return self._session.create_client(
                "s3",
                region_name=self.region,
                aws_access_key_id=self.credentials.get("aws_access_key_id"),
                aws_secret_access_key=self.credentials.get("aws_secret_access_key"),
                aws_session_token=self.credentials.get("aws_session_token"),
            )
        else:
            # Use default credential chain (env vars, ~/.aws/credentials, etc.)
            return self._session.create_client(
                "s3",
                region_name=self.region,
            )

    def _storage_key_to_s3_key(self, key: StorageKey) -> str:
        """
        Convert a StorageKey to an S3 object key.

        The StorageKey parts are joined with '/' to form a hierarchical path.
        The configured prefix is prepended if set.

        Args:
            key: The StorageKey to convert

        Returns:
            S3 object key string
        """
        parts = key.to_parts()
        path = "/".join(parts)
        if self.prefix:
            return f"{self.prefix}/{path}"
        return path

    def _s3_key_to_storage_key(self, s3_key: str) -> StorageKey:
        """
        Convert an S3 object key back to a StorageKey.

        Strips the configured prefix if present and splits on '/'.

        Args:
            s3_key: S3 object key string

        Returns:
            StorageKey instance
        """
        # Strip prefix if present
        if self.prefix and s3_key.startswith(self.prefix + "/"):
            s3_key = s3_key[len(self.prefix) + 1 :]
        parts = s3_key.split("/")
        return StorageKey.from_parts(parts)

    async def load(self, key: StorageKey) -> Optional[bytes]:
        """
        Load a value from S3.

        Args:
            key: The storage key to load

        Returns:
            The stored bytes if the key exists, None otherwise
        """
        s3_key = self._storage_key_to_s3_key(key)

        async with self._create_client() as s3:
            try:
                response = await s3.get_object(Bucket=self.bucket, Key=s3_key)
                return await response["Body"].read()
            except ClientError as e:
                if e.response["Error"]["Code"] == "NoSuchKey":
                    return None
                raise

    async def load_range(self, prefix: StorageKey) -> List[Tuple[StorageKey, bytes]]:
        """
        Load all keys with the given prefix from S3.

        This lists all objects under the prefix path and loads each one
        in parallel for better performance. Concurrency is limited by
        max_concurrent_requests. Used by automerge-py to load all chunks
        for a document.

        Args:
            prefix: The key prefix to match

        Returns:
            List of (key, value) tuples for all matching keys
        """
        s3_prefix = self._storage_key_to_s3_key(prefix) + "/"
        sem = asyncio.Semaphore(self.max_concurrent_requests)

        async with self._create_client() as s3:
            # Collect all object keys first
            object_keys: List[str] = []
            paginator = s3.get_paginator("list_objects_v2")
            async for page in paginator.paginate(Bucket=self.bucket, Prefix=s3_prefix):
                for obj in page.get("Contents", []):
                    object_keys.append(obj["Key"])

            # Fetch all objects in parallel with limited concurrency
            async def fetch_object(s3_key: str) -> Optional[Tuple[StorageKey, bytes]]:
                async with sem:
                    try:
                        response = await s3.get_object(Bucket=self.bucket, Key=s3_key)
                        data = await response["Body"].read()
                        storage_key = self._s3_key_to_storage_key(s3_key)
                        return (storage_key, data)
                    except ClientError as e:
                        # Skip objects that were deleted between list and get
                        if e.response["Error"]["Code"] == "NoSuchKey":
                            return None
                        raise

            fetched = await asyncio.gather(*[fetch_object(key) for key in object_keys])
            return [item for item in fetched if item is not None]

    async def put(self, key: StorageKey, value: bytes) -> None:
        """
        Store a value in S3.

        Args:
            key: The storage key
            value: The bytes to store
        """
        s3_key = self._storage_key_to_s3_key(key)

        async with self._create_client() as s3:
            await s3.put_object(Bucket=self.bucket, Key=s3_key, Body=value)

    async def delete(self, key: StorageKey) -> None:
        """
        Delete a value from S3.

        Args:
            key: The storage key to delete
        """
        s3_key = self._storage_key_to_s3_key(key)

        async with self._create_client() as s3:
            await s3.delete_object(Bucket=self.bucket, Key=s3_key)
