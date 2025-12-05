"""
Storage adapters for automerge-py.

This module provides storage implementations for persisting Automerge
repository data. The core in-memory and filesystem storages are in repo.py,
while cloud-specific storages are in this submodule.

Available storages:
    - S3Storage: Amazon S3 storage (requires aiobotocore)

Example:
    >>> from automerge.storages.s3 import S3Storage
    >>> from automerge.repo import Repo
    >>>
    >>> storage = S3Storage(bucket="my-bucket", prefix="my-app")
    >>> repo = await Repo.load(storage)
"""

from .s3 import S3Storage

__all__ = ["S3Storage"]


