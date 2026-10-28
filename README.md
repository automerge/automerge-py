# Automerge-py

Python bindings for [Automerge](https://github.com/automerge/automerge).

## Quickstart

Install the bindings with `pip install automerge`.

For WebSocket support (optional):
```bash
pip install automerge[websocket]
```

For S3 storage support (optional):
```bash
pip install automerge[s3]
```

Now you can create a document and do all sorts of Automerge things with it!

> [!NOTE]
> This package contains both `automerge.core` (low-level wrapper) and
> `automerge.repo` (higher-level API with sync support).
>
> The low-level `automerge.core` provides direct access to the Rust automerge
> library. The higher-level `automerge.repo` provides document management,
> storage, and network synchronization.

```py
from automerge.core import Document, ROOT, ObjType, ScalarType

doc = Document()
with doc.transaction() as tx:
    list = tx.put_object(ROOT, "colours", ObjType.List)
    tx.insert(list, 0, ScalarType.Str, "blue")
    tx.insert(list, 1, ScalarType.Str, "red")

doc2 = doc.fork()
with doc2.transaction() as tx:
    tx.insert(list, 0, ScalarType.Str, "green")

with doc.transaction() as tx:
    tx.delete(list, 0)

doc.merge(doc2)  # `doc` now contains {"colours": ["green", "red"]}
```

For synchronization without `Repo`, see [low-level sync](docs/sync.md), including
publish-only peers, live read-only mode changes, and sync completion checks.

### Reading conflicts

The low-level `get()` method returns one deterministic winner when concurrent
writes conflict. Use `doc.get_all(obj_id, prop, heads=None)` to inspect every
value instead:

```py
from automerge.core import Document, ROOT, ScalarType

left = Document()
right = left.fork()
with left.transaction() as tx:
    tx.put(ROOT, "colour", ScalarType.Str, "blue")
with right.transaction() as tx:
    tx.put(ROOT, "colour", ScalarType.Str, "red")
left.merge(right)

conflicts = left.get_all(ROOT, "colour")
assert sorted(value[1] for value, _ in conflicts) == ["blue", "red"]
```

Each result is a `(value, id)` pair, using the same representation as `get()`:
scalars are `(ScalarType, Python value)` tuples; objects are `ObjType` values
whose byte IDs can be used to read their contents. Scalar IDs identify the
operations that assigned the values, so equal concurrent values can still
have distinct IDs.

Missing or deleted properties return `[]`; an unconflicted property returns
one pair. Pass a map key or list index as `prop`, and optional change hashes
as `heads` for historical reads (`heads=[]` selects the empty history).
`Transaction.get_all()` also sees pending writes; passing heads selects the
historical snapshot instead. Writing a new value after observing all conflicting
writes resolves the conflict.

### Using the High-Level API

The `automerge.Document` class provides a more Pythonic interface using proxies:

```py
from automerge import Document, ImmutableString

doc = Document()

# Use Python dict/list syntax
with doc.change() as d:
    d["title"] = "My Document"  # Creates collaborative Text by default
    d["version"] = ImmutableString(
        "1.0.0"
    )  # Use ImmutableString for non-editable strings
    d["tags"] = []
    d["tags"][0] = "python"
    d["tags"][1] = "automerge"

# Read values naturally
print(doc["title"])  # TextReadProxy that acts like a string
print(doc["version"])  # Regular Python string
print(len(doc["tags"]))  # 2

# Edit collaborative text
with doc.change() as d:
    d["title"].insert(0, "✨ ")  # Text objects support insert, delete, splice
    d["tags"][0].insert(6, " 3.12")

print(str(doc["title"]))  # "✨ My Document"
```

Text objects are collaborative sequences that automatically merge concurrent edits. Use `ImmutableString` when you need a simple, non-editable string value.

 ### Using automerge.repo with WebSocket Sync
 
 The `automerge.repo` module provides an API for managing synchronization and storage:
 
 ```py
 import asyncio
 from automerge.repo import Repo, InMemoryStorage
 from automerge.transports import WebSocketServer, WebSocketClientTransport
 from automerge import ROOT, ScalarType


 # Server side
 async def run_server():
     storage = InMemoryStorage()
     repo = await Repo.load(storage)

     async with repo:
         async with WebSocketServer(repo, "localhost", 8080):
             print("Server running on ws://localhost:8080")
             await asyncio.sleep(3600)  # Keep running


 # Client side
 async def run_client():
     storage = InMemoryStorage()
     repo = await Repo.load(storage)

     async with repo:
         # The factory opens a fresh transport on every connection attempt.
         url = "ws://localhost:8080"
         dialer = await repo.add_dialer(
             url, connect=lambda: WebSocketClientTransport.connect(url)
         )
         await dialer.wait_connected()

         # Create and modify documents - changes sync automatically!
         handle = await repo.create()

         with handle.change() as doc:
             doc["message"] = "Hello, Automerge!"

         # Read document contents using direct access
         doc = handle.doc()
         print(f"Message: {doc['message']}")
         await asyncio.sleep(3600)  # Keep syncing and reconnecting as needed.
 ```
 
 See the [examples/](examples/) directory for more complete examples and usage patterns.

For automatic reconnection and explicit connection ownership, use
`Repo.add_dialer()` and `Repo.add_listener()`. See the
[dialer, listener, and connection API guide](docs/connectors.md) for transport
factories, retry policies, identity checks, and lifecycle examples.

`Repo.find()` waits until a document is ready or currently unavailable. To
inspect search progress or keep watching for later availability, use
`Repo.search_for_doc()`. See the [document-search guide](docs/search.md).

Use `Repo.load(share_policy=...)` to grant read-only access or deny document
sharing per peer. See the [access-control guide](docs/share-policy.md) for policy
async iterators, live permission updates, and migration from one-shot callbacks.

### Using S3 Storage

The `automerge.storages.s3` module provides S3-backed storage for persisting documents:

```py
from automerge.storages.s3 import S3Storage
from automerge.repo import Repo

# Create S3-backed storage
storage = S3Storage(
    bucket="my-bucket",
    region="us-east-1",
    prefix="users/user-123",  # Optional: isolate data by user/tenant
)

# Use exactly like any other storage
repo = await Repo.load(storage)
async with repo:
    handle = await repo.create()
    with handle.change() as doc:
        doc["key"] = "value"
```

## Developing

```bash
# Create venv in "env" folder (required by maturin)
python3 -m venv env
# Activate venv
source ./env/bin/activate
# Install maturin
pip install maturin
# Build the bindings
maturin develop
```

### Testing

Install test dependencies:

```bash
pip install pytest pytest-asyncio "moto[server]"
```

Run tests:

```bash
# Run all tests
pytest tests/ -v

# Run specific test file
pytest tests/test_s3_storage_mock.py -v
```

> **Note**: The S3 mock tests use moto's server mode (`ThreadedMotoServer`) instead of
> decorators because aiobotocore uses aiohttp for HTTP requests, which bypasses moto's
> standard patching. Server mode runs a local S3-compatible HTTP endpoint that works
> correctly with async clients.
