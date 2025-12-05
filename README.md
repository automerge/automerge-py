# Automerge-py

Python bindings for [Automerge](https://github.com/automerge/automerge).

## Quickstart

Install the bindings with `pip install automerge`.

For WebSocket support (optional):
```bash
pip install automerge[websocket]
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

### Using the High-Level API

The `automerge.Document` class provides a more Pythonic interface using proxies:

```py
from automerge import Document, ImmutableString

doc = Document()

# Use Python dict/list syntax
with doc.change() as d:
    d["title"] = "My Document"  # Creates collaborative Text by default
    d["version"] = ImmutableString("1.0.0")  # Use ImmutableString for non-editable strings
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
         # Connect to server
         transport = await WebSocketClientTransport.connect("ws://localhost:8080")
         await repo.connect(transport)
 
         # Create and modify documents - changes sync automatically!
         handle = await repo.create()
 
         def update_doc(doc):
             doc["message"] = "Hello, Automerge!"
 
         await handle.change(update_doc)
 
         # Read document contents using direct access
         doc = handle.doc()
         print(f"Message: {doc['message']}")
 ```
 
 See the [examples/](examples/) directory for more complete examples and usage patterns.
 
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
