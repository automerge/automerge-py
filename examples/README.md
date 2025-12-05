# Automerge Examples

This directory contains example scripts demonstrating how to use automerge-py with different transports.

## Prerequisites

Install automerge with WebSocket support:

```bash
pip install automerge[websocket]
```

Or if you're developing:

```bash
pip install -e .[websocket]
```

## WebSocket Examples

### Basic Server (`websocket_server.py`)

Runs a WebSocket server that shares Automerge documents with connected clients.

```bash
python examples/websocket_server.py
```

The server will:
- Start on `ws://localhost:8080`
- Create an initial document
- Accept connections from clients
- Show the number of active connections

### Basic Client (`websocket_client.py`)

Connects to a WebSocket server and syncs documents.

```bash
# First, start the server in another terminal:
python examples/websocket_server.py

# Then run the client:
python examples/websocket_client.py
```

The client will:
- Connect to the server
- Create a new document
- Make changes that sync to the server
- Show the document URL

## How It Works

### Server Side

```python
from automerge.repo import Repo, InMemoryStorage
from automerge.transports import WebSocketServer

# Create a repository
storage = InMemoryStorage()
repo = await Repo.load(storage)

# Start WebSocket server
async with repo:
    async with WebSocketServer(repo, "localhost", 8080):
        # Server is now running
        await asyncio.sleep(3600)
```

### Client Side

```python
from automerge.repo import Repo, InMemoryStorage
from automerge.transports import WebSocketClientTransport

# Create a repository
storage = InMemoryStorage()
repo = await Repo.load(storage)

# Connect to server
async with repo:
    transport = await WebSocketClientTransport.connect("ws://localhost:8080")
    await repo.connect(transport)
```

## Document Operations

### Creating a Document

```python
# Create a new document
handle = await repo.create()

# Initialize with content
def init_doc(doc):
    doc["title"] = "Hello"
    doc["count"] = 0

await handle.change(init_doc)
```

### Reading a Document

```python
# Find a document by URL
handle = await repo.find(url)

if handle:
    doc = handle.doc()
    title = doc["title"]
    print(f"Title: {title}")
```

### Modifying a Document

```python
# Make changes to a document
def update_count(doc):
    current = doc["count"]
    doc["count"] = current + 1

await handle.change(update_count)
```

### Important: Read vs Write Access

- **Reading**: Use `doc = handle.doc()` for read-only access with Python dict-like syntax
- **Writing**: Use `await handle.change(callback)` for mutations
- Documents from `doc()` are read-only and will raise errors on write attempts
- Use `change()` for all mutations

## Tips

- **Storage**: The examples use `InMemoryStorage` for simplicity. For production, implement a persistent storage backend.
- **Ports**: Make sure the ports used (8080, 8081) are available on your system.
