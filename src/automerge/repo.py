from __future__ import annotations

import asyncio
import os
import time
from enum import Enum
from pathlib import Path
from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    List,
    Optional,
    Protocol,
    Tuple,
    Union,
)

from automerge._automerge import (
    AutomergeUrl,
    CheckAnnouncePolicyAction,
    CommandId,
    CommandResultCreateConnection,
    CommandResultCreateDocument,
    CommandResultFindDocument,
    ConnectionEventConnectionFailed,
    ConnectionEventHandshakeCompleted,
    ConnectionId,
    DisconnectAction,
    DispatchedCommand,
    DocumentActor,
    DocumentActorId,
    DocumentId,
    Hub,
    HubEvent,
    IoResult,
    IoTask,
    LoaderStateLoaded,
    LoaderStateNeedIo,
    PeerId,
    SamodLoader,
    SendAction,
    SpawnArgs,
    StorageKey,
    StorageResult,
    StorageTaskAction,
    StorageTaskDelete,
    StorageTaskLoad,
    StorageTaskLoadRange,
    StorageTaskPut,
)

from .connectors import (
    Backoff,
    Connection,
    ConnectorClosedError,
    Dialer,
    DialFailedError,
    Listener,
    PermanentDialError as PermanentDialError,
    _validate_peer_id,
)


class Storage(Protocol):
    """Protocol for storage adapters.

    Storage adapters are responsible for persisting repository data. They must
    implement the load, load_range, put, and delete methods to handle storage
    operations requested by the repository.
    """

    async def load(self, key: StorageKey) -> Optional[bytes]:
        """Load a value from storage.

        Args:
            key: The storage key to load

        Returns:
            The stored bytes if the key exists, None otherwise
        """
        ...

    async def load_range(self, prefix: StorageKey) -> List[Tuple[StorageKey, bytes]]:
        """Load all keys with the given prefix.

        Args:
            prefix: The key prefix to match

        Returns:
            List of (key, value) tuples for all matching keys
        """
        ...

    async def put(self, key: StorageKey, value: bytes) -> None:
        """Store a value in storage.

        Args:
            key: The storage key
            value: The bytes to store
        """
        ...

    async def delete(self, key: StorageKey) -> None:
        """Delete a value from storage.

        Args:
            key: The storage key to delete
        """
        ...


# Announce Policy Types


class AnnouncePolicy(Protocol):
    """Protocol for class-based announce policies.

    Announce policies control which documents are announced to which peers during
    synchronization. This allows for privacy, security, and performance optimization
    by selectively sharing documents based on custom logic.

    Implement this protocol by providing an async `should_announce` method that
    returns True if a document should be announced to a peer, False otherwise.

    Example:
        class TrustedPeerPolicy:
            def __init__(self, trusted_peers: set[str]):
                self.trusted_peers = trusted_peers

            async def should_announce(self, document_id: str, peer_id: str) -> bool:
                return peer_id in self.trusted_peers
    """

    async def should_announce(self, document_id: str, peer_id: str) -> bool:
        """Check if a document should be announced to a peer.

        Args:
            document_id: The document ID to potentially announce
            peer_id: The peer ID requesting the document

        Returns:
            True if the document should be announced, False otherwise
        """
        ...


# Function-based announce policy type
AnnouncePolicyFunc = Callable[[str, str], Awaitable[bool]]

# Combined type for Repo parameter
AnnouncePolicyType = Union[AnnouncePolicyFunc, AnnouncePolicy, None]


class InMemoryStorage:
    """Simple in-memory storage implementation.

    This storage adapter keeps all data in memory using a dictionary. It's
    useful for testing and development, but data is not persisted across
    process restarts.
    """

    def __init__(self):
        self._storage: Dict[str, bytes] = {}

    async def load(self, key: StorageKey) -> Optional[bytes]:
        """Load a value from memory."""
        key_str = self._key_to_str(key)
        return self._storage.get(key_str)

    async def load_range(self, prefix: StorageKey) -> List[Tuple[StorageKey, bytes]]:
        """Load all keys with the given prefix from memory."""
        prefix_str = self._key_to_str(prefix) + "/"
        results = []
        for key_str, value in self._storage.items():
            if key_str.startswith(prefix_str):
                # Convert back to StorageKey
                parts = key_str.split("/")
                key = StorageKey.from_parts(parts)
                results.append((key, value))
        return results

    async def put(self, key: StorageKey, value: bytes) -> None:
        """Store a value in memory."""
        key_str = self._key_to_str(key)
        self._storage[key_str] = value

    async def delete(self, key: StorageKey) -> None:
        """Delete a value from memory."""
        key_str = self._key_to_str(key)
        self._storage.pop(key_str, None)

    def _key_to_str(self, key: StorageKey) -> str:
        """Convert a StorageKey to a string for use as dict key."""
        return "/".join(key.to_parts())


class FileSystemStorage:
    """File system storage implementation.

    This storage adapter persists data to files on disk. Each storage key
    maps to a file path where all components except the last become directories
    and the last component becomes the filename.

    The top-level directory is splayed over two levels using the first two
    characters of the first key component (similar to git's object storage).
    For example, a key ["abc123", "data"] becomes "ab/c123/data".

    This design improves performance by avoiding directories with too many
    entries, which can slow down file system operations.
    """

    def __init__(self, base_path: Union[str, Path]):
        """Initialize the file system storage.

        Args:
            base_path: The base directory for storing files. Will be created
                if it doesn't exist.
        """
        self._base_path = Path(base_path)
        self._base_path.mkdir(parents=True, exist_ok=True)

    def _key_to_path(self, key: StorageKey) -> Path:
        """Convert a StorageKey to a file system path.

        The first component is splayed: "abc123" becomes "ab/c123".
        Remaining components become nested directories, with the last
        component being the filename.

        Args:
            key: The storage key to convert

        Returns:
            The file system path for this key

        Raises:
            ValueError: If the first key component has fewer than 3 characters
        """
        parts = key.to_parts()
        if not parts:
            raise ValueError("Storage key must have at least one part")

        # Splay the first component: use first 2 chars as a directory
        first = parts[0]
        if len(first) < 3:
            raise ValueError(
                f"First key component must have at least 3 characters, got {len(first)!r}"
            )

        splayed = [first[:2], first[2:]]

        # Build the full path: base / splay_dir / splay_rest / remaining_parts
        path_parts = splayed + list(parts[1:])
        return self._base_path.joinpath(*path_parts)

    def _path_to_key(self, path: Path) -> StorageKey:
        """Convert a file system path back to a StorageKey.

        Reverses the splaying operation to reconstruct the original key.

        Args:
            path: The file system path (relative to base_path)

        Returns:
            The reconstructed StorageKey
        """
        # Get path parts relative to base
        rel_path = path.relative_to(self._base_path)
        parts = list(rel_path.parts)

        if len(parts) < 2:
            raise ValueError(f"Invalid storage path: {path}")

        # Unsplay the first two components back into one
        splay_dir = parts[0]
        splay_rest = parts[1]

        # Reconstruct original first component
        first = splay_dir + splay_rest

        # Rebuild the key parts
        key_parts = [first] + parts[2:]
        return StorageKey.from_parts(key_parts)

    async def load(self, key: StorageKey) -> Optional[bytes]:
        """Load a value from the file system.

        Args:
            key: The storage key to load

        Returns:
            The stored bytes if the file exists, None otherwise
        """
        path = self._key_to_path(key)
        if not path.exists():
            return None

        # Use asyncio to avoid blocking
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, path.read_bytes)

    async def load_range(self, prefix: StorageKey) -> List[Tuple[StorageKey, bytes]]:
        """Load all keys with the given prefix from the file system.

        Uses directory listing to find all files under the prefix path.

        Args:
            prefix: The key prefix to match

        Returns:
            List of (key, value) tuples for all matching keys
        """
        prefix_path = self._key_to_path(prefix)

        # If prefix path doesn't exist, return empty list
        if not prefix_path.exists():
            return []

        results = []
        loop = asyncio.get_event_loop()

        # Walk the directory tree
        def collect_files():
            files = []
            if prefix_path.is_dir():
                for root, _, filenames in os.walk(prefix_path):
                    for filename in filenames:
                        file_path = Path(root) / filename
                        files.append(file_path)
            return files

        files = await loop.run_in_executor(None, collect_files)

        # Read all files and convert paths back to keys
        for file_path in files:
            key = self._path_to_key(file_path)
            value = await loop.run_in_executor(None, file_path.read_bytes)
            results.append((key, value))

        return results

    async def put(self, key: StorageKey, value: bytes) -> None:
        """Store a value to the file system.

        Creates parent directories as needed.

        Args:
            key: The storage key
            value: The bytes to store
        """
        path = self._key_to_path(key)

        # Create parent directories
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None, lambda: path.parent.mkdir(parents=True, exist_ok=True)
        )

        # Write the file
        await loop.run_in_executor(None, lambda: path.write_bytes(value))

    async def delete(self, key: StorageKey) -> None:
        """Delete a value from the file system.

        Args:
            key: The storage key to delete
        """
        path = self._key_to_path(key)

        if not path.exists():
            return

        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, path.unlink)


class Transport(Protocol):
    """Protocol for network transports.

    Transports handle sending and receiving messages between peers.
    """

    async def send(self, msg: bytes):
        """Send a message to the peer.

        Args:
            msg: The message bytes to send
        """
        ...

    async def recv(self) -> bytes:
        """Receive a message from the peer.

        Returns:
            The received message bytes
        """
        ...

    async def close(self):
        """Close the transport connection.

        This should clean up any resources and stop any background tasks
        associated with the transport.
        """
        ...


class ConnFinishedReason(Enum):
    Shutdown = 0
    TheyDisconnected = 1
    WeDisconnected = 2
    ErrorSending = 3
    ErrorReceiving = 4


class Repo:
    """Automerge repository for managing documents with synchronization.

    The Repo manages multiple Automerge documents, handles synchronization
    with other peers, and persists data through a storage adapter.

    Lifecycle:
        1. Load the repository from storage (creates Hub)
        2. Start the Hub event loop
        3. Use the repository
        4. Stop the Hub event loop

    Examples:
        ```python
        # Using context manager (recommended)
        repo = await Repo.load(InMemoryStorage())
        async with repo:
            handle = await repo.create()
            # Use the repo...

        # Manual lifecycle control (advanced)
        repo = await Repo.load(storage)
        await repo.start()
        try:
            # Use the repo...
        finally:
            await repo.stop()
        ```
    """

    def __init__(
        self,
        storage: Storage,
        peer_id: PeerId,
        hub: Hub,
        announce_policy: AnnouncePolicyType = None,
    ):
        """Initialize a repository (internal - use Repo.load() instead).

        Args:
            storage: Storage adapter for persisting repository data
            peer_id: The peer ID for this repository
            hub: The loaded Hub instance
            announce_policy: Optional policy to control document announcement to peers.
                If None, all documents are announced to all peers (default).
        """
        self._storage = storage
        self._peer_id = peer_id
        self._hub = hub
        self._announce_policy = announce_policy
        self._hub_task: Optional[asyncio.Task] = None
        self._hub_event_queue: asyncio.Queue[HubEvent] = asyncio.Queue()
        self._pending_commands: Dict[CommandId, asyncio.Future] = {}
        self._command_callbacks: dict[CommandId, Callable] = {}
        self._event_waiters: set[asyncio.Future] = set()
        self._connections: dict[ConnectionId, Connection] = {}
        self._dialers: dict[int, Dialer] = {}
        self._listeners: dict[int, Listener] = {}
        self._closing = False
        self._stop_task: asyncio.Task | None = None
        self._shutdown_event = asyncio.Event()
        self._started = False

        # Document actor management
        self._doc_actors: Dict[DocumentActorId, DocumentActor] = {}
        self._doc_actor_tasks: Dict[DocumentActorId, asyncio.Task] = {}
        self._doc_actor_queues: Dict[DocumentActorId, asyncio.Queue] = {}
        self._actor_to_doc: Dict[
            DocumentActorId, DocumentId
        ] = {}  # Maps actor_id to document_id

        # Connection management
        self._transports: Dict[ConnectionId, Transport] = {}
        self._recv_tasks: Dict[ConnectionId, asyncio.Task] = {}
        self._send_tasks: Dict[ConnectionId, asyncio.Task] = {}
        self._send_queues: Dict[ConnectionId, asyncio.Queue] = {}

        # Pending IO task tracking (for clean shutdown)
        self._pending_io_tasks: set = set()

    @classmethod
    async def load(
        cls,
        storage: Optional[Storage] = None,
        peer_id: Optional[bytes] = None,
        announce_policy: AnnouncePolicyType = None,
    ) -> "Repo":
        """Load a repository from storage.

        This runs the loader loop to initialize the Hub but does not start
        the Hub event loop. Call start() or use as a context manager to begin
        processing events.

        Args:
            storage: Storage adapter for persisting repository data
            peer_id: Optional peer ID bytes. If None, a random ID is generated.
            announce_policy: Optional policy to control which documents are announced
                to which peers during synchronization. Can be:
                - An async function: `async def policy(document_id: str, peer_id: str) -> bool`
                - An object implementing the AnnouncePolicy protocol with an async
                  `should_announce(document_id, peer_id)` method
                - None (default): all documents are announced to all peers

                Example function-based policy:
                    ```python
                    async def allow_public_docs(document_id: str, peer_id: str) -> bool:
                        # Only announce documents starting with "public-"
                        return document_id.startswith("public-")

                    repo = await Repo.load(storage, announce_policy=allow_public_docs)
                    ```

                Example class-based policy with state:
                    ```python
                    class TrustedPeerPolicy:
                        def __init__(self, trusted_peers: set[str]):
                            self.trusted_peers = trusted_peers

                        async def should_announce(self, document_id: str, peer_id: str) -> bool:
                            return peer_id in self.trusted_peers

                    policy = TrustedPeerPolicy({"peer-alice", "peer-bob"})
                    repo = await Repo.load(storage, announce_policy=policy)
                    ```

                Error Handling: If the policy raises an exception during evaluation,
                the error is logged and the document is NOT announced (fail-closed for
                security). This ensures that policy errors don't accidentally leak
                sensitive documents.

        Returns:
            A loaded Repo instance (not yet started)
        """
        # Create peer ID
        py_peer_id = PeerId.from_string(peer_id.hex()) if peer_id else PeerId.random()

        # Create loader
        loader = SamodLoader(py_peer_id)

        if storage is None:
            storage = InMemoryStorage()

        # Helper to execute IO task (need access to storage)
        async def execute_io_task(io_task: IoTask) -> IoResult:
            action = io_task.action

            if isinstance(action, StorageTaskAction):
                task = action.task

                if isinstance(task, StorageTaskLoad):
                    value = await storage.load(task.key)
                    storage_result = StorageResult.load(value)
                    return IoResult.from_storage_result(io_task.task_id, storage_result)

                elif isinstance(task, StorageTaskLoadRange):
                    values = await storage.load_range(task.prefix)
                    storage_result = StorageResult.load_range(values)
                    return IoResult.from_storage_result(io_task.task_id, storage_result)

                elif isinstance(task, StorageTaskPut):
                    await storage.put(task.key, bytes(task.value))
                    storage_result = StorageResult.put()
                    return IoResult.from_storage_result(io_task.task_id, storage_result)

                elif isinstance(task, StorageTaskDelete):
                    await storage.delete(task.key)
                    storage_result = StorageResult.delete()
                    return IoResult.from_storage_result(io_task.task_id, storage_result)

            raise ValueError(f"Unknown IO task action type during load: {type(action)}")

        # Loading loop - execute IO tasks until Hub is loaded
        while True:
            state = loader.step(time.time())

            if isinstance(state, LoaderStateNeedIo):
                # Execute all IO tasks
                tasks = state.tasks
                for io_task in tasks:
                    result = await execute_io_task(io_task)
                    loader.provide_io_result(result)
            elif isinstance(state, LoaderStateLoaded):
                # Loading complete - get the Hub
                hub = state.hub
                break

        # Create and return the Repo instance
        return cls(storage, py_peer_id, hub, announce_policy)

    async def start(self):
        """Start the Hub event loop.

        This begins processing events in the background. Can only be called once.

        Raises:
            RuntimeError: If the Hub loop is already started
        """
        if self._started:
            raise RuntimeError("Repo already started")
        if self._closing:
            raise ConnectorClosedError("Repo is closed")

        self._started = True
        self._hub_task = asyncio.create_task(self._hub_loop())

    async def stop(self):
        """Close all owned resources and stop the Hub, safely and idempotently."""
        if self._stop_task is None:
            if not self._started:
                return
            self._closing = True
            self._stop_task = asyncio.create_task(self._stop())
        await asyncio.shield(self._stop_task)

    async def _stop(self):
        # Drain registrations queued before stop began before taking ownership
        # snapshots. No new public connector registration is allowed now.
        await self._dispatch_event(HubEvent.tick())
        for connection in self._connections.values():
            if connection._reason is None:
                connection._reason = ConnFinishedReason.Shutdown
        resources = list(self._dialers.values()) + list(self._listeners.values())
        await asyncio.gather(*(resource.close() for resource in resources))
        # Process stop before setting the Python loop's shutdown flag.
        await self._dispatch_event(HubEvent.stop())
        self._shutdown_event.set()
        if self._hub_task:
            await self._hub_task
            self._hub_task = None
        # Hub shutdown satisfies any outstanding removal/close notifications.
        for future in list(self._event_waiters):
            if not future.done():
                future.set_result(None)
        # Include registrations that were already queued when shutdown began.
        resources = list(self._dialers.values()) + list(self._listeners.values())
        await asyncio.gather(*(resource.close() for resource in resources))
        await asyncio.gather(
            *(self._finish_connection(conn_id) for conn_id in list(self._connections))
        )

        # Wait for pending IO tasks (especially storage writes) to complete
        # before cancelling other tasks. This ensures data is persisted to disk
        # even on platforms with different asyncio scheduling (e.g. Windows ProactorEventLoop).
        if self._pending_io_tasks:
            await asyncio.gather(*list(self._pending_io_tasks), return_exceptions=True)

        # Cancel all remaining tasks
        for task in list(self._recv_tasks.values()):
            task.cancel()
        for task in list(self._send_tasks.values()):
            task.cancel()
        for task in list(self._doc_actor_tasks.values()):
            task.cancel()

        # Close any remaining transports
        # (The hub should have closed most of them via DisconnectActions)
        for conn_id, transport in list(self._transports.items()):
            try:
                await transport.close()
            except Exception as e:
                print(f"Error closing transport during shutdown: {e}")

        # Wait for all tasks to complete
        all_tasks = (
            list(self._recv_tasks.values())
            + list(self._send_tasks.values())
            + list(self._doc_actor_tasks.values())
        )
        if all_tasks:
            await asyncio.gather(*all_tasks, return_exceptions=True)

        # Cancel any pending commands
        for future in self._pending_commands.values():
            if not future.done():
                future.cancel()

        self._command_callbacks.clear()
        for future in list(self._event_waiters):
            if not future.done():
                future.cancel()
        self._started = False

    async def __aenter__(self):
        """Enter the async context manager - start the Hub loop if not started."""
        if not self._started:
            await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Exit the async context manager - stop the Hub loop."""
        await self.stop()
        return False

    async def _hub_loop(self):
        """Main Hub event processing loop.

        This loop:
        1. Waits for events from the event queue
        2. Processes events through the Hub
        3. Executes resulting IO tasks
        4. Completes pending commands
        5. Handles connection events
        """
        while not self._shutdown_event.is_set():
            try:
                # Wait for an event with timeout to check shutdown
                try:
                    event = await asyncio.wait_for(
                        self._hub_event_queue.get(), timeout=0.1
                    )
                except asyncio.TimeoutError:
                    # Send periodic tick event
                    event = HubEvent.tick()

                event_waiter = None
                if isinstance(event, tuple):
                    event, event_waiter = event
                # Process event through Hub.
                try:
                    results = self._hub.handle_event(time.time(), event)
                except Exception as error:
                    if event_waiter is not None and not event_waiter.done():
                        event_waiter.set_exception(error)
                    raise

                # Execute new IO tasks
                for io_task in results.new_tasks:
                    # Execute task and feed result back to Hub
                    self._create_io_task(self._handle_io_task(io_task))

                # Complete pending commands
                for cmd_id, cmd_result in results.completed_commands.items():
                    # If this is a create_connection command, set up send queue immediately
                    if isinstance(cmd_result, CommandResultCreateConnection):
                        conn_id = cmd_result.connection_id
                        if conn_id not in self._send_queues:
                            self._send_queues[conn_id] = asyncio.Queue()

                    future = self._pending_commands.pop(cmd_id, None)
                    callback = self._command_callbacks.pop(cmd_id, None)
                    if callback is not None:
                        try:
                            cmd_result = callback(cmd_result)
                        except Exception as error:
                            if future is not None and not future.done():
                                future.set_exception(error)
                            continue
                    if future is not None and not future.done():
                        future.set_result(cmd_result)
                    elif callback is not None:
                        # Cancellation must not leak a successful registration.
                        self._create_io_task(cmd_result.close())

                for conn_event in results.connection_events:
                    connection = self._connections.get(conn_event.connection_id)
                    if connection is None:
                        continue
                    if isinstance(conn_event, ConnectionEventHandshakeCompleted):
                        connection._handshake_completed(conn_event.peer_info.peer_id)
                    elif isinstance(conn_event, ConnectionEventConnectionFailed):
                        connection._failed(conn_event.error)

                for dialer_id in results.failed_dialers:
                    dialer = self._dialers.get(dialer_id)
                    if dialer is not None:
                        dialer._fail(
                            dialer._last_error
                            or DialFailedError("Retry budget exhausted")
                        )
                for dialer_id, _url in results.dial_requests:
                    dialer = self._dialers.get(dialer_id)
                    if dialer is not None:
                        dialer._start_attempt()

                # Handle document actor spawning
                for spawn_args in results.spawn_actors:
                    self._create_io_task(self._spawn_document_actor(spawn_args))

                # Handle messages to document actors
                for actor_id, msg in results.actor_messages:
                    if actor_id in self._doc_actor_queues:
                        await self._doc_actor_queues[actor_id].put(msg)
                    else:
                        print(f"Warning: Message for unknown actor {actor_id}")

                if event_waiter is not None and not event_waiter.done():
                    event_waiter.set_result(None)
                # Check if Hub stopped
                if results.stopped:
                    break

            except Exception as e:
                # Log error but keep loop running
                print(f"Error in Hub loop: {e}")
                import traceback

                traceback.print_exc()

    async def _check_announce_policy(self, document_id: str, peer_id: str) -> bool:
        """Check announce policy for a document and peer.

        Args:
            document_id: The document ID to check
            peer_id: The peer ID to check against

        Returns:
            True if the document should be announced, False otherwise
        """
        if self._announce_policy is None:
            # Default: announce everything (backwards compatible)
            return True

        try:
            if callable(self._announce_policy):
                # Async function
                return await self._announce_policy(document_id, peer_id)
            else:
                # Class-based policy
                return await self._announce_policy.should_announce(document_id, peer_id)
        except Exception as e:
            # Fail closed: deny on error for security
            print(
                f"Error in announce policy check for document {document_id}, peer {peer_id}: {e}"
            )
            import traceback

            traceback.print_exc()
            return False

    async def _execute_io_task(
        self, io_task: IoTask, actor_id: Optional[DocumentActorId] = None
    ) -> IoResult:
        """Execute a single IO task and return the result.

        Args:
            io_task: The IO task to execute
            actor_id: Optional document actor ID for document-specific tasks

        Returns:
            The result of executing the task
        """
        action = io_task.action

        if isinstance(action, StorageTaskAction):
            # Execute storage operation - use isinstance to determine task type
            task = action.task

            if isinstance(task, StorageTaskLoad):
                value = await self._storage.load(task.key)
                storage_result = StorageResult.load(value)
                return IoResult.from_storage_result(io_task.task_id, storage_result)

            elif isinstance(task, StorageTaskLoadRange):
                values = await self._storage.load_range(task.prefix)
                storage_result = StorageResult.load_range(values)
                return IoResult.from_storage_result(io_task.task_id, storage_result)

            elif isinstance(task, StorageTaskPut):
                await self._storage.put(task.key, bytes(task.value))
                storage_result = StorageResult.put()
                return IoResult.from_storage_result(io_task.task_id, storage_result)

            elif isinstance(task, StorageTaskDelete):
                await self._storage.delete(task.key)
                storage_result = StorageResult.delete()
                return IoResult.from_storage_result(io_task.task_id, storage_result)

        elif isinstance(action, SendAction):
            # Enqueue message for sending - the send loop will handle it in order
            conn_id = action.connection_id

            if conn_id in self._send_queues:
                # Queue the IO task - the send loop will process it
                await self._send_queues[conn_id].put(io_task)
            else:
                # Connection doesn't exist or was already closed
                raise ValueError(f"Connection {conn_id} not found")

            # Return None - we'll send io_complete from the send loop
            return None

        elif isinstance(action, DisconnectAction):
            await self._finish_connection(action.connection_id)
            return IoResult.from_disconnect_result(io_task.task_id)

        elif isinstance(action, CheckAnnouncePolicyAction):
            # Check announce policy
            if actor_id is None:
                raise ValueError("actor_id required for CheckAnnouncePolicyAction")

            # Look up document_id from actor_id
            document_id = self._actor_to_doc.get(actor_id)
            if document_id is None:
                # Actor not found - deny by default
                print(
                    f"Warning: actor_id {actor_id} not found in _actor_to_doc mapping"
                )
                should_announce = False
            else:
                # Get peer_id from action and check policy
                peer_id = str(action.peer_id)
                document_id_str = str(document_id)
                should_announce = await self._check_announce_policy(
                    document_id_str, peer_id
                )

            return IoResult.from_check_announce_policy_result(
                io_task.task_id, should_announce=should_announce
            )

        raise ValueError(f"Unknown IO task action type: {type(action)}")

    def _create_io_task(self, coro) -> asyncio.Task:
        """Create an asyncio task and track it for cleanup during shutdown.

        This ensures that pending IO operations (especially storage writes)
        complete before the repo shuts down, preventing data loss.

        Args:
            coro: The coroutine to run as a task

        Returns:
            The created asyncio Task
        """
        task = asyncio.create_task(coro)
        self._pending_io_tasks.add(task)
        task.add_done_callback(self._pending_io_tasks.discard)
        return task

    async def _handle_io_task(self, io_task: IoTask):
        """Execute an IO task and feed the result back to the Hub.

        Args:
            io_task: The IO task to execute
        """
        try:
            result = await self._execute_io_task(io_task)
            # Send io_complete event to Hub for network operations
            # Storage operations complete synchronously within handle_event
            if result is not None:
                await self._hub_event_queue.put(HubEvent.io_complete(result))
        except Exception as e:
            print(f"Error executing IO task: {e}")
            import traceback

            traceback.print_exc()

    async def _dispatch_command(
        self, command: DispatchedCommand, *, on_result: Callable | None = None
    ) -> Any:
        future = asyncio.get_running_loop().create_future()
        self._pending_commands[command.command_id] = future
        if on_result is not None:
            self._command_callbacks[command.command_id] = on_result
        self._hub_event_queue.put_nowait(command.event)
        return await future

    async def _dispatch_event(self, event: HubEvent):
        if not self._started or self._hub.is_stopped():
            raise ConnectorClosedError("Repo is stopped")
        future = asyncio.get_running_loop().create_future()
        self._event_waiters.add(future)
        self._hub_event_queue.put_nowait((event, future))
        try:
            await future
        finally:
            self._event_waiters.discard(future)

    def _require_running(self):
        if not self._started or self._closing or self._hub.is_stopped():
            raise ConnectorClosedError("Repo must be running")

    async def add_dialer(
        self,
        url: str,
        *,
        connect: Callable[[], Awaitable[Transport]],
        backoff: Backoff = Backoff(),
        expected_peer_id: PeerId | None = None,
    ) -> Dialer:
        """Register an outgoing connector; returns without waiting for a handshake.

        ``connect`` must open a fresh transport on every attempt. Raise
        ``PermanentDialError`` from it for failures that cannot be retried.
        The repository owns the returned dialer until it is closed.
        """
        self._require_running()
        if not callable(connect):
            raise TypeError("connect must be a transport factory")
        if not isinstance(backoff, Backoff):
            raise TypeError("backoff must be a Backoff")
        _validate_peer_id(expected_peer_id)
        command = HubEvent.add_dialer(
            url, backoff.initial_delay, backoff.max_delay, backoff.max_retries
        )

        def registered(dialer_id):
            dialer = Dialer(self, dialer_id, url, connect, backoff, expected_peer_id)
            self._dialers[dialer_id] = dialer
            if self._closing:
                self._create_io_task(dialer.close())
            return dialer

        return await self._dispatch_command(command, on_result=registered)

    async def add_listener(self, url: str) -> Listener:
        """Register a logical inbound endpoint; this does not bind a socket."""
        self._require_running()
        command = HubEvent.add_listener(url)

        def registered(listener_id):
            listener = Listener(self, listener_id, url)
            self._listeners[listener_id] = listener
            if self._closing:
                self._create_io_task(listener.close())
            return listener

        return await self._dispatch_command(command, on_result=registered)

    async def _remove_connector(self, connector):
        for connection in connector._connections:
            if connection._reason is None:
                connection._reason = (
                    ConnFinishedReason.Shutdown
                    if self._closing
                    else ConnFinishedReason.WeDisconnected
                )
        if isinstance(connector, Dialer):
            event = HubEvent.remove_dialer(connector._id)
            mapping = self._dialers
        else:
            event = HubEvent.remove_listener(connector._id)
            mapping = self._listeners
        if self._started and not self._hub.is_stopped():
            await self._dispatch_event(event)
        connections = list(connector._connections)
        await asyncio.gather(*(connection.close() for connection in connections))
        mapping.pop(connector._id, None)

    async def _attach_transport(
        self, command, transport, owner: Dialer | Listener
    ) -> Connection:
        if not self._started or self._hub.is_stopped():
            await transport.close()
            raise ConnectorClosedError("Repo is stopped")
        if not all(
            callable(getattr(transport, method, None))
            for method in ("send", "recv", "close")
        ):
            if callable(getattr(transport, "close", None)):
                await transport.close()
            raise TypeError("Factory must return a Transport")

        def attached(result):
            conn_id = result.connection_id
            connection = Connection(self, conn_id, transport, owner)
            self._connections[conn_id] = connection
            self._transports[conn_id] = transport
            self._send_queues.setdefault(conn_id, asyncio.Queue())
            owner._connections.add(connection)
            if isinstance(owner, Dialer):
                owner._current = connection
            self._send_tasks[conn_id] = asyncio.create_task(
                self._send_loop(conn_id, transport)
            )
            self._recv_tasks[conn_id] = asyncio.create_task(
                self._recv_loop(conn_id, transport)
            )
            if self._closing:
                connection._reason = ConnFinishedReason.Shutdown
            if self._closing or owner.closed:
                self._create_io_task(connection.close())
            return connection

        return await self._dispatch_command(command, on_result=attached)

    def _connection_lost(self, conn_id, reason, error=None):
        connection = self._connections.get(conn_id)
        if connection is not None:
            if connection._reason is None:
                connection._reason = reason
            if error is not None:
                connection.error = error
        if self._started and not self._hub.is_stopped():
            self._hub_event_queue.put_nowait(HubEvent.connection_lost(conn_id))

    async def _close_connection(self, connection):
        if connection._finished.done():
            return
        if connection._reason is None:
            connection._reason = ConnFinishedReason.WeDisconnected
        if self._started and not self._hub.is_stopped():
            await self._dispatch_event(HubEvent.connection_lost(connection._id))
        await self._finish_connection(connection._id)
        await connection.wait_closed()

    async def _finish_connection(self, conn_id):
        connection = self._connections.get(conn_id)
        if connection is None:
            return
        if connection._cleanup_task is None:
            connection._cleanup_task = asyncio.create_task(
                self._cleanup_connection(connection)
            )
        await asyncio.shield(connection._cleanup_task)

    async def _cleanup_connection(self, connection):
        conn_id = connection._id
        tasks = []
        for mapping in (self._recv_tasks, self._send_tasks):
            task = mapping.pop(conn_id, None)
            if task is not None:
                task.cancel()
                tasks.append(task)
        self._send_queues.pop(conn_id, None)
        self._transports.pop(conn_id, None)
        try:
            await connection._transport.close()
        except Exception as error:
            connection.error = error
        finally:
            await asyncio.gather(*tasks, return_exceptions=True)
            self._connections.pop(conn_id, None)
            connection._finish(connection._reason or ConnFinishedReason.WeDisconnected)

    async def _spawn_document_actor(self, spawn_args: SpawnArgs):
        """Spawn a new document actor.

        Args:
            spawn_args: Arguments for spawning the actor
        """
        actor_id = spawn_args.actor_id
        document_id = spawn_args.document_id

        # Create the document actor - returns (actor, initial_result)
        actor, initial_result = DocumentActor.new(time.time(), spawn_args)

        # Store the actor and document mapping
        self._doc_actors[actor_id] = actor
        self._actor_to_doc[actor_id] = document_id

        # Create message queue for this actor
        queue = asyncio.Queue()
        self._doc_actor_queues[actor_id] = queue

        # Handle initial IO tasks from spawn
        for io_task in initial_result.io_tasks:
            self._create_io_task(self._handle_doc_actor_io(actor_id, io_task))

        # Handle initial outgoing messages
        for msg in initial_result.outgoing_messages:
            # Send message back to Hub event queue
            await self._hub_event_queue.put(HubEvent.actor_message(actor_id, msg))

        # Start the actor's control loop
        task = self._create_io_task(self._document_actor_loop(actor_id))
        self._doc_actor_tasks[actor_id] = task

    async def _document_actor_loop(self, actor_id: DocumentActorId):
        """Main loop for a document actor.

        Args:
            actor_id: The document actor ID
        """
        queue = self._doc_actor_queues[actor_id]
        actor = self._doc_actors[actor_id]

        try:
            while not self._shutdown_event.is_set():
                try:
                    # Wait for message with timeout to check shutdown
                    msg = await asyncio.wait_for(queue.get(), timeout=0.1)
                except asyncio.TimeoutError:
                    continue

                # Process message through document actor
                result = actor.handle_message(time.time(), msg)

                # Execute IO tasks
                for io_task in result.io_tasks:
                    self._create_io_task(self._handle_doc_actor_io(actor_id, io_task))

                # Send outgoing messages to Hub
                for outgoing_msg in result.outgoing_messages:
                    await self._hub_event_queue.put(
                        HubEvent.actor_message(actor_id, outgoing_msg)
                    )

                # Check if actor stopped
                if result.stopped:
                    break

        except Exception as e:
            print(f"Error in document actor loop for {actor_id}: {e}")
            import traceback

            traceback.print_exc()
        finally:
            # Clean up
            if actor_id in self._doc_actors:
                del self._doc_actors[actor_id]
            if actor_id in self._doc_actor_queues:
                del self._doc_actor_queues[actor_id]
            if actor_id in self._doc_actor_tasks:
                del self._doc_actor_tasks[actor_id]
            if actor_id in self._actor_to_doc:
                del self._actor_to_doc[actor_id]

    async def _handle_doc_actor_io(self, actor_id: DocumentActorId, io_task: IoTask):
        """Execute an IO task for a document actor.

        Args:
            actor_id: The document actor ID
            io_task: The IO task to execute
        """
        try:
            # Execute the IO task (pass actor_id for document-specific tasks)
            result = await self._execute_io_task(io_task, actor_id)

            # Get the actor and feed the result back
            if actor_id in self._doc_actors:
                actor = self._doc_actors[actor_id]
                doc_result = actor.handle_io_complete(time.time(), result)

                # Execute any new IO tasks that resulted
                for new_io_task in doc_result.io_tasks:
                    self._create_io_task(
                        self._handle_doc_actor_io(actor_id, new_io_task)
                    )

                # Handle outgoing messages
                for outgoing_msg in doc_result.outgoing_messages:
                    await self._hub_event_queue.put(
                        HubEvent.actor_message(actor_id, outgoing_msg)
                    )

        except Exception as e:
            print(f"Error handling IO task for document actor {actor_id}: {e}")
            import traceback

            traceback.print_exc()

    async def _send_loop(self, conn_id: ConnectionId, transport: Transport):
        """Send loop for a connection.

        Continuously processes send tasks from the queue and sends messages
        in FIFO order to maintain message ordering required by samod-core.

        Args:
            conn_id: The connection ID
            transport: The transport to send on
        """
        try:
            queue = self._send_queues[conn_id]
            while not self._shutdown_event.is_set():
                try:
                    # Wait for a send task with timeout to check shutdown
                    io_task = await asyncio.wait_for(queue.get(), timeout=0.1)
                except asyncio.TimeoutError:
                    continue

                # Extract the message from the SendAction
                action = io_task.action
                if not isinstance(action, SendAction):
                    print(f"Warning: Non-SendAction in send queue: {type(action)}")
                    continue

                message = action.message

                try:
                    # Send the message
                    await transport.send(bytes(message))

                    # Send io_complete event to Hub
                    result = IoResult.from_send_result(io_task.task_id)
                    await self._hub_event_queue.put(HubEvent.io_complete(result))

                except asyncio.CancelledError:
                    self._connection_lost(conn_id, ConnFinishedReason.WeDisconnected)
                    break
                except Exception as e:
                    self._connection_lost(conn_id, ConnFinishedReason.ErrorSending, e)
                    break

        finally:
            # Clean up
            if conn_id in self._send_queues:
                del self._send_queues[conn_id]
            if conn_id in self._send_tasks:
                del self._send_tasks[conn_id]

    async def _recv_loop(self, conn_id: ConnectionId, transport: Transport):
        """Receive loop for a connection.

        Continuously receives messages from the transport and dispatches them
        to the Hub until the connection is closed or an error occurs.

        Args:
            conn_id: The connection ID
            transport: The transport to receive from
        """
        try:
            while not self._shutdown_event.is_set():
                try:
                    # Receive message from transport
                    msg = await transport.recv()

                    # Put receive event in queue (don't wait for completion to avoid blocking recv loop)
                    receive_cmd = HubEvent.receive(conn_id, msg)
                    await self._hub_event_queue.put(receive_cmd.event)

                except asyncio.CancelledError:
                    self._connection_lost(conn_id, ConnFinishedReason.WeDisconnected)
                    break
                except Exception as e:
                    self._connection_lost(conn_id, ConnFinishedReason.ErrorReceiving, e)
                    break

        finally:
            # Clean up
            if conn_id in self._transports:
                del self._transports[conn_id]
            if conn_id in self._recv_tasks:
                del self._recv_tasks[conn_id]
            # Cancel send loop if it's still running
            if conn_id in self._send_tasks:
                self._send_tasks[conn_id].cancel()
            if conn_id in self._send_queues:
                del self._send_queues[conn_id]

    async def create(self, doc: Optional[Dict[str, Any]] = None) -> "DocHandle":
        """Create a new document in the repository.

        Args:
            doc: Optional initial document content (not yet supported)

        Returns:
            A handle to the created document
        """
        # Dispatch create_document command
        command = HubEvent.create_document()
        result = await self._dispatch_command(command)

        # Result should be CommandResultCreateDocument
        if not isinstance(result, CommandResultCreateDocument):
            raise RuntimeError(f"Unexpected result type: {type(result)}")

        # Return a handle to the document
        return DocHandle(result.actor_id, result.document_id, self)

    async def find(self, url: AutomergeUrl | str) -> Optional["DocHandle"]:
        """Find an existing document by URL.

        This method searches for a document by its URL. If the document exists
        in this repository (either because it was created here or synced from
        a peer), returns a handle to it. Otherwise returns None.

        Args:
            url: The document URL to find

        Returns:
            A handle to the document if found, None otherwise

        Example:
            >>> url = handle_a.url
            >>> handle_b = await repo_b.find(url)
            >>> if handle_b:
            ...     print("Document found!")
        """
        document_id: DocumentId
        if isinstance(url, str):
            url2 = AutomergeUrl.from_str(url)
            document_id = url2.document_id()
        else:
            # Extract document ID from URL
            document_id = url.document_id()

        # Dispatch find_document command
        command = HubEvent.find_document(document_id)
        result = await self._dispatch_command(command)

        # Result should be CommandResultFindDocument
        if not isinstance(result, CommandResultFindDocument):
            raise RuntimeError(f"Unexpected result type: {type(result)}")

        # If not found, return None
        if not result.found:
            return None

        # Document was found - return a handle
        return DocHandle(result.actor_id, document_id, self)


class ChangeContext:
    """Context manager for document modifications.

    This class is returned by DocHandle.change() and provides a context manager
    interface for modifying documents. Changes are committed when the context
    exits normally, or rolled back if an exception is raised.

    Example:
        with handle.change() as doc:
            doc["key"] = "value"
            doc["count"] = 42

    Note:
        - Do not call `handle.doc()` inside the change block
        - Do not nest change() calls
    """

    def __init__(self, handle: "DocHandle"):
        """Initialize the change context.

        Args:
            handle: The DocHandle to modify
        """
        self._handle = handle
        self._guard = None

    def __enter__(self):
        """Enter the change context and return a mutable document proxy.

        Returns:
            MapWriteProxy: A dict-like mutable view of the document
        """
        import automerge.core as core

        from .document import MapWriteProxy

        # Get the document actor
        actor = self._handle._repo._doc_actors.get(self._handle._actor_id)
        if actor is None:
            raise ValueError(f"Document actor {self._handle._actor_id} not found")

        # Begin the change - this acquires the mutex and creates a transaction
        self._guard = actor.begin_change()
        tx = self._guard.transaction()
        return MapWriteProxy(tx, core.ROOT, None)

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        """Exit the change context, committing or rolling back.

        Args:
            exc_type: Exception type if an exception was raised, None otherwise
            exc_val: Exception value if an exception was raised, None otherwise
            exc_tb: Exception traceback if an exception was raised, None otherwise

        Returns:
            False (never suppress exceptions)
        """
        if self._guard is None:
            return False

        try:
            commit = exc_type is None
            result = self._guard.end_change(commit, time.time())

            # Always process side effects - DocActorResult may contain effects
            # even if the transaction was rolled back
            for io_task in result.io_tasks:
                self._handle._repo._create_io_task(
                    self._handle._repo._handle_doc_actor_io(
                        self._handle._actor_id, io_task
                    )
                )

            for msg in result.outgoing_messages:
                self._handle._repo._hub_event_queue.put_nowait(
                    HubEvent.actor_message(self._handle._actor_id, msg)
                )

            # Emit change event only if there were actual changes
            if result.patches:
                self._handle._emit("change", result.patches)
        finally:
            self._guard = None

        return False  # Don't suppress exceptions


class DocHandle:
    """Handle to an Automerge document in the repository.

    DocHandle provides access to a document managed by the repository.
    It allows reading and modifying the document.
    """

    def __init__(self, actor_id: DocumentActorId, document_id: DocumentId, repo: Repo):
        """Initialize a document handle.

        Args:
            actor_id: The document actor ID
            document_id: The document ID
            repo: The repository managing this document
        """
        self._actor_id = actor_id
        self._document_id = document_id
        self._repo = repo
        self._event_callbacks: Dict[str, List[Callable]] = {}

    @property
    def url(self) -> AutomergeUrl:
        """Get the document's URL.

        Returns:
            The AutomergeUrl for this document
        """
        return AutomergeUrl.from_document_id(self._document_id)

    @property
    def document_id(self) -> DocumentId:
        """Get the document ID.

        Returns:
            The DocumentId
        """
        return self._document_id

    def doc(self):
        """Return a read-only view of the document.

        This document reference is read-only. Any attempt to modify it will
        raise an exception. Use change() for mutations.

        Returns:
            MapReadProxy: A dict-like read-only view of the document

        Example:
            >>> doc = handle.doc()
            >>> print(doc["content"])
            >>> print(doc["nested"]["value"])

        Note:
            Each property access blocks while acquiring the document lock.
            For write operations, use DocHandle.change() instead.
        """
        import automerge.core as core

        from .document import MapReadProxy

        # Get the document actor
        actor = self._repo._doc_actors.get(self._actor_id)
        if actor is None:
            raise ValueError(f"Document actor {self._actor_id} not found")

        # Get the actor-backed document
        core_doc = actor.get_document()

        # Wrap in MapReadProxy for Pythonic dict-like access
        return MapReadProxy(core_doc, core.ROOT, None)

    def change(self) -> ChangeContext:
        """Return a context manager for modifying the document.

        Example:
            with handle.change() as doc:
                doc["key"] = "value"
                doc["count"] = 42

        Note:
            - Do not use `await` inside the change block
            - Do not call `handle.doc()` inside the change block
            - Do not nest change() calls

        Returns:
            ChangeContext: A context manager that provides mutable document access
        """
        return ChangeContext(self)

    def on(self, event: str, callback: Callable) -> None:
        """Register a callback for a document event.

        Args:
            event: The event name (currently only "change" is supported)
            callback: The function to call when the event occurs.
                     For "change" events, the callback receives a list of patches
                     describing the changes made to the document.

        Example:
            >>> def on_change(patches):
            ...     print(f"Document changed! {len(patches)} patches")
            >>> handle.on("change", on_change)
            >>> with handle.change() as doc:
            ...     doc["key"] = "value"
            # Will print "Document changed! N patches"
        """
        if event not in self._event_callbacks:
            self._event_callbacks[event] = []
        self._event_callbacks[event].append(callback)

    def off(self, event: str, callback: Callable) -> None:
        """Remove a callback for a document event.

        Args:
            event: The event name
            callback: The callback function to remove (must be the same object
                     that was passed to on())

        Example:
            >>> def on_change(patches):
            ...     print("Document changed!")
            >>> handle.on("change", on_change)
            >>> # Later, to remove the listener:
            >>> handle.off("change", on_change)
        """
        if event in self._event_callbacks and callback in self._event_callbacks[event]:
            self._event_callbacks[event].remove(callback)

    def _emit(self, event: str, *args) -> None:
        """Emit an event to all registered callbacks.

        Args:
            event: The event name to emit
            *args: Arguments to pass to the callbacks
        """
        if event in self._event_callbacks:
            for callback in self._event_callbacks[event]:
                try:
                    callback(*args)
                except Exception as e:
                    # Log the error but don't let it propagate
                    # This prevents one bad callback from breaking others
                    print(f"Error in {event} callback: {e}")

    def __repr__(self) -> str:
        return f"DocHandle(actor_id={self._actor_id}, document_id={self._document_id})"
