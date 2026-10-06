"""Long-lived transport connectors and their individual connections.

Public types are also available from ``automerge.repo``. Factories create fresh
transports; samod, rather than a second Python retry loop, schedules retries.
"""

from __future__ import annotations

import asyncio
import math
from dataclasses import dataclass
from typing import TYPE_CHECKING, Awaitable, Callable, TypeVar

from ._automerge import HubEvent, PeerId

if TYPE_CHECKING:
    from .repo import ConnFinishedReason, ConnectionId, Repo, Transport


@dataclass(frozen=True)
class Backoff:
    """Exponential retry backoff with jitter; delays are measured in seconds.

    ``max_retries=None`` retries indefinitely; zero allows only the first attempt.
    The retry budget resets after a successful Automerge handshake.
    """

    initial_delay: float = 0.1
    max_delay: float = 30.0
    max_retries: int | None = None

    def __post_init__(self):
        for name in ("initial_delay", "max_delay"):
            value = getattr(self, name)
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(value)
                or value < 0
            ):
                raise ValueError(f"{name} must be finite and non-negative")
        if self.max_delay < self.initial_delay:
            raise ValueError("max_delay must be >= initial_delay")
        if self.max_retries is not None and (
            isinstance(self.max_retries, bool)
            or not isinstance(self.max_retries, int)
            or not 0 <= self.max_retries <= 0xFFFFFFFF
        ):
            raise ValueError("max_retries must be None or a non-negative u32")


class DialFailedError(RuntimeError):
    """A dialer permanently failed or exhausted its retry budget."""


class PermanentDialError(DialFailedError):
    """Raise from a transport factory when retrying cannot succeed."""


class ConnectorClosedError(RuntimeError):
    """An operation requires an open connector or running repository."""


def _validate_peer_id(peer_id):
    if peer_id is not None and not isinstance(peer_id, PeerId):
        raise TypeError("expected_peer_id must be a PeerId or None")


_ResourceType = TypeVar("_ResourceType", bound="_Resource")


class _Resource:
    def __init__(self, repo: Repo):
        self._repo = repo
        self._closed = False
        self._close_task: asyncio.Task | None = None

    @property
    def closed(self) -> bool:
        return self._closed

    def _require_open(self):
        if self.closed or self._repo._closing:
            raise ConnectorClosedError(f"{type(self).__name__} is closed")

    def _on_closing(self):
        pass

    async def close(self) -> None:
        """Close once; cancellation of a waiter does not cancel cleanup."""
        if self._close_task is None:
            if self._closed:
                return
            self._closed = True
            self._on_closing()
            self._close_task = asyncio.create_task(self._close_impl())
        await asyncio.shield(self._close_task)

    async def __aenter__(self: _ResourceType) -> _ResourceType:
        self._require_open()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()
        return False


class Connection(_Resource):
    """One transport connection, not an outgoing reconnection policy.

    A listener returns this handle before the handshake completes. ``peer_id``
    becomes available after a successful handshake. Closing a dialer's individual
    connection permits reconnection; close the dialer to disable reconnection.
    """

    def __init__(
        self,
        repo: Repo,
        connection_id: ConnectionId,
        transport: Transport,
        owner: Dialer | Listener,
    ):
        super().__init__(repo)
        self._id = connection_id
        self._transport = transport
        self._owner = owner
        self._finished = asyncio.get_running_loop().create_future()
        self._cleanup_task: asyncio.Task | None = None
        self._reason: ConnFinishedReason | None = None
        self._peer_id: PeerId | None = None
        self.error: Exception | None = None

    @property
    def peer_id(self) -> PeerId | None:
        return self._peer_id

    async def wait_closed(self) -> ConnFinishedReason:
        """Wait for transport cleanup; cancelling this wait leaves it running."""
        return await asyncio.shield(self._finished)

    async def _close_impl(self):
        await self._repo._close_connection(self)

    def _handshake_completed(self, peer_id):
        self._peer_id = peer_id
        if isinstance(self._owner, Dialer):
            self._owner._changed.set()

    def _failed(self, error):
        if self.error is None and self._reason is None:
            self.error = RuntimeError(error)
        if isinstance(self._owner, Dialer):
            self._owner._last_error = self.error
            if self._owner._current is self:
                self._owner._current = None
            self._owner._changed.set()

    def _finish(self, reason):
        self._closed = True
        self._owner._connections.discard(self)
        if isinstance(self._owner, Dialer):
            if self._owner._current is self:
                self._owner._current = None
            self._owner._changed.set()
        if not self._finished.done():
            self._finished.set_result(reason)


class Dialer(_Resource):
    """A persistent outgoing connector with a fresh transport per attempt."""

    def __init__(
        self,
        repo: Repo,
        dialer_id: int,
        url: str,
        connect: Callable[[], Awaitable[Transport]],
        backoff: Backoff,
        expected_peer_id: PeerId | None,
    ):
        super().__init__(repo)
        self._id = dialer_id
        self.url = url
        self.backoff = backoff
        self._connect = connect
        self._expected_peer_id = expected_peer_id
        self._connections: set[Connection] = set()
        self._current: Connection | None = None
        self._changed = asyncio.Event()
        self._attempt: asyncio.Task | None = None
        self._failure: Exception | None = None
        self._last_error: Exception | None = None

    @property
    def connection(self) -> Connection | None:
        conn = self._current
        if conn is not None and conn.peer_id is not None and not conn.closed:
            return conn
        return None

    async def wait_connected(self) -> Connection:
        """Return a handshaken connection, or wait for the next one.

        Does not imply that documents have finished synchronizing. Cancelling or
        timing out this wait does not stop the dialer.
        """
        while True:
            self._changed.clear()
            self._require_open()
            if self._failure is not None:
                raise DialFailedError(
                    f"Dialer for {self.url} failed"
                ) from self._failure
            connection = self.connection
            if connection is not None:
                return connection
            await self._changed.wait()

    def _on_closing(self):
        self._changed.set()

    def _fail(self, error):
        self._failure = error
        self._changed.set()

    def _start_attempt(self):
        if self.closed or self._repo._closing or self._failure is not None:
            return
        if self._attempt is None or self._attempt.done():
            self._attempt = asyncio.create_task(self._dial())
            self._attempt.add_done_callback(self._attempt_done)

    def _attempt_done(self, task):
        if not task.cancelled():
            error = task.exception()
            if error is not None and not self.closed:
                self._fail(error)

    async def _dial(self):
        if self.closed or self._repo._closing:
            return
        transport = None
        try:
            transport = await self._connect()
            if self.closed or self._repo._closing:
                return
            command = HubEvent.create_dialer_connection(
                self._id, self._expected_peer_id
            )
            # Once submitted, Repo owns cleanup even if this attempt is cancelled.
            supplied, transport = transport, None
            await self._repo._attach_transport(command, supplied, self)
        except asyncio.CancelledError:
            if not self.closed and not self._repo._closing:
                self._repo._hub_event_queue.put_nowait(
                    HubEvent.dial_failed(self._id, "Transport factory cancelled", False)
                )
            raise
        except Exception as error:
            if not self.closed and not self._repo._closing:
                self._last_error = error
                permanent = isinstance(error, PermanentDialError)
                if permanent:
                    self._fail(error)
                await self._repo._dispatch_event(
                    HubEvent.dial_failed(self._id, str(error), permanent)
                )
        finally:
            if transport is not None:
                await transport.close()

    async def _close_impl(self):
        if self._attempt is not None and not self._attempt.done():
            self._attempt.cancel()
            await asyncio.gather(self._attempt, return_exceptions=True)
        await self._repo._remove_connector(self)


class Listener(_Resource):
    """A logical accepting endpoint and its inbound connections.

    This does not bind a socket. An external server supplies transports through
    ``accept()``; the listener owns them after acceptance and closes them together.
    """

    def __init__(self, repo: Repo, listener_id: int, url: str):
        super().__init__(repo)
        self._id = listener_id
        self.url = url
        self._connections: set[Connection] = set()

    @property
    def connections(self) -> tuple[Connection, ...]:
        return tuple(self._connections)

    async def accept(
        self, transport: Transport, *, expected_peer_id: PeerId | None = None
    ) -> Connection:
        """Attach an incoming transport; returns before handshake completion.

        The caller authenticates the transport. ``expected_peer_id`` only binds
        its Automerge handshake to that authenticated identity.
        """
        self._require_open()
        _validate_peer_id(expected_peer_id)
        command = HubEvent.create_listener_connection(self._id, expected_peer_id)
        return await self._repo._attach_transport(command, transport, self)

    async def _close_impl(self):
        await self._repo._remove_connector(self)
