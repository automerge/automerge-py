# Dialers, listeners, and connections

`automerge.repo` can connect to other repositories over arbitrary transports.
As well as handling the Automerge sync protocol, `automerge.repo` also handles
reconnection. The basic concepts you need are:

- `Dialer`: a handle to an outgoing connection, returned from
  `Repo.add_dialer()`
- `Listener`: a handle to an incoming connection, returned from
  `Repo.add_listener()`
- `Connection`: a handle to a single transport connection, returned from
  `Dialer.wait_connected()` or `Listener.accept()`
- `Transport`: an abstract interface for sending and receiving bytes,
  implemented by your transport backend. See `automerge.transports`
  for examples.
- `Backoff`: a retry policy for outgoing connections, used by `Dialer`.

`Dialer` and `Listener` both support `async with` and an asynchronous,
idempotent `close()`.


## Outgoing connections

```py
import asyncio

from automerge.repo import Backoff, Repo
from automerge.transports import WebSocketClientTransport


async def synchronize(storage):
    repo = await Repo.load(storage)
    url = "wss://sync.example.com/automerge"

    async def connect():
        return await WebSocketClientTransport.connect(url)

    async with repo:
        dialer = await repo.add_dialer(
            url,
            connect=connect,
            backoff=Backoff(initial_delay=0.1, max_delay=30.0, max_retries=10),
        )
        async with dialer:
            connection = await asyncio.wait_for(dialer.wait_connected(), timeout=10)
            print("Connected to", connection.peer_id)
            # Use the repository while it reconnects as needed.
            await asyncio.sleep(3600)
```

`add_dialer()` returns after registration, without waiting for a connection.
The factory is called without arguments on each attempt. It must return a fresh
`Transport`, implementing asynchronous `send(bytes)`, `recv() -> bytes`, and
`close()`. The URL identifies the endpoint, purely for diagnostic purposes;

### Connection Failures

If the `connect` factory raises an exception `Repo` will treat it as a
transient problem and retry with an exponential backoff policy. If you want to
signal that a failure is permanent, raise `PermanentDialError` instead. 

### Connection Events

`wait_connected()` returns the current handshaken connection or waits for the
next one. It raises `DialFailedError` on permanent failure or retry exhaustion,
and `ConnectorClosedError` on closure. 

Closing an individual outgoing `Connection` allows the dialer to reconnect.
Close the `Dialer` instead to cancel a pending factory, prevent further retries,
and close its connections.

## Incoming connections

```py
listener = await repo.add_listener("ws://localhost:8080")


async def on_transport(transport, authenticated_peer_id=None):
    connection = await listener.accept(
        transport,
        expected_peer_id=authenticated_peer_id,
    )
    try:
        await connection.wait_closed()
    finally:
        await connection.close()
```

The url passed here is again for diagnostic purposes. The thing doing the
actual work of accepting an incoming connection is whatever you are doing to
produce a `Transport` instance that you pass to `listener.accept()`.

The `listener` interface provides a way to see the accepted connections on
some interface via `listener.connections` and also a single interface to close
down the entire interface via `listener.close()`.

## Authentication

You can authenticate incoming transports however you like (e.g. checking a 
header on the upgrade request for websockets). However, the underlying sync
protocol needs to know what the authenticated peer ID is in order to ensure
that the underlying messages are from the expected peer. This in turn allows
 the announce policy to be enforced.

Use `expected_peer_id=PeerId.from_string(authenticated_identity)` on
`add_dialer()` or `listener.accept()` to bind the Automerge handshake to an
identity authenticated by your transport/application. 

## Connection lifecycle

- `connection.peer_id` is `None` until a successful handshake.
- `connection.wait_closed()` waits for transport cleanup and returns a
  `ConnFinishedReason`. Cancelling this wait does not close the connection.
- `connection.error` records transport/protocol errors, if any.
- Calling `close()` repeatedly is safe. Cancelling a close waiter does not cancel
  the underlying cleanup; a later `close()` can wait for its completion.

## Migrating from earlier development releases

Earlier development releases exposed `Repo.connect(transport)` and
`Repo.accept(transport)`. These convenience methods were removed before the
repository API's first stable release. All connections now belong to an explicit
dialer or listener. This section is for users of those development releases.

For outgoing connections, register a dialer with a factory that opens a fresh
transport instead of passing an already-open transport. `await dialer.wait_connected()`
waits for the handshake; `await connection.wait_closed()` waits for an individual
connection's lifetime. Use `Backoff(max_retries=0)` to disable retries, and
`await dialer.close()` to stop the connector and clean up its connections.

For incoming connections, register one listener for your server, then call
`await listener.accept(transport)` for each accepted transport. Wait on the returned
connection's `wait_closed()` when your server handler needs to run for its lifetime.
Close the listener when stopping the server, or use `WebSocketServer`, which owns
both its socket and listener.
