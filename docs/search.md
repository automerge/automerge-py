# Document searches

There are two ways to obtain a document handle from a repository. `find()` is
a simple one-shot operation that returns a handle if one could be found or 
throws if it could not. Often what you want is to keep wathing for a document
until it is available - for example you might be waiting for the network to
come up. For this pupose you can use `Repo.search_for_doc(url)` which returns a
`Search` object that you can use to watch for the document to become available.

For example:


```py
from automerge.repo import DocSearchPhase


async def wait_for_document(repo, url):
    async with await repo.search_for_doc(url) as search:
        print("Initial phase:", search.state.phase)
        async for state in search:
            if state.phase == DocSearchPhase.Ready:
                return await search.wait_result()
            print("Waiting for:", state.pending_connections)
```

This example deliberately keeps watching even when a document is currently
unavailable. A later peer connection or synchronization can make it ready.
The caller owns the repository's running context and can use
`asyncio.wait_for()` to impose a timeout.

## Search registration and state

```py
search = await repo.search_for_doc(url)
```

`search.url` identifies the document. `search.state` is the most recent immutable
`DocSearch` snapshot, with these fields:

- `phase`: `DocSearchPhase.Loading`, `Searching`, or `Ready`.
- `peers`: a copy of the per-connection request-state dictionary, populated during
  `Searching`. Values are `PeerRequestState.Requested`, `Unavailable`, `Syncing`,
  or `Available`.
- `pending_connections`: a copy of the list of connector URL strings that may
  still supply the document.
- `is_currently_unavailable()`: true when local loading has finished, no potential
  connections are pending, and all consulted peers report unavailability.
  This can be true when there are no peers; it is **not** a permanent failure.

Older snapshots do not change when new updates arrive. Changing the returned
`peers` dictionary or `pending_connections` list does not modify a snapshot or
samod's state.

## Waiting and updates

`await search.wait_result()` returns a `DocHandle` when ready, or `None` when
currently unavailable. 

`search.updates()` is an async iterator yielding the current snapshot followed by
future changed snapshots. `async for state in search` is shorthand. Each iterator
has its own queue: consumers do not steal updates from each other. Registration
and same-event updates are handled together, so a new subscription cannot miss
its first state transition.

## Closing and shutdown

`await search.close()` unsubscribes the Python handle. It is idempotent and
cancellation-safe, and `Search` supports asynchronous context managers. Closing:

- Wakes `wait_result()` callers with `SearchClosedError`.
- Ends existing update iterators normally.
- Removes the handle from the repository's subscription registry.
- Leaves the last snapshot inspectable.

The repository closes all its search handles on shutdown. Closing a search does
**not** cancel samod's underlying document search or unload its document actor;
upstream exposes no search-cancellation command. Other subscriptions to the same
document continue to receive updates.
