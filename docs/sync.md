# Low-level sync

Use `automerge.core.Document`, `SyncState`, and `Message` when managing your own
transport instead of using `Repo`. Keep a separate `SyncState` for each
(document, remote peer) pair on each side of the connection.

Call `doc.generate_sync_message(state)` to obtain a `Message`, or `None` when
there is currently nothing to send. Send `message.encode()` over your transport;
the recipient calls `doc.receive_sync_message(state, Message.decode(data))`.
After receiving a message or making a local change, check for another outgoing
message. One side returning `None` does not mean the other side has finished;
continue delivering messages and acknowledgements in both directions.

## Publish-only (read-only) sync

```python
from automerge.core import Document, SyncState

publisher = Document()
state = SyncState.new_read_only()

# Use publisher.generate_sync_message(state) and
# publisher.receive_sync_message(state, message) as usual.

# Later, allow this peer's changes and initiate another sync exchange:
state.set_read_only(False)

# Stop accepting incoming changes again:
state.set_read_only(True)
```

**Read-only means incoming changes are ignored, not that local edits are
forbidden.** The document still sends its own changes to the remote peer. This
mode is per sync state: it does not block local transactions, merges, or changes
received through another peer's state.

`state.is_peer_read_only()` reports the **remote** peer's advertised mode. It
returns `False` until a received message advertises read-only mode. The sync
protocol automatically avoids sending changes to peers known to be read-only;
you should still exchange protocol messages with them.

Keep the existing state when toggling modes. Switching back to read-write
resets the exchange so previously ignored changes can be resent. With peers
advertising reset support, the next message carries `SyncReset`. Calling
`set_read_only()` with the current mode does nothing.

## Message flags

`message.flags` is a read-only property returning a fresh list of known
`automerge.core.MessageFlag` values, or `None` for legacy messages without a
flags section. An empty list means the section exists but has no known flag
bits. Modifying the returned list does not change the message.

- `MessageFlag.ReadOnly`: the sender ignores incoming changes.
- `MessageFlag.SyncReset`: requests a fresh exchange, clearing the recipient's
  record of already-sent changes.
- `MessageFlag.SupportsSyncReset`: advertises support for reset requests; this
  is distinct from requesting a reset on this message.

V1/V2 message-format support is negotiated separately by Automerge and is not
represented by these flags. Flags are inspected here, not manually constructed;
`SyncState` controls what the protocol emits.

## Completion

`doc.has_our_changes(state)` checks whether the state's shared heads match the
document's current heads. This is a directional check, not proof of two-way
convergence. After a local edit it will be false until the peer catches up and
acknowledges the changes. Read-only peers may settle with different documents,
so having no outgoing message does not imply this check is true.

For repository-managed sharing and access policies instead, see
[repository access control](share-policy.md).
