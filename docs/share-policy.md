# Document sharing and access control

When opening a `Repo` with `Repo.load()` you can specify a `share_policy=`
constructor argument which controls the access granted to each peer for some
document. The `share_policy` returns a stream of `ShareConfig` decisions, which
allows you to dynamically change permissions while the repository is running.

The share policy controls two things:

* Whether a remote peer has read-only, read-write, or no access to a document.
* Whether to announce a document to the peer in question

The default share policy if you don't specify anything allows read-write access
and announces all documents to all peers. 

## Example

```py
from collections.abc import AsyncIterator
from automerge.repo import Repo, ShareConfig


# An example of a share policy that makes decisions based on the string peer ID
# in a real application this would probably look something up in a database
async def permissions(document_id: str, peer_id: str) -> AsyncIterator[ShareConfig]:
    if peer_id == "editor":
        yield ShareConfig.read_write()
    elif peer_id == "viewer":
        yield ShareConfig.read_only()
    else:
        yield ShareConfig.none()


repo = await Repo.load(share_policy=permissions)
```

For a `share_policy` to be meaningful the peer ID on incoming connections must
be authenticated, see [authentication](connectors.md#authentication).

## Decisions

The `share_policy` returns a stream of `ShareConfig` decisions, which look like
this:

```py
class ShareConfig:
    # Constructors
    def __init__(
        self, access: ShareAccess = ShareAccess.ReadWrite, announce: bool = True
    ) -> None: ...
    @staticmethod
    def none() -> ShareConfig: ...
    @staticmethod
    def read_only() -> ShareConfig: ...
    @staticmethod
    def read_write() -> ShareConfig: ...

    # Add an announce policy
    def with_announce(self, announce: bool) -> ShareConfig: ...

    # Properties
    @property
    def access(self) -> ShareAccess: ...
    @property
    def announce(self) -> bool: ...


class ShareAccess(Enum):
    None_: ShareAccess
    ReadOnly: ShareAccess
    ReadWrite: ShareAccess
```

As you can see, a `ShareConfig` has two properties: `access` and `announce`.
These properties are used by the `Repo` to determine whether to allow access
(read, or read-write) and whether to announce the document.


| Decision                   | Outgoing document changes | Incoming document changes | Ephemeral messages |
| ---                        | ---                       | ---                       | ---                |
| `ShareConfig.none()`       | Blocked                   | Blocked                   | Blocked both ways  |
| `ShareConfig.read_only()`  | Allowed                   | Ignored                   | Allowed both ways  |
| `ShareConfig.read_write()` | Allowed                   | Allowed                   | Allowed both ways  |


## Policy forms and errors

The `share_policy=` `Repo` constructor argument accepts:

- A constant `ShareConfig`, e.g. `Repo.load(share_policy=ShareConfig.read_only())`.
- A callable `(document_id: str, peer_id: str) -> AsyncIterator[ShareConfig]`,
  typically an async generator.
- An object implementing `share_config(document_id, peer_id)` with the same
  async-iterator return contract.

The repository calls the policy without awaiting the call, then consumes its
iterator. A protocol declaration uses `def share_config(...) ->
AsyncIterator[ShareConfig]`; implementations can use `async def` with `yield`.

`Repo` will wait for the first decision from an async iterator before allowing
any document traffic from the peer in question. If the iterator throws an 
exception then `Repo` will default to `ShareConfig.none()` and log the error. 

## Changing permissions while connected

In collaborative applications it's reasonably common to change permissions
whilst a peer is conneted. This is why the `share_policy=` returns an async
iterator of `ShareConfig` decisions. By yielding a new value you can change the
access `Repo` grants to a peer for a document.

E.g. Yield a new configuration whenever the application's ACL changes:

```py
async def permissions(document_id: str, peer_id: str) -> AsyncIterator[ShareConfig]:
    # Assume we have some `permission_service` that can watch ACLs for a
    # document/peer pair
    async for acl in permission_service.watch(document_id, peer_id):
        if acl.can_write:
            yield ShareConfig.read_write()
        elif acl.can_read:
            yield ShareConfig.read_only()
        else:
            yield ShareConfig.none()
```
