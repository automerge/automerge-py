"""Share policies and the lifecycle of per-document/peer permission streams.

Public policy types are also available from ``automerge.repo``.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
from collections.abc import AsyncIterator, Iterable
from dataclasses import dataclass
from typing import Callable, Protocol, Union

from ._automerge import (
    DocumentActorId,
    DocumentId,
    PeerId,
    ShareAccess as ShareAccess,
    ShareConfig as ShareConfig,
)


class SharePolicy(Protocol):
    """Per-document permissions granted to a remote peer.

    The repository consumes one stream per active document/peer pair.
    Yield new decisions to change permissions.
    """

    def share_config(
        self, document_id: str, peer_id: str
    ) -> AsyncIterator[ShareConfig]:
        """Return a stream of permissions for the given document and peer."""
        ...


SharePolicyFunc = Callable[[str, str], AsyncIterator[ShareConfig]]
SharePolicyType = Union[ShareConfig, SharePolicyFunc, SharePolicy, None]


@dataclass(eq=False)
class _PolicySubscription:
    actor_id: DocumentActorId
    document_id: DocumentId
    peer_id: PeerId
    current: ShareConfig | None = None
    task: asyncio.Task | None = None

    @property
    def key(self):
        return self.actor_id, str(self.peer_id)


def _validate_share_policy(share_policy: SharePolicyType) -> None:
    if share_policy is not None and not (
        isinstance(share_policy, ShareConfig)
        or callable(share_policy)
        or callable(getattr(share_policy, "share_config", None))
    ):
        raise TypeError("share_policy must be a ShareConfig, callable, or SharePolicy")


class _SharePolicyManager:
    """Own permission streams independently of repository and actor internals.

    The owner supplies document identities and active peers, and invalidates
    subscriptions when actors stop. ``apply`` must apply decisions synchronously:
    no queued update may outlive the subscription that produced it.
    """

    def __init__(
        self,
        policy: SharePolicyType,
        apply: Callable[[DocumentActorId, PeerId, ShareConfig], None],
    ):
        self._policy = policy
        self._apply = apply
        self._closed = False
        self._subscriptions: dict[tuple[DocumentActorId, str], _PolicySubscription] = {}
        self._tasks: dict[asyncio.Task, _PolicySubscription] = {}

    def stream(self, document_id: str, peer_id: str) -> AsyncIterator[ShareConfig]:
        """Normalize shorthand policies without awaiting the iterator factory."""
        policy = self._policy
        if policy is None or isinstance(policy, ShareConfig):

            async def single():
                yield ShareConfig.read_write() if policy is None else policy

            return single()
        callback = policy if callable(policy) else policy.share_config
        iterator = callback(document_id, peer_id)
        if not isinstance(iterator, AsyncIterator):
            # Reject the replaced one-shot API without leaking its coroutine.
            if inspect.iscoroutine(iterator):
                iterator.close()
            raise TypeError("share_policy must return AsyncIterator[ShareConfig]")
        return iterator

    def _is_active(self, subscription: _PolicySubscription) -> bool:
        return (
            not self._closed
            and self._subscriptions.get(subscription.key) is subscription
        )

    def _remove(self, subscription: _PolicySubscription):
        # Invalidate before cancelling: even a cancellation-resistant iterator
        # cannot publish to a replacement subscription or actor.
        if self._subscriptions.get(subscription.key) is subscription:
            del self._subscriptions[subscription.key]
            if subscription.task is not None:
                subscription.task.cancel()

    def begin_close(self, actor_id: DocumentActorId | None = None) -> None:
        """Invalidate streams synchronously; follow with close() to await cleanup.

        With no actor ID, permanently reject new registrations for this manager.
        Actor-specific closure leaves other actors and future registrations alone.
        """
        if actor_id is None:
            self._closed = True
        for subscription in list(self._subscriptions.values()):
            if actor_id is None or subscription.actor_id == actor_id:
                self._remove(subscription)

    async def close(self, actor_id: DocumentActorId | None = None):
        """Cancel streams, including awaiting cleanup already started on disconnect."""
        self.begin_close(actor_id)
        await asyncio.gather(
            *(
                task
                for task, subscription in self._tasks.items()
                if actor_id is None or subscription.actor_id == actor_id
            ),
            return_exceptions=True,
        )

    async def _consume(self, subscription: _PolicySubscription):
        def fail_closed():
            if self._is_active(subscription):
                logging.getLogger(__name__).exception(
                    "Error in share policy for document %s, peer %s",
                    subscription.document_id,
                    subscription.peer_id,
                )
                self._publish(subscription, ShareConfig.none())

        stream = None
        try:
            stream = self.stream(
                str(subscription.document_id), str(subscription.peer_id)
            )
            async for config in stream:
                if not self._is_active(subscription):
                    break
                if not isinstance(config, ShareConfig):
                    raise TypeError("share_policy must yield ShareConfig")
                self._publish(subscription, config)
                # A generator can yield without awaiting; let other actors run.
                await asyncio.sleep(0)
            if subscription.current is None:
                self._publish(subscription, ShareConfig.none())
        except (Exception, asyncio.CancelledError):
            # Lifecycle cancellation has already invalidated the subscription.
            # Otherwise revoke before potentially slow iterator cleanup.
            fail_closed()
        finally:
            close = getattr(stream, "aclose", None)
            if close is not None:
                try:
                    # Cancellation during normal exhaustion's aclose must not
                    # interrupt cleanup. The owning policy task still awaits it.
                    cleanup = asyncio.ensure_future(close())
                    try:
                        await asyncio.shield(cleanup)
                    except asyncio.CancelledError:
                        await cleanup
                        raise
                except Exception:
                    fail_closed()

    def register(
        self, actor_id: DocumentActorId, document_id: DocumentId, peer_id: PeerId
    ):
        if self._closed:
            return
        key = actor_id, str(peer_id)
        subscription = self._subscriptions.get(key)
        if subscription is None:
            subscription = _PolicySubscription(actor_id, document_id, peer_id)
            self._subscriptions[key] = subscription
            task = asyncio.create_task(self._consume(subscription))
            subscription.task = task
            self._tasks[task] = subscription
            task.add_done_callback(self._tasks.pop)
        if subscription.current is not None:
            self._publish(subscription, subscription.current)

    def _publish(self, subscription: _PolicySubscription, config: ShareConfig):
        if not self._is_active(subscription):
            return
        # Identity checking and application must not be separated by an await.
        subscription.current = config
        self._apply(subscription.actor_id, subscription.peer_id, config)

    def reconcile(self, actor_id: DocumentActorId, peers: Iterable[PeerId]) -> None:
        """Invalidate subscriptions for peers no longer active on this actor."""
        active_peers = {str(peer) for peer in peers}
        for subscription in list(self._subscriptions.values()):
            if (
                subscription.actor_id == actor_id
                and str(subscription.peer_id) not in active_peers
            ):
                self._remove(subscription)
