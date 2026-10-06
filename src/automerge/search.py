"""Python subscriptions over samod's native document-search snapshots."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING

from ._automerge import AutomergeUrl, DocSearch, DocSearchPhase
from .connectors import _Resource

if TYPE_CHECKING:
    from .repo import DocHandle, DocumentActorId, DocumentId, Repo


class SearchClosedError(RuntimeError):
    """A search subscription or its repository has been closed."""


class Search(_Resource):
    """A live subscription to one document's search state.

    A search subscription is created by ``Repo.find()``. Once created you
    examine the current state which is an instance of ``DocSearch`` and you
    can listen for state updates by iterating over ``search.updates()``. You
    can also wait for the search to become ready with ``search.wait_result()``.
    """

    def __init__(
        self,
        repo: Repo,
        document_id: DocumentId,
        actor_id: DocumentActorId,
        state: DocSearch,
    ):
        super().__init__(repo)
        self._document_id = document_id
        self._actor_id = actor_id
        self._state = state
        self._changed = asyncio.Event()
        self._subscribers: set[asyncio.Queue[DocSearch | None]] = set()

    @property
    def url(self) -> AutomergeUrl:
        return AutomergeUrl.from_document_id(self._document_id)

    @property
    def state(self) -> DocSearch:
        """The most recent state of the search."""
        return self._state

    def _require_open(self):
        if self.closed or self._repo._closing:
            raise SearchClosedError("Search is closed")

    def _update(self, state: DocSearch):
        if self.closed or state == self._state:
            return
        self._state = state
        self._changed.set()
        for queue in self._subscribers:
            queue.put_nowait(state)

    async def wait_result(self) -> DocHandle | None:
        """Wait for readiness or current unavailability, like ``Repo.find()``.

        None is not permanent failure: this subscription can subsequently become
        ready. Cancelling a wait leaves the subscription and other waiters alive.
        """
        from .repo import DocHandle

        while True:
            self._changed.clear()
            self._require_open()
            if self.state.phase == DocSearchPhase.Ready:
                return DocHandle(self._actor_id, self._document_id, self._repo)
            if self.state.is_currently_unavailable():
                return None
            await self._changed.wait()

    def __aiter__(self) -> AsyncIterator[DocSearch]:
        return self.updates()

    async def updates(self) -> AsyncIterator[DocSearch]:
        """Yield the current snapshot and future updates."""
        self._require_open()
        queue: asyncio.Queue[DocSearch | None] = asyncio.Queue()
        self._subscribers.add(queue)
        queue.put_nowait(self.state)
        try:
            while True:
                state = await queue.get()
                if state is None or self.closed or self._repo._closing:
                    return
                yield state
        finally:
            self._subscribers.discard(queue)

    def _on_closing(self):
        self._changed.set()
        for queue in self._subscribers:
            queue.put_nowait(None)
        self._subscribers.clear()

    async def _close_impl(self):
        searches = self._repo._searches.get(self._document_id)
        if searches is not None:
            searches.discard(self)
            if not searches:
                self._repo._searches.pop(self._document_id, None)
