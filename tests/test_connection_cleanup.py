"""Minimal test to verify connection cleanup works correctly."""

import pytest


@pytest.mark.asyncio
async def test_connection_cleanup_with_deny_policy():
    """Test that connection cleanup works correctly with announce policy.

    This test verifies that when connections are cancelled and closed,
    the Hub is properly notified so it stops emitting SendActions for
    those connections.
    """
    import asyncio
    from automerge.repo import Repo, InMemoryStorage
    from automerge.transports import InMemoryTransport

    # Add a deny-all policy to Repo A
    async def deny_all_policy(document_id: str, peer_id: str) -> bool:
        return False

    storage_a = InMemoryStorage()
    storage_b = InMemoryStorage()

    repo_a = await Repo.load(storage_a, announce_policy=deny_all_policy)
    repo_b = await Repo.load(storage_b)

    async with repo_a, repo_b:
        # Create a document in Repo A BEFORE connecting
        handle_a = await repo_a.create()

        def init_doc(doc):
            doc["message"] = "Test document"

        await handle_a.change(init_doc)

        # Now create and connect transports
        transport_a, transport_b = InMemoryTransport.create_pair()

        conn_task_a = asyncio.create_task(repo_a.connect(transport_a))
        conn_task_b = asyncio.create_task(repo_b.accept(transport_b))

        # Wait for connection to establish and policy to be checked
        await asyncio.sleep(1.0)

        # Cancel connection tasks
        conn_task_a.cancel()
        conn_task_b.cancel()

        try:
            await conn_task_a
        except asyncio.CancelledError:
            pass

        try:
            await conn_task_b
        except asyncio.CancelledError:
            pass

        # Close transports
        transport_a.close()
        transport_b.close()

        # Wait for cleanup
        await asyncio.sleep(0.5)

        # Now call find() - this should NOT hang because the Hub was notified of disconnection
        handle_b = await asyncio.wait_for(repo_b.find(handle_a.url), timeout=2.0)

        # With deny policy, document should not have been announced
        assert handle_b is None, (
            "Document should not have been announced with deny policy"
        )
