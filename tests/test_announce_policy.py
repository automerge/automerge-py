"""Tests for announce policy functionality."""

from __future__ import annotations

import pytest

from automerge.repo import InMemoryStorage, Repo


class TestAnnouncePolicyLogic:
    """Unit tests for announce policy logic."""

    @pytest.mark.asyncio
    async def test_default_policy_allows_all(self):
        """Test that None policy (default) allows all documents."""
        storage = InMemoryStorage()
        repo = await Repo.load(storage)

        # Default policy should allow everything
        result = await repo._check_announce_policy("doc-123", "peer-456")
        assert result is True

        result = await repo._check_announce_policy("any-doc", "any-peer")
        assert result is True

    @pytest.mark.asyncio
    async def test_async_function_policy_allow(self):
        """Test async function policy that allows documents."""

        async def allow_all_policy(document_id: str, peer_id: str) -> bool:
            return True

        storage = InMemoryStorage()
        repo = await Repo.load(storage, announce_policy=allow_all_policy)

        result = await repo._check_announce_policy("doc-123", "peer-456")
        assert result is True

    @pytest.mark.asyncio
    async def test_async_function_policy_deny(self):
        """Test async function policy that denies documents."""

        async def deny_all_policy(document_id: str, peer_id: str) -> bool:
            return False

        storage = InMemoryStorage()
        repo = await Repo.load(storage, announce_policy=deny_all_policy)

        result = await repo._check_announce_policy("doc-123", "peer-456")
        assert result is False

    @pytest.mark.asyncio
    async def test_async_function_policy_selective(self):
        """Test async function policy with conditional logic."""

        async def selective_policy(document_id: str, peer_id: str) -> bool:
            # Only allow documents starting with "public-"
            return document_id.startswith("public-")

        storage = InMemoryStorage()
        repo = await Repo.load(storage, announce_policy=selective_policy)

        # Should allow public documents
        result = await repo._check_announce_policy("public-doc-1", "peer-1")
        assert result is True

        # Should deny private documents
        result = await repo._check_announce_policy("private-doc-1", "peer-1")
        assert result is False

    @pytest.mark.asyncio
    async def test_class_based_policy(self):
        """Test class-based policy implementing AnnouncePolicy protocol."""

        class TrustedPeerPolicy:
            def __init__(self, trusted_peers: set[str]):
                self.trusted_peers = trusted_peers

            async def should_announce(self, document_id: str, peer_id: str) -> bool:
                return peer_id in self.trusted_peers

        policy = TrustedPeerPolicy({"peer-alice", "peer-bob"})
        storage = InMemoryStorage()
        repo = await Repo.load(storage, announce_policy=policy)

        # Should allow trusted peers
        result = await repo._check_announce_policy("doc-1", "peer-alice")
        assert result is True

        result = await repo._check_announce_policy("doc-1", "peer-bob")
        assert result is True

        # Should deny untrusted peers
        result = await repo._check_announce_policy("doc-1", "peer-charlie")
        assert result is False

    @pytest.mark.asyncio
    async def test_policy_error_handling_denies(self):
        """Test that policy errors result in denial (fail closed)."""

        async def broken_policy(document_id: str, peer_id: str) -> bool:
            raise RuntimeError("Policy broke!")

        storage = InMemoryStorage()
        repo = await Repo.load(storage, announce_policy=broken_policy)

        # Should deny on error (fail closed for security)
        result = await repo._check_announce_policy("doc-1", "peer-1")
        assert result is False

    @pytest.mark.asyncio
    async def test_class_policy_error_handling_denies(self):
        """Test that class-based policy errors result in denial."""

        class BrokenPolicy:
            async def should_announce(self, document_id: str, peer_id: str) -> bool:
                raise ValueError("Something went wrong")

        storage = InMemoryStorage()
        repo = await Repo.load(storage, announce_policy=BrokenPolicy())

        # Should deny on error
        result = await repo._check_announce_policy("doc-1", "peer-1")
        assert result is False

    @pytest.mark.asyncio
    async def test_policy_with_state(self):
        """Test that class-based policies can maintain state."""

        class CountingPolicy:
            def __init__(self):
                self.check_count = 0

            async def should_announce(self, document_id: str, peer_id: str) -> bool:
                self.check_count += 1
                return True

        policy = CountingPolicy()
        storage = InMemoryStorage()
        repo = await Repo.load(storage, announce_policy=policy)

        # Make several checks
        await repo._check_announce_policy("doc-1", "peer-1")
        await repo._check_announce_policy("doc-2", "peer-1")
        await repo._check_announce_policy("doc-3", "peer-2")

        # Verify state was maintained
        assert policy.check_count == 3

    @pytest.mark.asyncio
    async def test_policy_receives_correct_parameters(self):
        """Test that policy receives the correct document_id and peer_id."""
        received_params = []

        async def capture_policy(document_id: str, peer_id: str) -> bool:
            received_params.append((document_id, peer_id))
            return True

        storage = InMemoryStorage()
        repo = await Repo.load(storage, announce_policy=capture_policy)

        await repo._check_announce_policy("test-doc", "test-peer")

        assert len(received_params) == 1
        assert received_params[0] == ("test-doc", "test-peer")


class TestAnnouncePolicyIntegration:
    """Integration tests for announce policy with document sync.

    Note: These tests verify that the announce policy is consulted during
    the document announcement flow. Full end-to-end sync testing is complex
    due to timing and connection lifecycle issues.
    """

    @pytest.mark.asyncio
    async def test_policy_consulted_on_connection(self):
        """Test that announce policy is consulted when peers connect."""
        import asyncio

        from automerge.transports import InMemoryTransport

        # Track policy calls
        policy_calls = []

        async def tracking_policy(document_id: str, peer_id: str) -> bool:
            policy_calls.append((document_id, peer_id))
            return True

        # Create document BEFORE establishing connection
        storage_a = InMemoryStorage()
        repo_a = await Repo.load(storage_a, announce_policy=tracking_policy)

        async with repo_a:
            # Create a document
            handle_a = await repo_a.create()

            def init_doc(doc):
                doc["message"] = "Test"

            await handle_a.change(init_doc)

            # Now connect to another repo
            storage_b = InMemoryStorage()
            repo_b = await Repo.load(storage_b)

            async with repo_b:
                transport_a, transport_b = InMemoryTransport.create_pair()

                conn_task_a = asyncio.create_task(repo_a.connect(transport_a))
                conn_task_b = asyncio.create_task(repo_b.accept(transport_b))

                # Wait for connection and announce
                await asyncio.sleep(1.0)

                # Verify policy was called
                assert len(policy_calls) >= 1, (
                    "Policy should have been called when peer connected"
                )

                # Verify it was called for our document
                doc_ids = [call[0] for call in policy_calls]
                assert str(handle_a.document_id) in doc_ids, (
                    "Policy should have been called for our document"
                )

                # Clean up
                transport_a.close()
                transport_b.close()
                await asyncio.sleep(0.1)
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

    @pytest.mark.asyncio
    async def test_policy_called_with_deny_result(self):
        """Test that deny policy prevents document from being announced and synced."""
        import asyncio

        from automerge.transports import InMemoryTransport

        # Track policy calls
        policy_calls = []

        async def deny_all_policy(document_id: str, peer_id: str) -> bool:
            policy_calls.append((document_id, peer_id, False))
            return False

        # Create document in Repo A (with deny policy) BEFORE connection
        storage_a = InMemoryStorage()
        storage_b = InMemoryStorage()

        repo_a = await Repo.load(storage_a, announce_policy=deny_all_policy)
        repo_b = await Repo.load(storage_b)

        async with repo_a, repo_b:
            # Create document before connecting
            handle_a = await repo_a.create()

            def init_doc(doc):
                doc["message"] = "Test document"

            await handle_a.change(init_doc)

            # Connect the repos
            transport_a, transport_b = InMemoryTransport.create_pair()
            conn_task_a = asyncio.create_task(repo_a.connect(transport_a))
            conn_task_b = asyncio.create_task(repo_b.accept(transport_b))

            # Wait for connection and policy check
            await asyncio.sleep(1.0)

            # Verify policy was called and returned False
            assert len(policy_calls) >= 1, "Deny policy should have been called"
            doc_ids = [call[0] for call in policy_calls]
            assert str(handle_a.document_id) in doc_ids, (
                "Policy should have been called for our document"
            )

            # Verify all calls returned False
            results = [call[2] for call in policy_calls]
            assert all(r is False for r in results), (
                "All policy calls should have returned False"
            )

            # Disconnect the peers
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

            transport_a.close()
            transport_b.close()

            # Wait for cleanup
            await asyncio.sleep(0.5)

            # Try to find the document - should NOT be available because it was not announced
            handle_b = await asyncio.wait_for(repo_b.find(handle_a.url), timeout=2.0)
            assert handle_b is None, (
                "Document should not have been announced with deny policy"
            )

    @pytest.mark.asyncio
    async def test_policy_called_with_allow_result(self):
        """Test that allow policy allows document to be announced and synced."""
        import asyncio

        from automerge.transports import InMemoryTransport

        # Track policy calls
        policy_calls = []

        async def allow_all_policy(document_id: str, peer_id: str) -> bool:
            policy_calls.append((document_id, peer_id, True))
            return True

        # Create document in Repo A (with allow policy) BEFORE connection
        storage_a = InMemoryStorage()
        storage_b = InMemoryStorage()

        repo_a = await Repo.load(storage_a, announce_policy=allow_all_policy)
        repo_b = await Repo.load(storage_b)

        async with repo_a, repo_b:
            # Create document before connecting
            handle_a = await repo_a.create()

            def init_doc(doc):
                doc["message"] = "Test document"

            await handle_a.change(init_doc)

            # Connect the repos
            transport_a, transport_b = InMemoryTransport.create_pair()
            conn_task_a = asyncio.create_task(repo_a.connect(transport_a))
            conn_task_b = asyncio.create_task(repo_b.accept(transport_b))

            # Wait for connection and policy check
            await asyncio.sleep(1.0)

            # Verify policy was called and returned True
            assert len(policy_calls) >= 1, "Allow policy should have been called"
            doc_ids = [call[0] for call in policy_calls]
            assert str(handle_a.document_id) in doc_ids, (
                "Policy should have been called for our document"
            )

            # Verify all calls returned True
            results = [call[2] for call in policy_calls]
            assert all(r is True for r in results), (
                "All policy calls should have returned True"
            )

            # Disconnect the peers
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

            transport_a.close()
            transport_b.close()

            # Wait for cleanup
            await asyncio.sleep(0.5)

            # Try to find the document - should be available because it was announced
            handle_b = await asyncio.wait_for(repo_b.find(handle_a.url), timeout=2.0)
            assert handle_b is not None, (
                "Document should have been announced with allow policy"
            )

            # Verify the content was synced
            doc_b = handle_b.doc()
            message = doc_b.get("message")
            assert message == "Test document", (
                "Document content should have been synced"
            )
