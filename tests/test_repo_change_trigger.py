import asyncio
from typing import List

import pytest

from automerge._automerge import Patch
from automerge.repo import InMemoryStorage, Repo
from automerge.transports import InMemoryTransport


@pytest.mark.asyncio
async def test_document_sync_basic_with_on():
    """Test basic document synchronization between two repos"""
    # Create two repos with separate storage

    r_a, r_b = (
        await Repo.load(InMemoryStorage()),
        await Repo.load(InMemoryStorage()),
    )

    async with r_a, r_b:
        # Create paired transports
        t_a, t_b = InMemoryTransport.create_pair()

        # Connect the repos - Repo A initiates (client), Repo B accepts (server)
        asyncio.create_task(r_a.connect(t_a))
        asyncio.create_task(r_b.accept(t_b))

        h_a = await r_a.create()

        # Set some initial content
        with h_a.change() as doc:
            doc["title"] = "Hello from Repo A"
            doc["count"] = 42

        # Give time for the document to sync to Repo B
        await asyncio.sleep(0.1)

        # The document should have synced from Repo A to Repo B
        h_b = await r_b.find(h_a.url)

        # set a listener on h_b to verify that changes from Repo A trigger events in Repo B
        patch_list = []

        def on_change(patches: List[Patch]):
            nonlocal patch_list
            print(f"patches rec {len(patches)}")
            for p in patches:
                print(p.action, p.path, p.value)
            patch_list += patches

        h_b.on("change", on_change)

        # give time for changes to sync and events to trigger
        await asyncio.sleep(0.1)

        assert len(patch_list) == 0

        # make some changes
        with h_a.change() as doc:
            doc["title"] = "Hello again"
            doc["count"] = 67

        # give time for changes to sync and events to trigger
        await asyncio.sleep(0.1)

        # Verify that the change has synced
        assert h_b.doc()["title"] == "Hello again"

        # verify that the change event was triggered in Repo B
        assert len(patch_list) > 0

    await r_a.stop()
    await r_b.stop()
