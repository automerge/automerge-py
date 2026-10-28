import pytest

from automerge.core import (
    ROOT,
    Document,
    Message,
    MessageFlag,
    ScalarType,
    SyncState,
    extract,
)


def put(doc: Document, key: str, value: str) -> None:
    with doc.transaction() as tx:
        tx.put(ROOT, key, ScalarType.Str, value)


def round_trip(message: Message) -> Message:
    decoded = Message.decode(message.encode())
    assert decoded.flags == message.flags
    return decoded


def sync(doc1: Document, state1: SyncState, doc2: Document, state2: SyncState) -> None:
    # Only stop when BOTH peers have nothing to send; one may still owe an ack.
    for _ in range(20):
        msg1 = doc1.generate_sync_message(state1)
        msg2 = doc2.generate_sync_message(state2)
        if msg1 is None and msg2 is None:
            return
        if msg1 is not None:
            doc2.receive_sync_message(state2, round_trip(msg1))
        if msg2 is not None:
            doc1.receive_sync_message(state1, round_trip(msg2))
    pytest.fail("sync did not settle within 20 rounds")


def test_sync() -> None:
    doc1, doc2 = Document(), Document()
    state1, state2 = SyncState(), SyncState()
    put(doc1, "hello", "world")
    assert not doc1.has_our_changes(state1)

    sync(doc1, state1, doc2, state2)

    assert extract(doc2) == extract(doc1) == {"hello": "world"}
    assert doc1.has_our_changes(state1)
    assert doc2.has_our_changes(state2)
    assert not state1.is_peer_read_only()
    assert not state2.is_peer_read_only()

    put(doc1, "new", "change")
    assert not doc1.has_our_changes(state1)
    sync(doc1, state1, doc2, state2)
    assert doc1.has_our_changes(state1)
    assert doc2.has_our_changes(state2)
    assert doc1.get_heads() == doc2.get_heads()


def test_read_only_publishes_local_changes() -> None:
    publisher, peer = Document(), Document()
    publish_state, peer_state = SyncState.new_read_only(), SyncState()
    put(publisher, "published", "hello")
    put(peer, "private", "world")
    # This reports the REMOTE mode, not the local mode, and is initially unknown.
    assert not publish_state.is_peer_read_only()
    assert not peer_state.is_peer_read_only()

    sync(publisher, publish_state, peer, peer_state)

    assert extract(publisher) == {"published": "hello"}
    assert extract(peer) == {"published": "hello", "private": "world"}
    assert not publish_state.is_peer_read_only()
    assert peer_state.is_peer_read_only()
    # No more messages does not mean all changes were accepted by both peers.
    assert not peer.has_our_changes(peer_state)

    put(publisher, "another", "local edit")
    sync(publisher, publish_state, peer, peer_state)
    assert extract(peer)["another"] == "local edit"
    assert "private" not in extract(publisher)


def test_toggle_read_only_and_recover_ignored_changes() -> None:
    publisher, peer = Document(), Document()
    publish_state, peer_state = SyncState(), SyncState()
    put(publisher, "published", "initial")
    sync(publisher, publish_state, peer, peer_state)

    # Prepare a change message before the peer knows we are switching modes.
    put(peer, "in_flight", "ignored")
    in_flight = peer.generate_sync_message(peer_state)
    assert in_flight is not None
    publish_state.set_read_only(True)
    publisher.receive_sync_message(publish_state, round_trip(in_flight))
    assert "in_flight" not in extract(publisher)

    announcement = publisher.generate_sync_message(publish_state)
    assert announcement is not None
    assert announcement.flags is not None
    assert MessageFlag.ReadOnly in announcement.flags
    assert MessageFlag.SyncReset not in announcement.flags
    peer.receive_sync_message(peer_state, round_trip(announcement))
    assert peer_state.is_peer_read_only()
    sync(publisher, publish_state, peer, peer_state)

    # More changes accumulate while the peer knows not to send them.
    for i in range(2):
        put(peer, f"skipped{i}", "waiting")
        put(publisher, f"local{i}", "published")
        sync(publisher, publish_state, peer, peer_state)
        assert f"skipped{i}" not in extract(publisher)
        assert extract(peer)[f"local{i}"] == "published"

    publish_state.set_read_only(False)
    reset = publisher.generate_sync_message(publish_state)
    assert reset is not None
    assert reset.flags is not None
    assert MessageFlag.SyncReset in reset.flags
    assert MessageFlag.SupportsSyncReset in reset.flags
    assert MessageFlag.ReadOnly not in reset.flags
    peer.receive_sync_message(peer_state, round_trip(reset))
    assert not peer_state.is_peer_read_only()
    sync(publisher, publish_state, peer, peer_state)

    assert extract(publisher) == extract(peer)
    assert extract(publisher)["in_flight"] == "ignored"
    assert extract(publisher)["skipped1"] == "waiting"
    assert publisher.get_heads() == peer.get_heads()
    assert publisher.has_our_changes(publish_state)
    assert peer.has_our_changes(peer_state)

    # Setting the same mode again must not start another exchange.
    publish_state.set_read_only(False)
    assert publisher.generate_sync_message(publish_state) is None


def test_both_peers_read_only_then_writable() -> None:
    doc1, doc2 = Document(), Document()
    state1, state2 = SyncState.new_read_only(), SyncState.new_read_only()
    put(doc1, "one", "first")
    put(doc2, "two", "second")
    sync(doc1, state1, doc2, state2)
    assert extract(doc1) == {"one": "first"}
    assert extract(doc2) == {"two": "second"}
    assert state1.is_peer_read_only()
    assert state2.is_peer_read_only()
    assert not doc1.has_our_changes(state1)
    assert not doc2.has_our_changes(state2)

    state1.set_read_only(True)
    assert doc1.generate_sync_message(state1) is None
    state1.set_read_only(False)
    state2.set_read_only(False)
    sync(doc1, state1, doc2, state2)
    assert extract(doc1) == extract(doc2) == {"one": "first", "two": "second"}
    assert not state1.is_peer_read_only()
    assert not state2.is_peer_read_only()


@pytest.mark.parametrize(
    ("suffix", "expected"),
    [
        (b"", None),  # Legacy message with no flags section.
        (b"\x00", []),  # Present but empty flags section.
        (b"\x01\x02", []),  # Legacy V2 capability, not a bitfield flag.
        (b"\x02\x02\x81", [MessageFlag.SyncReset]),
        (b"\x02\x02\x82", [MessageFlag.ReadOnly]),
        (b"\x02\x02\x84", [MessageFlag.SupportsSyncReset]),
        (
            b"\x02\x02\x87",
            [
                MessageFlag.SyncReset,
                MessageFlag.ReadOnly,
                MessageFlag.SupportsSyncReset,
            ],
        ),
    ],
)
def test_message_flags(suffix: bytes, expected: list[MessageFlag] | None) -> None:
    # V1 header followed by empty heads, need, have and changes lists.
    message = Message.decode(b"\x42\x00\x00\x00\x00" + suffix)
    assert message.flags == expected
    assert round_trip(message).flags == expected
    with pytest.raises(AttributeError):
        message.flags = []
    flags = message.flags
    if flags is not None:
        flags.append(MessageFlag.ReadOnly)
        assert message.flags == expected  # Accessor returns a detached snapshot.


def test_has_our_changes_during_transaction() -> None:
    doc, state = Document(), SyncState()
    with doc.transaction():
        with pytest.raises(Exception, match="transaction is active"):
            doc.has_our_changes(state)
