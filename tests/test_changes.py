import base64
from datetime import datetime
from typing import List, Optional, Tuple

from automerge.core import Change, ROOT, Document, ObjType, ScalarType, extract


JS_CHANGE_B64 = (
    "hW9KgwNzpmABWQAQyKbNSA9KTuOgi2xhz8t7LAEBAAAABhUVNAFCAlYFVxZwAn0F"
    "dGl0bGUFY291bnQHZW5hYmxlZAMDAX3WAhQCSGVsbG8gZnJvbSBKYXZhU2NyaXB0KgMA"
)


def test_apply_javascript_change() -> None:
    doc = Document()
    change = Change.from_bytes(base64.b64decode(JS_CHANGE_B64))

    doc.apply_changes([change])

    assert extract(doc) == {
        "count": 42,
        "enabled": True,
        "title": "Hello from JavaScript",
    }


def test_change_from_bytes_and_missing_dependencies() -> None:
    source = Document(actor_id=b"source")
    with source.transaction() as tx:
        tx.put(ROOT, "first", ScalarType.Str, "one")
    first = source.get_last_local_change()
    assert first is not None

    with source.transaction() as tx:
        tx.put(ROOT, "second", ScalarType.Str, "two")
    second = source.get_last_local_change()
    assert second is not None

    target = Document(actor_id=b"target")
    target.apply_changes([Change.from_bytes(second.raw_bytes)])
    assert target.get_missing_deps([]) == [first.hash]

    target.apply_changes([Change.from_bytes(first.raw_bytes)])
    assert target.get_missing_deps([]) == []
    assert extract(target) == {"first": "one", "second": "two"}


def test_counter_round_trip_and_increment() -> None:
    doc = Document(actor_id=b"counter")
    with doc.transaction() as tx:
        tx.put(ROOT, "value", ScalarType.Counter, 4)

    value = doc.get(ROOT, "value")
    assert value is not None
    assert value[0] == (ScalarType.Counter, 4)

    with doc.transaction() as tx:
        tx.increment(ROOT, "value", 5)

    value = doc.get(ROOT, "value")
    assert value is not None
    assert value[0] == (ScalarType.Counter, 9)
    assert extract(doc) == {"value": 9}


def test_get_changes() -> None:
    doc = Document()
    changes = doc.get_changes([])
    assert len(changes) == 0
    with doc.transaction() as tx:
        tx.put(ROOT, "hello", ScalarType.Str, "world")
    changes = doc.get_changes([])
    assert len(changes) == 1
    change = changes[0]
    assert change.actor_id == doc.get_actor()
    assert len(change) == 1
    assert change.max_op == 1
    assert change.start_op == 1
    assert change.message is None
    assert change.deps == []
    assert change.hash is not None
    assert change.seq == 1
    assert isinstance(change.timestamp, datetime)
    assert change.bytes is not None

    with doc.transaction() as tx:
        tx.put(ROOT, "foo", ScalarType.Str, "bar")
    changes = doc.get_changes([])
    assert len(changes) == 2
    assert changes[0].hash == change.hash
    assert changes[1].deps == [change.hash]
    assert changes[1].seq == 2
    second_change_hash = changes[1].hash

    changes = doc.get_changes([change.hash])
    assert len(changes) == 1
    assert changes[0].hash == second_change_hash


def test_multi_author_changes() -> None:
    docA = Document(actor_id=b"A")
    docB = Document(actor_id=b"B")

    # First A types something
    with docA.transaction() as tx:
        text = tx.put_object(ROOT, "text", ObjType.Text)
        tx.insert(text, 0, ScalarType.Str, "h")
        tx.insert(text, 1, ScalarType.Str, "i")

    # B sees what A typed
    docB.merge(docA)

    # Then B types something
    with docB.transaction() as tx:
        tx.insert(text, 2, ScalarType.Str, " ")
        tx.insert(text, 3, ScalarType.Str, "y")
        tx.insert(text, 4, ScalarType.Str, "o")

    # A sees what B typed
    docA.merge(docB)

    # Then A and B type something at the same time.
    with docA.transaction() as tx:
        tx.insert(text, 5, ScalarType.Str, " ")
        tx.insert(text, 6, ScalarType.Str, "😊")
    with docB.transaction() as tx:
        tx.insert(text, 5, ScalarType.Str, " ")
        tx.insert(text, 6, ScalarType.Str, "👋")
    # And they exchange states.
    docB.merge(docA)
    docA.merge(docB)

    assert len(docA.get_heads()) == 2

    # docA and docB are the same now, so pick one arbitrarily to read changes
    # out of for history linearization
    doc = docA

    changes = doc.get_changes([])
    assert changes[0].actor_id == b"A"
    assert changes[1].actor_id == b"B"

    last_actor: Optional[bytes] = None
    snapshots: List[Tuple[str, Optional[bytes]]] = []
    seen_heads: List[bytes] = []
    # Go through each of the changes, saving when the author changes.
    for change in changes:
        if change.actor_id != last_actor:
            # Do a save!
            snapshot = doc.text(text, seen_heads)
            snapshots.append((snapshot, last_actor))
            last_actor = change.actor_id
        # Could do some optimization here to remove things from seen_heads that
        # are ancestors of this change.
        seen_heads.append(change.hash)
    # We could keep waiting for more changes to come in from the current last
    # actor, but let's say we waited a while and we want to persist now.
    snapshots.append((doc.text(text), last_actor))
    assert snapshots == [
        ("", None),
        ("hi", b"A"),
        ("hi yo", b"B"),
        ("hi yo 😊", b"A"),
        ("hi yo 👋 😊", b"B"),
    ]
