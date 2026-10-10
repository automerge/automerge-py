"""Tests for author metadata (automerge 0.12 Author IDs)."""

from automerge.core import ROOT, Document, ScalarType


def test_set_and_get_author() -> None:
    doc = Document()
    assert doc.get_author() is None

    doc.set_author(b"alice")
    assert doc.get_author() == b"alice"

    # None clears the author
    doc.set_author(None)
    assert doc.get_author() is None


def test_author_in_constructor() -> None:
    doc = Document(author=b"alice")
    assert doc.get_author() == b"alice"
    assert doc.get_actor() is not None


def test_changing_author_regenerates_actor() -> None:
    doc = Document(author=b"alice")
    first_actor = doc.get_actor()

    # Setting the same author is a no-op (keeps the actor)
    doc.set_author(b"alice")
    assert doc.get_actor() == first_actor

    # Changing the author generates a fresh actor ID
    doc.set_author(b"bob")
    assert doc.get_actor() != first_actor

    # The initial assignment also regenerates the actor: when using authors,
    # never manually manage actor IDs
    doc = Document(actor_id=b"a")
    assert doc.get_actor() == b"a"
    doc.set_author(b"alice")
    assert doc.get_actor() != b"a"


def test_author_stored_in_changes() -> None:
    doc = Document(author=b"alice")
    with doc.transaction() as tx:
        tx.put(ROOT, "key", ScalarType.Str, "value")

    change = doc.get_last_local_change()
    assert change is not None
    assert change.author == b"alice"

    # Subsequent changes from the same actor do not repeat the assignment;
    # resolve through the document's actor-to-author mapping instead
    with doc.transaction() as tx:
        tx.put(ROOT, "key", ScalarType.Str, "value2")

    change = doc.get_last_local_change()
    assert change is not None
    assert change.author is None
    assert doc.get_author_for_actor(change.actor_id) == b"alice"


def test_changes_without_author() -> None:
    doc = Document()
    with doc.transaction() as tx:
        tx.put(ROOT, "key", ScalarType.Str, "value")

    change = doc.get_last_local_change()
    assert change is not None
    assert change.author is None
    assert doc.get_authors() == []


def test_author_survives_save_and_load() -> None:
    doc = Document(author=b"alice")
    with doc.transaction() as tx:
        tx.put(ROOT, "key", ScalarType.Str, "value")
    saved = doc.save()

    loaded = Document.load(saved)

    # The author is per-session and is not restored by a plain load...
    assert loaded.get_author() is None

    # ...but the historical assignment is preserved in the change extra bytes
    changes = loaded.get_changes([])
    assert len(changes) == 1
    assert changes[0].author == b"alice"

    # And resolves both ways through the loaded document
    assert loaded.get_authors() == [b"alice"]
    assert loaded.get_author_for_actor(changes[0].actor_id) == b"alice"
    assert loaded.get_actors_for_author(b"alice") == [changes[0].actor_id]


def test_load_with_author() -> None:
    doc = Document(author=b"alice")
    with doc.transaction() as tx:
        tx.put(ROOT, "key", ScalarType.Str, "value")

    # The author= load argument sets the author for subsequent edits
    loaded = Document.load(doc.save(), author=b"carol")
    assert loaded.get_author() == b"carol"

    with loaded.transaction() as tx:
        tx.put(ROOT, "key2", ScalarType.Str, "value2")

    change = loaded.get_last_local_change()
    assert change is not None
    assert change.author == b"carol"


def test_author_across_merge() -> None:
    a = Document(author=b"alice")
    with a.transaction() as tx:
        tx.put(ROOT, "a", ScalarType.Str, "from-a")

    # Same author, different actor (and document)
    b = Document(author=b"alice")
    with b.transaction() as tx:
        tx.put(ROOT, "b", ScalarType.Str, "from-b")

    a.merge(b)

    # Authors are deduplicated; both actors resolve to the same author
    assert a.get_authors() == [b"alice"]
    assert len(a.get_actors_for_author(b"alice")) == 2


def test_fork_carries_author() -> None:
    doc = Document(author=b"alice")
    forked = doc.fork()
    assert forked.get_author() == b"alice"


def test_high_level_document_author() -> None:
    from automerge import Document as HighLevelDocument

    doc = HighLevelDocument(author=b"alice")
    assert doc.author == b"alice"

    with doc.change() as c:
        c["key"] = "value"

    change = doc._doc.get_last_local_change()
    assert change is not None
    assert change.author == b"alice"

    doc.set_author(b"bob")
    assert doc.author == b"bob"

    doc.set_author(None)
    assert doc.author is None
