import pytest
from automerge.core import Document, ROOT, ObjType, ScalarType, extract


def test_basic() -> None:
    doc = Document()

    with doc.transaction() as tx:
        hello = tx.put_object(ROOT, "hello", ObjType.Map)
        tx.put(hello, "test", ScalarType.Str, "world")
        text = tx.put_object(ROOT, "text", ObjType.Text)
        tx.insert(text, 0, ScalarType.Str, "h")
        tx.insert(text, 1, ScalarType.Str, "i")

    assert extract(doc) == {"hello": {"test": "world"}, "text": "hi"}


def test_rollback() -> None:
    doc = Document()

    with pytest.raises(Exception) as e_info:
        with doc.transaction() as tx:
            tx.put_object(ROOT, "hello", ObjType.Map)
            raise Exception("hi")
    assert e_info.value.args[0] == "hi"  # type: ignore[misc]
    assert extract(doc) == {}


def test_actor_id() -> None:
    doc = Document(actor_id=b"foo")
    assert doc.get_actor() == b"foo"
    doc.set_actor(b"bar")
    assert doc.get_actor() == b"bar"


def test_keys() -> None:
    doc = Document()

    with doc.transaction() as tx:
        map_id = tx.put_object(ROOT, "map", ObjType.Map)
        tx.put(map_id, "foo", ScalarType.Boolean, True)
        tx.put(map_id, "hello", ScalarType.Str, "world")
        list_id = tx.put_object(ROOT, "list", ObjType.List)
        tx.insert(list_id, 0, ScalarType.Str, "one")
        tx.insert(list_id, 1, ScalarType.Boolean, True)

    assert doc.keys(ROOT) == ["list", "map"]
    assert doc.keys(map_id) == ["foo", "hello"]
    list_keys = doc.keys(list_id)
    assert len(list_keys) == 2


def test_values() -> None:
    doc = Document()

    with doc.transaction() as tx:
        map_id = tx.put_object(ROOT, "map", ObjType.Map)
        tx.put(map_id, "foo", ScalarType.Boolean, True)
        tx.put(map_id, "hello", ScalarType.Str, "world")
        list_id = tx.put_object(ROOT, "list", ObjType.List)
        tx.insert(list_id, 0, ScalarType.Str, "one")
        tx.insert(list_id, 1, ScalarType.Boolean, True)

    values = doc.values(map_id)
    assert [v[0] for v in values] == [
        (ScalarType.Boolean, True),
        (ScalarType.Str, "world"),
    ]
    values = doc.values(list_id)
    assert [v[0] for v in values] == [
        (ScalarType.Str, "one"),
        (ScalarType.Boolean, True),
    ]


def test_diff() -> None:
    doc = Document()

    with doc.transaction() as tx:
        map_id = tx.put_object(ROOT, "map", ObjType.Map)
        tx.put(map_id, "hello", ScalarType.Str, "world")
        list_id = tx.put_object(ROOT, "list", ObjType.List)
        tx.insert(list_id, 0, ScalarType.Boolean, True)

    patch = doc.diff([], doc.get_heads())
    assert len(patch) == 4


def test_text_heads() -> None:
    doc1 = Document(actor_id=b"A")

    with doc1.transaction() as tx:
        text_id = tx.put_object(ROOT, "text", ObjType.Text)
        tx.insert(text_id, 0, ScalarType.Str, "h")

    doc2 = doc1.fork()
    doc2.set_actor(b"B")

    with doc1.transaction() as tx:
        tx.insert(text_id, 1, ScalarType.Str, "i")

    with doc2.transaction() as tx:
        tx.insert(text_id, 1, ScalarType.Str, "o")

    a_change = doc1.get_last_local_change()
    assert a_change is not None
    b_change = doc2.get_last_local_change()
    assert b_change is not None

    doc1.merge(doc2)
    heads = doc1.get_heads()
    assert len(heads) == 2

    assert doc1.text(text_id, [a_change.hash]) == "hi"
    assert doc1.text(text_id, [b_change.hash]) == "ho"
    assert doc1.text(text_id, [a_change.hash, b_change.hash]) == "hoi"
