import pytest

from automerge.core import ROOT, Document, ObjType, ScalarType


@pytest.mark.parametrize("container", [ObjType.Map, ObjType.List])
def test_get_all_conflicts_and_history(container) -> None:
    doc = Document(actor_id=b"A")
    with doc.transaction() as tx:
        obj = tx.put_object(ROOT, "container", container)
        if container == ObjType.Map:
            prop = "value"
            tx.put(obj, prop, ScalarType.Str, "original")
        else:
            prop = 0
            tx.insert(obj, prop, ScalarType.Str, "original")
    original = doc.get(obj, prop)
    original_heads = doc.get_heads()
    other = doc.fork()
    other.set_actor(b"B")

    with doc.transaction() as tx:
        tx.put(obj, prop, ScalarType.Str, "left")
    left = doc.get(obj, prop)
    assert left is not None
    left_heads = doc.get_heads()
    with other.transaction() as tx:
        tx.put(obj, prop, ScalarType.Str, "right")
    right = other.get(obj, prop)
    assert right is not None
    right_heads = other.get_heads()

    doc.merge(other)
    conflicts = doc.get_all(obj, prop)
    conflict_heads = doc.get_heads()
    assert len(conflicts) == 2
    assert left in conflicts
    assert right in conflicts
    assert left[1] != right[1]
    assert all(isinstance(id, bytes) for _, id in conflicts)
    assert doc.get(obj, prop) in conflicts
    assert doc.get_all(obj, prop, heads=None) == conflicts
    assert doc.get_all(obj, prop, heads=conflict_heads) == conflicts
    assert doc.get_all(obj, prop, original_heads) == [original]
    assert doc.get_all(obj, prop, left_heads) == [left]
    assert doc.get_all(obj, prop, right_heads) == [right]

    # A write that has seen both branches resolves the conflict. Reads through
    # both the transaction and its document see pending writes, just like get().
    with doc.transaction() as tx:
        assert tx.get_all(obj, prop) == conflicts
        assert tx.get_all(obj, prop, heads=left_heads) == [left]
        tx.put(obj, prop, ScalarType.Str, "resolved")
        resolved = tx.get(obj, prop)
        assert tx.get_all(obj, prop) == [resolved]
        assert doc.get_all(obj, prop) == [resolved]
        assert tx.get_all(obj, prop, heads=conflict_heads) == conflicts
        assert doc.get_all(obj, prop, heads=conflict_heads) == conflicts

    assert doc.get_all(obj, prop) == [resolved]
    assert doc.get_all(obj, prop, heads=conflict_heads) == conflicts
    with doc.transaction() as tx:
        tx.delete(obj, prop)
        assert tx.get_all(obj, prop) == []
    assert doc.get_all(obj, prop) == []
    assert doc.get_all(obj, prop, heads=conflict_heads) == conflicts


def test_get_all_missing_and_single_values() -> None:
    doc = Document()
    assert doc.get_all(ROOT, "missing") == []
    with doc.transaction() as tx:
        assert tx.get_all(ROOT, "missing") == []
        tx.put(ROOT, "null", ScalarType.Null, None)
        tx.put(ROOT, "flag", ScalarType.Boolean, True)
        obj = tx.put_object(ROOT, "list", ObjType.List)
        for reader in (doc, tx):
            assert reader.get_all(ROOT, "null") == [reader.get(ROOT, "null")]
            assert reader.get_all(ROOT, "null")[0][0] == (ScalarType.Null, None)
            assert reader.get_all(ROOT, "flag")[0][0] == (ScalarType.Boolean, True)
            assert reader.get_all(ROOT, "list") == [(ObjType.List, obj)]
            assert reader.get_all(obj, 0) == []
            assert reader.get_all(ROOT, "null", heads=[]) == []
    assert doc.get_all(ROOT, "null") == [doc.get(ROOT, "null")]
    assert doc.get_all(ROOT, "null", heads=[]) == []
    assert doc.get_all(obj, 99) == []


def test_get_all_conflicting_objects() -> None:
    doc = Document(actor_id=b"A")
    other = doc.fork()
    other.set_actor(b"B")
    with doc.transaction() as tx:
        left = tx.put_object(ROOT, "value", ObjType.Map)
        tx.put(left, "nested", ScalarType.Str, "left")
    with other.transaction() as tx:
        right = tx.put_object(ROOT, "value", ObjType.List)
        tx.insert(right, 0, ScalarType.Str, "right")
    right_heads = other.get_heads()
    doc.merge(other)
    # Encoded IDs include a document-local actor-index hint. Compare IDs from
    # the merged document rather than assuming byte equality across documents.
    right_value = doc.get(ROOT, "value", heads=right_heads)
    assert right_value is not None
    right = right_value[1]

    conflicts = doc.get_all(ROOT, "value")
    assert len(conflicts) == 2
    assert (ObjType.Map, left) in conflicts
    assert (ObjType.List, right) in conflicts
    assert left != right
    # Object IDs from get_all can be used to read either conflicting subtree.
    for kind, obj in conflicts:
        prop = "nested" if kind == ObjType.Map else 0
        assert doc.get_all(obj, prop) == [doc.get(obj, prop)]
        assert doc.get(obj, prop)[0][1] == ("left" if kind == ObjType.Map else "right")


def test_get_all_preserves_equal_concurrent_values() -> None:
    doc = Document(actor_id=b"A")
    other = doc.fork()
    other.set_actor(b"B")
    for branch in (doc, other):
        with branch.transaction() as tx:
            tx.put(ROOT, "value", ScalarType.Int, 1)
    doc.merge(other)
    conflicts = doc.get_all(ROOT, "value")
    assert len(conflicts) == 2
    assert [value for value, _ in conflicts] == [(ScalarType.Int, 1)] * 2
    assert conflicts[0][1] != conflicts[1][1]


def test_get_all_rollback() -> None:
    doc = Document()
    with doc.transaction() as tx:
        tx.put(ROOT, "value", ScalarType.Str, "committed")
    committed = doc.get_all(ROOT, "value")
    with pytest.raises(RuntimeError, match="rollback"):
        with doc.transaction() as tx:
            tx.put(ROOT, "value", ScalarType.Str, "pending")
            assert tx.get_all(ROOT, "value")[0][0] == (ScalarType.Str, "pending")
            raise RuntimeError("rollback")
    assert doc.get_all(ROOT, "value") == committed


def test_get_all_invalid_arguments() -> None:
    doc = Document()

    # Invalid IDs, property types, and heads should error, not look like a
    # missing property. Exercise both the document and transaction read paths.
    def check(reader):
        with pytest.raises(ValueError):
            reader.get_all(b"invalid object id", "key")
        with pytest.raises(ValueError):
            reader.get_all(ROOT, "key", heads=[b"invalid hash"])
        with pytest.raises(Exception):
            reader.get_all(ROOT, 0)  # Sequence index on a map.

    check(doc)
    with doc.transaction() as tx:
        check(tx)
