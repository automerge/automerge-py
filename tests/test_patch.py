import pytest

from automerge.core import ROOT, Document, ObjType, ScalarType


def make_patch(setup_fn):
    """Apply setup_fn to a fresh document, return the resulting patches."""
    doc = Document()
    before = doc.get_heads()
    with doc.transaction() as tx:
        setup_fn(tx)
    return doc.diff(before, doc.get_heads())


def test_put_map():
    patches = make_patch(lambda tx: tx.put(ROOT, "title", ScalarType.Str, "hello"))
    assert len(patches) == 1
    p = patches[0]
    assert p.action == "put"
    assert p.path == ["title"]
    assert p.value == (ScalarType.Str, "hello")
    assert p.conflict is False
    assert p.length is None
    assert p.marks is None


def test_delete_map():
    doc = Document()
    with doc.transaction() as tx:
        tx.put(ROOT, "title", ScalarType.Str, "hello")
    before = doc.get_heads()
    with doc.transaction() as tx:
        tx.delete(ROOT, "title")
    patches = doc.diff(before, doc.get_heads())
    p = next(p for p in patches if p.action == "del")
    assert p.path == ["title"]
    assert p.length is None
    assert p.value is None


def test_insert():
    doc = Document()
    with doc.transaction() as tx:
        tx.put_object(ROOT, "items", ObjType.List)
    items = doc.get(ROOT, "items")[1]
    before = doc.get_heads()
    with doc.transaction() as tx:
        tx.insert(items, 0, ScalarType.Int, 1)
        tx.insert(items, 1, ScalarType.Int, 2)
    patches = doc.diff(before, doc.get_heads())
    p = next(p for p in patches if p.action == "insert")
    assert p.path == ["items", 0]
    assert p.value is not None  # list of inserted values


def test_put_seq():
    doc = Document()
    with doc.transaction() as tx:
        lst = tx.put_object(ROOT, "xs", ObjType.List)
        tx.insert(lst, 0, ScalarType.Int, 99)
    lst_id = doc.get(ROOT, "xs")[1]
    before = doc.get_heads()
    with doc.transaction() as tx:
        tx.put(lst_id, 0, ScalarType.Int, 42)
    patches = doc.diff(before, doc.get_heads())
    p = next(p for p in patches if p.action == "put")
    assert p.path == ["xs", 0]
    assert p.conflict is False


def test_delete_seq():
    doc = Document()
    with doc.transaction() as tx:
        lst = tx.put_object(ROOT, "xs", ObjType.List)
        tx.insert(lst, 0, ScalarType.Int, 1)
        tx.insert(lst, 1, ScalarType.Int, 2)
    lst_id = doc.get(ROOT, "xs")[1]
    before = doc.get_heads()
    with doc.transaction() as tx:
        tx.delete(lst_id, 0)
    patches = doc.diff(before, doc.get_heads())
    p = next(p for p in patches if p.action == "del")
    assert p.path == ["xs", 0]
    assert p.length == 1


@pytest.mark.skip(
    reason="Counter scalars hit todo!() in PyScalarValue — pre-existing limitation"
)
def test_increment():
    doc = Document()
    with doc.transaction() as tx:
        tx.put(ROOT, "score", ScalarType.Counter, 0)
    before = doc.get_heads()
    with doc.transaction() as tx:
        tx.increment(ROOT, "score", 5)
    patches = doc.diff(before, doc.get_heads())
    p = next(p for p in patches if p.action == "inc")
    assert p.path == ["score"]
    assert p.value == 5


def test_splice_text():
    doc = Document()
    with doc.transaction() as tx:
        text = tx.put_object(ROOT, "body", ObjType.Text)
        tx.insert(text, 0, ScalarType.Str, "h")
        tx.insert(text, 1, ScalarType.Str, "i")
    text_id = doc.get(ROOT, "body")[1]
    before = doc.get_heads()
    with doc.transaction() as tx:
        tx.insert(text_id, 2, ScalarType.Str, "!")
    patches = doc.diff(before, doc.get_heads())
    p = next(p for p in patches if p.action == "splice")
    assert p.path == ["body", 2]
    assert p.value == "!"
    assert p.marks is None or isinstance(p.marks, list)


def test_to_dict():
    patches = make_patch(lambda tx: tx.put(ROOT, "x", ScalarType.Int, 7))
    d = patches[0].to_dict()
    assert isinstance(d, dict)
    assert d["action"] == "put"
    assert d["path"] == ["x"]
    assert "value" in d
    assert d.get("conflict") is False


def test_eq():
    doc = Document()
    before = doc.get_heads()
    with doc.transaction() as tx:
        tx.put(ROOT, "k", ScalarType.Str, "v")
    patches = doc.diff(before, doc.get_heads())
    assert patches[0] == patches[0]


@pytest.mark.asyncio
async def test_patch_via_repo():
    """Integration: patches arrive via Repo change event with correct shape."""
    from automerge.repo import InMemoryStorage, Repo

    repo = await Repo.load(InMemoryStorage())
    async with repo:
        h = await repo.create()
        captured = []
        h.on("change", captured.append)
        with h.change() as doc:
            doc["title"] = "hello"
        import asyncio

        await asyncio.sleep(0.1)
    assert len(captured) == 1
    p = captured[0][0]
    assert p.action == "put"
    assert p.path == ["title"]
