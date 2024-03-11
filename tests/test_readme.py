from automerge.core import Document, ROOT, ObjType, ScalarType, extract

def test_readme() -> None:
    doc = Document()
    with doc.transaction() as tx:
        list = tx.put_object(ROOT, "colours", ObjType.List)
        tx.insert(list, 0, ScalarType.Str, "blue")
        tx.insert(list, 1, ScalarType.Str, "red")

    doc2 = doc.fork()
    with doc2.transaction() as tx:
        tx.insert(list, 0, ScalarType.Str, "green")

    with doc.transaction() as tx:
        tx.delete(list, 0)

    doc.merge(doc2)  # `doc` now contains {"colours": ["green", "red"]}
    assert extract(doc) == {'colours': ['green', 'red']}
