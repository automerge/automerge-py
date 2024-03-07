from automerge import Document, ROOT, ObjType, ScalarType, extract

def test_basic():
    doc = Document()

    with doc.transaction() as tx:
        hello = tx.put_object(ROOT, "hello", ObjType.Map)
        tx.put(hello, "test", ScalarType.Str, "world")
        text = tx.put_object(ROOT, "text", ObjType.Text)
        tx.insert(text, 0, ScalarType.Str, "h")
        tx.insert(text, 1, ScalarType.Str, "i")

    assert extract(doc) == {'hello': {'test': 'world'}, 'text': 'hi'}
