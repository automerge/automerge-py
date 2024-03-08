from automerge.core import Document, ROOT, ObjType, ScalarType, extract

def test_basic():
    doc = Document()

    with doc.transaction() as tx:
        hello = tx.put_object(ROOT, "hello", ObjType.Map)
        tx.put(hello, "test", ScalarType.Str, "world")
        text = tx.put_object(ROOT, "text", ObjType.Text)
        tx.insert(text, 0, ScalarType.Str, "h")
        tx.insert(text, 1, ScalarType.Str, "i")

    assert extract(doc) == {'hello': {'test': 'world'}, 'text': 'hi'}

def test_actor_id():
    doc = Document(actor_id=b'foo')
    assert doc.get_actor() == b'foo'
    doc.set_actor(b'bar')
    assert doc.get_actor() == b'bar'

def test_keys():
    doc = Document()

    with doc.transaction() as tx:
        map_id = tx.put_object(ROOT, "map", ObjType.Map)
        tx.put(map_id, "foo", ScalarType.Boolean, True)
        tx.put(map_id, "hello", ScalarType.Str, "world")
        list_id = tx.put_object(ROOT, "list", ObjType.List)
        tx.insert(list_id, 0, ScalarType.Str, "one")
        tx.insert(list_id, 1, ScalarType.Boolean, True)

    assert doc.keys(ROOT) == ['list', 'map']
    assert doc.keys(map_id) == ['foo', 'hello']
    list_keys = doc.keys(list_id)
    assert len(list_keys) == 2

def test_values():
    doc = Document()

    with doc.transaction() as tx:
        map_id = tx.put_object(ROOT, "map", ObjType.Map)
        tx.put(map_id, "foo", ScalarType.Boolean, True)
        tx.put(map_id, "hello", ScalarType.Str, "world")
        list_id = tx.put_object(ROOT, "list", ObjType.List)
        tx.insert(list_id, 0, ScalarType.Str, "one")
        tx.insert(list_id, 1, ScalarType.Boolean, True)

    values = doc.values(map_id)
    assert [v[0] for v in values] == [(ScalarType.Boolean, True), (ScalarType.Str, "world")]
    values = doc.values(list_id)
    assert [v[0] for v in values] == [(ScalarType.Str, "one"), (ScalarType.Boolean, True)]

def test_diff():
    doc = Document()

    with doc.transaction() as tx:
        map_id = tx.put_object(ROOT, "map", ObjType.Map)
        tx.put(map_id, "hello", ScalarType.Str, "world")
        list_id = tx.put_object(ROOT, "list", ObjType.List)
        tx.insert(list_id, 0, ScalarType.Boolean, True)

    patch = doc.diff([], doc.get_heads())
    assert len(patch) == 4

