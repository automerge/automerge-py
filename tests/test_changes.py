import datetime
from automerge import Document, ROOT, ScalarType

def test_get_changes():
    doc = Document()
    changes = doc.get_changes([])
    assert len(changes) == 0
    with doc.transaction() as tx:
        tx.put(ROOT, "hello", ScalarType.Str, "world")
    changes = doc.get_changes([])
    assert len(changes) == 1
    change = changes[0]
    assert change.actor_id() == doc.get_actor()
    assert len(change) == 1
    assert change.max_op() == 1
    assert change.start_op() == 1
    assert change.message() == None
    assert change.deps() == []
    assert change.hash() is not None
    assert change.seq() == 1
    assert isinstance(change.timestamp(), datetime.datetime)
    assert change.bytes() is not None
    
    with doc.transaction() as tx:
        tx.put(ROOT, "foo", ScalarType.Str, "bar")
    changes = doc.get_changes([])
    assert len(changes) == 2
    assert changes[0].hash() == change.hash()
    assert changes[1].deps() == [change.hash()]
    assert changes[1].seq() == 2
    second_change_hash = changes[1].hash()
    
    changes = doc.get_changes([change.hash()])
    assert len(changes) == 1
    assert changes[0].hash() == second_change_hash

if __name__ == "__main__":
    test_get_changes()
