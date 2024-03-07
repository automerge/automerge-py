from automerge.core import Document, ROOT, SyncState, Message, ScalarType, extract

def test_sync():
    doc1 = Document()
    with doc1.transaction() as tx:
        tx.put(ROOT, "hello", ScalarType.Str, "world")
        
    p1_state_p2 = SyncState()

    doc2 = Document()
    p2_state_p1 = SyncState()

    # Exchange messages until settled.

    while True:
        # sync from p1 to p2
        msg = doc1.generate_sync_message(p1_state_p2)
        if not msg:
            break

        # round trip through encode/decode to simulate network
        msg = Message.decode(msg.encode())

        doc2.receive_sync_message(p2_state_p1, msg)
        
        # now sync from p2 to p1
        msg = doc2.generate_sync_message(p2_state_p1)
        if not msg:
            break

        # round trip through encode/decode to simulate network
        msg = Message.decode(msg.encode())
        
        doc1.receive_sync_message(p1_state_p2, msg)
        
    assert extract(doc2) == extract(doc1)
