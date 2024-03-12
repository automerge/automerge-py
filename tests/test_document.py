from automerge import Document

def test_doc() -> None:
    doc = Document()
    with doc.change() as c:
        c['hello'] = 'world'

    assert doc['hello'] == 'world'
    
def test_doc_import_obj() -> None:
    doc = Document()
    with doc.change() as c:
        c['hello'] = {'this': {'is': 'a', 'whole': 'tree'}, 'of': [{'obj': 'ect'}, 's']}
        
    assert doc.to_py() == {'hello': {'this': {'is': 'a', 'whole': 'tree'}, 'of': [{'obj': 'ect'}, 's']}}
    
def test_nested_import() -> None:
    doc = Document()
    with doc.change() as c:
        c['hello'] = {}
        c['hello']['foo'] = 'bar'
        
    assert doc.to_py() == {'hello': {'foo': 'bar'}}
    {}.__getitem__
    
def test_iterable() -> None:
    doc = Document()
    with doc.change() as c:
        c['list'] = [1, 2, 3]
        c['map'] = {
            'k': 'v',
            'x': 'y',
        }
    assert len(doc['list']) == 3
    assert list(doc['list']) == [1, 2, 3]
    assert len(doc['map']) == 2
    assert list(doc['map']) == ['k', 'x']
    assert list(doc['map'].items()) == [('k', 'v'), ('x', 'y')]
