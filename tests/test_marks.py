from automerge.core import Document, ROOT, ObjType, ScalarType, ExpandMark, extract

def test_marks():
    doc = Document()
    with doc.transaction() as tx:
        text = tx.put_object(ROOT, "text", ObjType.Text)
        tx.insert(text, 0, ScalarType.Str, "h")
        tx.insert(text, 1, ScalarType.Str, "i")
        tx.mark(text, 0, 1, "bold", ScalarType.Boolean, True, ExpandMark.After)
    marks = doc.marks(text)
    assert len(marks) == 1
    mark = marks[0]
    assert mark.name == "bold"
    assert mark.start == 0
    assert mark.end == 1
    assert mark.value == (ScalarType.Boolean, True)

if __name__ == "__main__":
    test_marks()
