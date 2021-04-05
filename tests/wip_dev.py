import automerge

from pdb import set_trace as bp


# This tests a modification of a Text value *outside* of a context
txt_out = automerge.Text("")
txt_out.insert(0, 'hello')
del txt_out[0]
txt_out.insert(0, 'H')
txt_out.insert(5, ' world !')

print(txt_out)  # should be "Hello world !"


doc = automerge.Doc()
with doc as d:
    # This tests the assignment of a Text value built outside of the change context,
    # and assigned inside the change context.
    d["txt_outside"] = txt_out

    # This tests the creation and modification of a Text value from inside a change context
    d["text"] = automerge.Text("")
    d["text"].insert(0, 'hello')
    del d["text"][0]
    d["text"].insert(0, 'H')
    d["text"].insert(5, ' world !')
    print(doc["text"])

bp()
print(doc["text"])
print(doc["txt_outside"])

bp()
print(doc.changes())
