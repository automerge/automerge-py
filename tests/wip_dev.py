import automerge

from pdb import set_trace as bp

text = automerge.Text("")
print(text)

text.insert(0, 'hello')
print(text)

del text[0]
print(text)

text.insert(0, 'H')
print(text)

text.insert(5, [' ',  'w', 'o', 'r', 'l', 'd', ' ', '!'])
print(text, flush=True)


doc = automerge.Doc()
with doc as d:

    # doesn't work yet
    # TODO : ApplyPatch.update_map_object
    # see def apply_diffs
    bp()
    d["text"] = text
