from ._automerge import *

def extract(doc, obj_id=ROOT):
    match doc.object_type(obj_id):
        case ObjType.Map:
            d = {}
            for k in doc.keys(obj_id):
                v, id = doc.get(obj_id, k)
                d[k] = extract(doc, id) if isinstance(v, ObjType) else v[1]
            return d
        case ObjType.List:
            d = []
            for k in range(0, doc.length(obj_id)):
                v, id = doc.get(obj_id, k)
                d.append(extract(doc, id) if isinstance(v, ObjType) else v[1])
            return d
        case ObjType.Text:
            return doc.text(obj_id)


__doc__ = _automerge.__doc__
if hasattr(_automerge, "__all__"):
    __all__ = _automerge.__all__
