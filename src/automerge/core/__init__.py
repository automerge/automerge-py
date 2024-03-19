from datetime import datetime
from .. import _automerge
from .._automerge import *

ScalarValue = str | bytes | int | float | bool | datetime | None
Thing = dict[str, 'Thing'] | list['Thing'] | ScalarValue
Value = ObjType | tuple[ScalarType, ScalarValue]

def extract(doc: Document, obj_id: bytes = ROOT) -> Thing:
    ot = doc.object_type(obj_id)
    if ot == ObjType.Map:
        d: dict[str, Thing] = {}
        for k in doc.keys(obj_id):
            x = doc.get(obj_id, k)
            assert x is not None
            v, id = x
            d[k] = extract(doc, id) if isinstance(v, ObjType) else v[1]
        return d
    elif ot == ObjType.List:
        l: list[Thing] = []
        for k2 in range(0, doc.length(obj_id)):
            x = doc.get(obj_id, k2)
            assert x is not None
            v, id = x
            l.append(extract(doc, id) if isinstance(v, ObjType) else v[1])
        return l
    elif ot == ObjType.Text:
        return doc.text(obj_id)
    raise Exception("unexpected result from doc.object_type")

__doc__ = _automerge.__doc__
if hasattr(_automerge, "__all__"):
    __all__ = _automerge.__all__
