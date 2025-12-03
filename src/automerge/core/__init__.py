from datetime import datetime
from typing import Dict, List, Tuple, Union

from .. import _automerge

# from .._automerge import *
from .._automerge import (
    ROOT,
    Document,
    ExpandMark,
    Message,
    ObjType,
    ScalarType,
    SyncState,
)

__all__ = [
    "ROOT",
    "Document",
    "ExpandMark",
    "Message",
    "ObjType",
    "ScalarType",
    "SyncState",
    "extract",
]

ScalarValue = Union[str, bytes, int, float, bool, datetime, None]
Thing = Union[Dict[str, "Thing"], List["Thing"], ScalarValue]
Value = Union[ObjType, Tuple[ScalarType, ScalarValue]]


def extract(doc: Document, obj_id: bytes = ROOT) -> Thing:
    ot = doc.object_type(obj_id)
    if ot == ObjType.Map:
        d: Dict[str, Thing] = {}
        for k in doc.keys(obj_id):
            x = doc.get(obj_id, k)
            assert x is not None
            v, id = x
            d[k] = extract(doc, id) if isinstance(v, ObjType) else v[1]
        return d
    elif ot == ObjType.List:
        thelist: List[Thing] = []
        for k2 in range(0, doc.length(obj_id)):
            x = doc.get(obj_id, k2)
            assert x is not None
            v, id = x
            thelist.append(extract(doc, id) if isinstance(v, ObjType) else v[1])
        return thelist
    elif ot == ObjType.Text:
        return doc.text(obj_id)
    raise Exception("unexpected result from doc.object_type")


__doc__ = _automerge.__doc__
# if hasattr(_automerge, "__all__"):
#     __all__ = _automerge.__all__
