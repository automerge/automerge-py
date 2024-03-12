import automerge.core as core
from datetime import datetime
from typing import Iterator, cast, Mapping, Sequence, MutableMapping, MutableSequence, overload, TypeAlias, Iterable, KeysView
from contextlib import contextmanager

class ActorId:
    def __init__(self, id: bytes):
        self.id = id

    @staticmethod
    def random() -> 'ActorId':
        return ActorId(core.random_actor_id())
    
class ObjectId:
    def __init__(self, id: bytes):
        self.id = id
        
class ChangeHash:
    def __init__(self, hash: bytes):
        self.hash = hash

ProxyThing: TypeAlias = 'core.ScalarValue | Mapping[str, ProxyThing] | Sequence[ProxyThing]'

class ReadProxy:
    _doc: core.Document
    _obj_id: bytes
    _heads: list[bytes] | None

    def __init__(self, doc: core.Document, obj_id: bytes, heads: list[bytes] | None):
        self._doc = doc
        self._obj_id = obj_id
        self._heads = heads
        
    def __len__(self) -> int:
        return self._doc.length(self._obj_id, self._heads)

    def to_py(self) -> core.Thing:
        return core.extract(self._doc, self._obj_id)

    def _maybe_wrap(self, x: tuple[core.Value, bytes]) -> 'MapReadProxy | ListReadProxy | core.ScalarValue':
        value, obj_id = x
        if isinstance(value, core.ObjType):
            match value:
                case core.ObjType.List:
                    return ListReadProxy(self._doc, obj_id, self._heads)
                case core.ObjType.Map:
                    return MapReadProxy(self._doc, obj_id, self._heads)
            raise Exception("unknown obj type")
        _, v = value
        return v

class MapReadProxy(ReadProxy, Mapping[str, ProxyThing]):
    def __getitem__(self, key: str) -> 'MapReadProxy | ListReadProxy | core.ScalarValue':
        x = self._doc.get(self._obj_id, key, self._heads)
        if x is None: raise IndexError()
        return self._maybe_wrap(x)
    
    def __iter__(self) -> Iterator[str]:
        return iter(self._doc.keys(self._obj_id, self._heads))

class ListReadProxy(ReadProxy, Sequence[ProxyThing]):
    @overload
    def __getitem__(self, key: int) -> ProxyThing: ...
    @overload
    def __getitem__(self, key: slice) -> Sequence[ProxyThing]: ...
    def __getitem__(self, key: int | slice) -> ProxyThing | Sequence[ProxyThing]:
        if not isinstance(key, int): raise NotImplemented
        x = self._doc.get(self._obj_id, key, self._heads)
        if x is None: raise IndexError()
        return self._maybe_wrap(x)
    
class WriteProxy:
    _tx: core.Transaction
    _obj_id: bytes
    _heads: list[bytes] | None
    
    def __init__(self, tx: core.Transaction, obj_id: bytes, heads: list[bytes] | None) -> None:
        self._tx = tx
        self._obj_id = obj_id
        self._heads = heads

    def __len__(self) -> int:
        return self._tx.length(self._obj_id, self._heads)

MutableProxyThing: TypeAlias = 'core.ScalarValue | MutableMapping[str, MutableProxyThing] | MutableSequence[MutableProxyThing]'

class MapWriteProxy(WriteProxy, MutableMapping[str, MutableProxyThing]):
    _tx: core.Transaction

    def __getitem__(self, key: str | int) -> MutableProxyThing:
        x = self._tx.get(self._obj_id, key, self._heads)
        if x is None: return None
        value, obj_id = x
        if isinstance(value, core.ObjType):
            match value:
                case core.ObjType.Map:
                    return MapWriteProxy(self._tx, obj_id, self._heads)
                case core.ObjType.List:
                    return ListWriteProxy(self._tx, obj_id, self._heads)
            raise Exception("unknown ObjType")
        _, v = value
        return v
    
    def __setitem__(self, key: str, value: MutableProxyThing) -> None:
        if isinstance(value, MutableMapping):
            obj_id = self._tx.put_object(self._obj_id, key, core.ObjType.Map)
            m = MapWriteProxy(self._tx, obj_id, self._heads)
            for k, dv in value.items():
                m[k] = dv
        elif isinstance(value, MutableSequence):
            obj_id = self._tx.put_object(self._obj_id, key, core.ObjType.List)
            l = ListWriteProxy(self._tx, obj_id, self._heads)
            for i, lv in enumerate(value):
                l[i] = lv
        else: # scalar
            # Don't change the type of the value if there's already a value here.
            x = self._tx.get(self._obj_id, key, self._heads)
            if x is None:
                t = _infer_scalar_type(value)
            else:
                val, _ = x
                if isinstance(val, core.ObjType):
                    t = _infer_scalar_type(value)
                else:
                    t, _ = val
            self._tx.put(self._obj_id, key, t, value)
            
    def __delitem__(self, key: str) -> None:
        self._tx.delete(self._obj_id, key)
        
    def __iter__(self) -> Iterator[str]:
        raise NotImplemented

class ListWriteProxy(WriteProxy, MutableSequence[MutableProxyThing]):
    @overload
    def __getitem__(self, key: int) -> MutableProxyThing: ...
    @overload
    def __getitem__(self, key: slice) -> MutableSequence[MutableProxyThing]: ...
    def __getitem__(self, key: int | slice) -> MutableProxyThing | MutableSequence[MutableProxyThing]:
        if not isinstance(key, int): raise NotImplemented
        x = self._tx.get(self._obj_id, key, self._heads)
        if x is None: return None
        value, obj_id = x
        if isinstance(value, core.ObjType):
            match value:
                case core.ObjType.Map:
                    return MapWriteProxy(self._tx, obj_id, self._heads)
                case core.ObjType.List:
                    return ListWriteProxy(self._tx, obj_id, self._heads)
            raise Exception("unknown ObjType")
        _, v = value
        return v
    
    @overload
    def __setitem__(self, key: int, value: MutableProxyThing) -> None: ...
    @overload
    def __setitem__(self, key: slice, value: Iterable[MutableProxyThing]) -> None: ...
    def __setitem__(self, idx: int | slice, value: MutableProxyThing | Iterable[MutableProxyThing]) -> None:
        if not isinstance(idx, int): raise NotImplemented
        if isinstance(value, MutableMapping):
            if idx >= self._tx.length(self._obj_id, self._heads):
                obj_id = self._tx.insert_object(self._obj_id, idx, core.ObjType.Map)
            else:
                obj_id = self._tx.put_object(self._obj_id, idx, core.ObjType.Map)
            m = MapWriteProxy(self._tx, obj_id, self._heads)
            for k, dv in value.items():
                m[k] = dv
        elif isinstance(value, MutableSequence):
            if idx >= self._tx.length(self._obj_id, self._heads):
                obj_id = self._tx.insert_object(self._obj_id, idx, core.ObjType.List)
            else:
                obj_id = self._tx.put_object(self._obj_id, idx,
                                             core.ObjType.List)
            l = ListWriteProxy(self._tx, obj_id, self._heads)
            for i, lv in enumerate(cast(MutableSequence[MutableProxyThing], value)):
                l[i] = lv
        else: # scalar
            # Don't change the type of the value if there's already a value here.
            value = cast(core.ScalarValue, value)
            x = self._tx.get(self._obj_id, idx, self._heads)
            if x is None:
                t = _infer_scalar_type(value)
            else:
                val, _ = x
                if isinstance(val, core.ObjType):
                    t = _infer_scalar_type(value)
                else:
                    t, _ = val
            if idx >= self._tx.length(self._obj_id, self._heads):
                self._tx.insert(self._obj_id, idx, t, value)
            else:
                self._tx.put(self._obj_id, idx, t, value)

    @overload
    def __delitem__(self, idx: int) -> None: ...
    @overload
    def __delitem__(self, idx: slice) -> None: ...
    def __delitem__(self, idx: int | slice) -> None:
        if not isinstance(idx, int): raise NotImplemented
        self._tx.delete(self._obj_id, idx)
        
    def insert(self, idx: int, value: core.ScalarValue | MutableMapping[str, MutableProxyThing] | MutableSequence[MutableProxyThing] | None) -> None:
        if not isinstance(idx, int): raise NotImplemented
        if isinstance(value, MutableMapping):
            obj_id = self._tx.insert_object(self._obj_id, idx, core.ObjType.Map)
            m = MapWriteProxy(self._tx, obj_id, self._heads)
            for k, dv in value.items():
                m[k] = dv
        elif isinstance(value, MutableSequence):
            obj_id = self._tx.insert_object(self._obj_id, idx, core.ObjType.List)
            l = ListWriteProxy(self._tx, obj_id, self._heads)
            for i, lv in enumerate(value):
                l[i] = lv
        else: # scalar
            # Don't change the type of the value if there's already a value here.
            x = self._tx.get(self._obj_id, idx, self._heads)
            if x is None:
                t = _infer_scalar_type(value)
            else:
                val, _ = x
                if isinstance(val, core.ObjType):
                    t = _infer_scalar_type(value)
                else:
                    t, _ = val
            if idx >= self._tx.length(self._obj_id, self._heads):
                self._tx.insert(self._obj_id, idx, t, value)
            else:
                self._tx.put(self._obj_id, idx, t, value)

def _infer_scalar_type(value: core.ScalarValue) -> core.ScalarType:
    if isinstance(value, str):
        return core.ScalarType.Str
    elif isinstance(value, bytes):
        return core.ScalarType.Bytes
    elif isinstance(value, int):
        return core.ScalarType.Int
    elif isinstance(value, float):
        return core.ScalarType.F64
    elif isinstance(value, bool):
        return core.ScalarType.Boolean
    elif isinstance(value, datetime):
        return core.ScalarType.Timestamp
    elif value is None:
        return core.ScalarType.Null
    raise ValueError(f"unknown scalar: {value!r}")


class Document(MapReadProxy):
    def __init__(self, actor_id: ActorId | None = None) -> None:
        self._doc = core.Document(actor_id.id if actor_id else None)
        super().__init__(self._doc, core.ROOT, None)
        
    @contextmanager
    def change(self) -> Iterator[MapWriteProxy]:
        with self._doc.transaction() as tx:
            yield MapWriteProxy(tx, core.ROOT, None)
    