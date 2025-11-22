from contextlib import contextmanager
from datetime import datetime
from typing import (
    Iterable,
    Iterator,
    List,
    Mapping,
    MutableMapping,
    MutableSequence,
    Optional,
    Sequence,
    Tuple,
    Union,
    cast,
    overload,
)

import automerge.core as core


class ActorId:
    def __init__(self, id: bytes):
        self.id = id

    @staticmethod
    def random() -> "ActorId":
        return ActorId(core.random_actor_id())


class ObjectId:
    def __init__(self, id: bytes):
        self.id = id


class ChangeHash:
    def __init__(self, hash: bytes):
        self.hash = hash


ProxyThing = Union[core.ScalarValue, Mapping[str, "ProxyThing"], Sequence["ProxyThing"]]


class ReadProxy:
    _doc: core.Document
    _obj_id: bytes
    _heads: Union[List[bytes], None]

    def __init__(self, doc: core.Document, obj_id: bytes, heads: Optional[List[bytes]]):
        self._doc = doc
        self._obj_id = obj_id
        self._heads = heads

    def __len__(self) -> int:
        return self._doc.length(self._obj_id, self._heads)

    def to_py(self) -> core.Thing:
        return core.extract(self._doc, self._obj_id)

    def _maybe_wrap(
        self, x: Tuple[core.Value, bytes]
    ) -> "MapReadProxy | ListReadProxy | core.ScalarValue":
        value, obj_id = x
        if isinstance(value, core.ObjType):
            if value == core.ObjType.List:
                return ListReadProxy(self._doc, obj_id, self._heads)
            elif value == core.ObjType.Map:
                return MapReadProxy(self._doc, obj_id, self._heads)
            raise Exception("unknown obj type")
        _, v = value
        return v


class MapReadProxy(ReadProxy, Mapping[str, ProxyThing]):
    def __getitem__(
        self, key: str
    ) -> "MapReadProxy | ListReadProxy | core.ScalarValue":
        x = self._doc.get(self._obj_id, key, self._heads)
        if x is None:
            raise IndexError()
        return self._maybe_wrap(x)

    def __iter__(self) -> Iterator[str]:
        return iter(self._doc.keys(self._obj_id, self._heads))


class ListReadProxy(ReadProxy, Sequence[ProxyThing]):
    @overload
    def __getitem__(self, key: int) -> ProxyThing: ...
    @overload
    def __getitem__(self, key: slice) -> Sequence[ProxyThing]: ...
    def __getitem__(
        self, key: Union[int, slice]
    ) -> Union[ProxyThing, Sequence[ProxyThing]]:
        if not isinstance(key, int):
            raise NotImplemented
        x = self._doc.get(self._obj_id, key, self._heads)
        if x is None:
            raise IndexError()
        return self._maybe_wrap(x)


class WriteProxy:
    _tx: core.Transaction
    _obj_id: bytes
    _heads: Optional[List[bytes]]

    def __init__(
        self, tx: core.Transaction, obj_id: bytes, heads: Optional[List[bytes]]
    ) -> None:
        self._tx = tx
        self._obj_id = obj_id
        self._heads = heads

    def __len__(self) -> int:
        return self._tx.length(self._obj_id, self._heads)

    def _infer_scalar_type_for_key(
        self,
        key: Union[str, int],
        value: core.ScalarValue,
        default_type: core.ScalarType,
    ) -> core.ScalarType:
        """
        Infer the scalar type to use for a value, preserving existing type if present.

        Args:
            key: The key or index where the value will be stored
            value: The scalar value being stored
            default_type: The type to use if there's no existing value or it can be inferred

        Returns:
            The scalar type to use for storage
        """
        x = self._tx.get(self._obj_id, key, self._heads)
        if x is None:
            return default_type
        else:
            val, _ = x
            if isinstance(val, core.ObjType):
                return default_type
            else:
                t, _ = val
                return t

    def _insert_value(
        self,
        key: Union[str, int],
        value: "MutableProxyThing",
        operation: str = "put",
    ) -> None:
        """
        Insert a value into the document, handling type detection and object creation.

        Args:
            key: The key (string) or index (int) where to insert
            value: The value to insert
            operation: One of "put", "insert", or "put_or_insert"
                - "put": Always use put/put_object
                - "insert": Always use insert/insert_object
                - "put_or_insert": Use insert if index >= length, else put (for lists)
        """
        if isinstance(value, MutableMapping):
            obj_id = self._create_object(key, core.ObjType.Map, operation)
            m = MapWriteProxy(self._tx, obj_id, self._heads)
            for k, dv in value.items():
                m[k] = dv
        elif isinstance(value, MutableSequence):
            obj_id = self._create_object(key, core.ObjType.List, operation)
            l = ListWriteProxy(self._tx, obj_id, self._heads)
            for i, lv in enumerate(value):
                l[i] = lv
        else:  # scalar
            value = cast(core.ScalarValue, value)
            t = self._infer_scalar_type_for_key(key, value, _infer_scalar_type(value))
            self._put_scalar(key, t, value, operation)

    def _create_object(
        self, key: Union[str, int], obj_type: core.ObjType, operation: str
    ) -> bytes:
        """
        Create an object in the document based on the operation type.

        Args:
            key: The key or index where to create the object
            obj_type: The type of object to create
            operation: "put", "insert", or "put_or_insert"

        Returns:
            The object ID of the created object
        """
        if operation == "put":
            return self._tx.put_object(self._obj_id, key, obj_type)
        elif operation == "insert":
            return self._tx.insert_object(self._obj_id, key, obj_type)
        elif operation == "put_or_insert":
            if isinstance(key, int) and key >= self._tx.length(
                self._obj_id, self._heads
            ):
                return self._tx.insert_object(self._obj_id, key, obj_type)
            else:
                return self._tx.put_object(self._obj_id, key, obj_type)
        else:
            raise ValueError(f"Unknown operation: {operation}")

    def _put_scalar(
        self,
        key: Union[str, int],
        scalar_type: core.ScalarType,
        value: core.ScalarValue,
        operation: str,
    ) -> None:
        """
        Put a scalar value into the document based on the operation type.

        Args:
            key: The key or index where to put the value
            scalar_type: The type of the scalar value
            value: The scalar value to put
            operation: "put", "insert", or "put_or_insert"
        """
        if operation == "put":
            self._tx.put(self._obj_id, key, scalar_type, value)
        elif operation == "insert":
            self._tx.insert(self._obj_id, key, scalar_type, value)
        elif operation == "put_or_insert":
            if isinstance(key, int) and key >= self._tx.length(
                self._obj_id, self._heads
            ):
                self._tx.insert(self._obj_id, key, scalar_type, value)
            else:
                self._tx.put(self._obj_id, key, scalar_type, value)
        else:
            raise ValueError(f"Unknown operation: {operation}")


# Forward declaration for TextWriteProxy
class TextWriteProxy: ...


MutableProxyThing = Union[
    core.ScalarValue,
    MutableMapping[str, "MutableProxyThing"],
    MutableSequence["MutableProxyThing"],
    "TextWriteProxy",
]


class MapWriteProxy(WriteProxy, MutableMapping[str, MutableProxyThing]):
    _tx: core.Transaction

    def __getitem__(self, key: Union[str, int]) -> MutableProxyThing:
        x = self._tx.get(self._obj_id, key, self._heads)
        if x is None:
            return None
        value, obj_id = x
        if isinstance(value, core.ObjType):
            if value == core.ObjType.Map:
                return MapWriteProxy(self._tx, obj_id, self._heads)
            elif value == core.ObjType.List:
                return ListWriteProxy(self._tx, obj_id, self._heads)
            raise Exception("unknown ObjType")
        _, v = value
        return v

    def __setitem__(self, key: str, value: MutableProxyThing) -> None:
        self._insert_value(key, value, operation="put")

    def __delitem__(self, key: str) -> None:
        self._tx.delete(self._obj_id, key)

    def __iter__(self) -> Iterator[str]:
        raise NotImplemented


class ListWriteProxy(WriteProxy, MutableSequence[MutableProxyThing]):
    @overload
    def __getitem__(self, key: int) -> MutableProxyThing: ...
    @overload
    def __getitem__(self, key: slice) -> MutableSequence[MutableProxyThing]: ...
    def __getitem__(
        self, key: Union[int, slice]
    ) -> Union[MutableProxyThing, MutableSequence[MutableProxyThing]]:
        if not isinstance(key, int):
            raise NotImplemented
        x = self._tx.get(self._obj_id, key, self._heads)
        if x is None:
            return None
        value, obj_id = x
        if isinstance(value, core.ObjType):
            if value == core.ObjType.Map:
                return MapWriteProxy(self._tx, obj_id, self._heads)
            elif value == core.ObjType.List:
                return ListWriteProxy(self._tx, obj_id, self._heads)
            raise Exception("unknown ObjType")
        _, v = value
        return v

    @overload
    def __setitem__(self, key: int, value: MutableProxyThing) -> None: ...
    @overload
    def __setitem__(self, key: slice, value: Iterable[MutableProxyThing]) -> None: ...
    def __setitem__(
        self,
        idx: Union[int, slice],
        value: Union[MutableProxyThing, Iterable[MutableProxyThing]],
    ) -> None:
        if not isinstance(idx, int):
            raise NotImplemented
        self._insert_value(idx, value, operation="put_or_insert")

    @overload
    def __delitem__(self, idx: int) -> None: ...
    @overload
    def __delitem__(self, idx: slice) -> None: ...
    def __delitem__(self, idx: Union[int, slice]) -> None:
        if not isinstance(idx, int):
            raise NotImplemented
        self._tx.delete(self._obj_id, idx)

    def insert(
        self,
        idx: int,
        value: Union[
            core.ScalarValue,
            MutableMapping[str, MutableProxyThing],
            MutableSequence[MutableProxyThing],
            None,
        ],
    ) -> None:
        if not isinstance(idx, int):
            raise NotImplemented
        self._insert_value(idx, value, operation="insert")


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
    def __init__(self, actor_id: Optional[ActorId] = None) -> None:
        self._doc = core.Document(actor_id.id if actor_id else None)
        super().__init__(self._doc, core.ROOT, None)

    @contextmanager
    def change(self) -> Iterator[MapWriteProxy]:
        with self._doc.transaction() as tx:
            yield MapWriteProxy(tx, core.ROOT, None)
