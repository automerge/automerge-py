from __future__ import annotations

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


class ImmutableString(str):
    """
    Marker class for creating immutable scalar strings in Automerge documents.

    By default, assigning a string to a document creates a collaborative Text object
    that can be edited concurrently. Use ImmutableString when you want a simple,
    immutable string value instead (e.g., for version numbers, IDs, or metadata).

    Examples:
        >>> doc = Document()
        >>> with doc.change() as d:
        ...     d["content"] = "Editable text"           # Creates Text object
        ...     d["version"] = ImmutableString("1.0.0")  # Creates scalar string

        >>> # Text can be mutated
        >>> with doc.change() as d:
        ...     d["content"].insert(0, "My ")

        >>> # ImmutableString is a plain Python string
        >>> isinstance(doc["version"], str)
        True
        >>> hasattr(doc["version"], "insert")
        False
    """

    pass


ProxyThing = Union[
    core.ScalarValue,
    Mapping[str, "ProxyThing"],
    Sequence["ProxyThing"],
    "Text",
]


MutableProxyThing = Union[
    core.ScalarValue,
    MutableMapping[str, "MutableProxyThing"],
    MutableSequence["MutableProxyThing"],
    "MutableText",
]


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
    ) -> "MapReadProxy | ListReadProxy | Text | core.ScalarValue":
        value, obj_id = x
        if isinstance(value, core.ObjType):
            if value == core.ObjType.List:
                return ListReadProxy(self._doc, obj_id, self._heads)
            elif value == core.ObjType.Map:
                return MapReadProxy(self._doc, obj_id, self._heads)
            elif value == core.ObjType.Text:
                return Text(self._doc, obj_id, self._heads)
            raise Exception("unknown obj type")
        _, v = value
        return v


class MapReadProxy(ReadProxy, Mapping[str, ProxyThing]):
    def __getitem__(
        self, key: str
    ) -> "MapReadProxy | ListReadProxy | Text | core.ScalarValue":
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
            raise NotImplementedError
        x = self._doc.get(self._obj_id, key, self._heads)
        if x is None:
            raise IndexError()
        return self._maybe_wrap(x)


class Text(ReadProxy):
    """
    Represents a read-only collaborative text object in an Automerge document.

    Text objects are sequence of unicode characters that support concurrent editing.
    This class provides string-like access to the text content and automatically
    reflects any changes made to the underlying document.

    To create an immutable scalar string instead, use ImmutableString.
    To mutate text, you must access it within a `doc.change()` block,
    which will return a `MutableText` object.

    Examples:
        >>> doc = Document()
        >>> with doc.change() as d:
        ...     d["content"] = "Hello, world!"
        >>> text = doc["content"]
        >>> str(text)
        'Hello, world!'
        >>> len(text)
        13
        >>> text[0:5]
        'Hello'
        >>> # To mutate:
        >>> with doc.change() as d:
        ...     d["content"].insert(5, ", Automerge")
        >>> str(doc["content"])
        'Hello, Automerge, world!'
    """

    def __str__(self) -> str:
        return self._doc.text(self._obj_id, self._heads)

    def __len__(self) -> int:
        return self._doc.length(self._obj_id, self._heads)

    def __eq__(self, other) -> bool:
        if isinstance(other, Text):
            return self._doc.text(self._obj_id, self._heads) == self._doc.text(
                other._obj_id, other._heads
            )
        elif isinstance(other, MutableText):
            return self._doc.text(self._obj_id, self._heads) == self._doc.text(
                other._obj_id, other._heads
            )
        elif isinstance(other, str):
            return self._doc.text(self._obj_id, self._heads) == other
        return NotImplemented

    @overload
    def __getitem__(self, key: int) -> str: ...
    @overload
    def __getitem__(self, key: slice) -> str: ...
    def __getitem__(self, key: Union[int, slice]) -> str:
        text = self._doc.text(self._obj_id, self._heads)
        return text[key]

    def __repr__(self) -> str:
        text = self._doc.text(self._obj_id, self._heads)
        return f"Text({text!r})"


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
            newList = ListWriteProxy(self._tx, obj_id, self._heads)
            for i, lv in enumerate(value):
                newList[i] = lv
        elif isinstance(value, MutableText) or isinstance(value, Text):
            str_value = str(value)
            obj_id = self._create_object(key, core.ObjType.Text, operation)
            self._tx.splice_text(obj_id, 0, 0, str_value)
        elif isinstance(value, ImmutableString):
            # Explicit immutable string - store as scalar
            t = self._infer_scalar_type_for_key(key, str(value), core.ScalarType.Str)
            self._put_scalar(key, t, str(value), operation)
        elif isinstance(value, str):
            # Default string - create Text object
            obj_id = self._create_object(key, core.ObjType.Text, operation)
            text_proxy = MutableText(self._tx, obj_id, self._heads)
            text_proxy.splice(0, 0, value)
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


class TextWriteProxy: ...


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
            elif value == core.ObjType.Text:
                return MutableText(self._tx, obj_id, self._heads)
            raise Exception("unknown ObjType")
        _, v = value
        return v

    def __setitem__(self, key: str, value: MutableProxyThing) -> None:
        self._insert_value(key, value, operation="put")

    def __delitem__(self, key: str) -> None:
        self._tx.delete(self._obj_id, key)

    def __iter__(self) -> Iterator[str]:
        raise NotImplementedError


class ListWriteProxy(WriteProxy, MutableSequence[MutableProxyThing]):
    @overload
    def __getitem__(self, key: int) -> MutableProxyThing: ...
    @overload
    def __getitem__(self, key: slice) -> MutableSequence[MutableProxyThing]: ...
    def __getitem__(
        self, key: Union[int, slice]
    ) -> Union[MutableProxyThing, MutableSequence[MutableProxyThing]]:
        if not isinstance(key, int):
            raise NotImplementedError
        x = self._tx.get(self._obj_id, key, self._heads)
        if x is None:
            return None
        value, obj_id = x
        if isinstance(value, core.ObjType):
            if value == core.ObjType.Map:
                return MapWriteProxy(self._tx, obj_id, self._heads)
            elif value == core.ObjType.List:
                return ListWriteProxy(self._tx, obj_id, self._heads)
            elif value == core.ObjType.Text:
                return MutableText(self._tx, obj_id, self._heads)
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
            raise NotImplementedError
        self._insert_value(idx, value, operation="put_or_insert")

    @overload
    def __delitem__(self, idx: int) -> None: ...
    @overload
    def __delitem__(self, idx: slice) -> None: ...
    def __delitem__(self, idx: Union[int, slice]) -> None:
        if not isinstance(idx, int):
            raise NotImplementedError
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
            raise NotImplementedError
        self._insert_value(idx, value, operation="insert")


class MutableText(Text, WriteProxy):
    def __init__(
        self, tx: core.Transaction, obj_id: bytes, heads: Optional[List[bytes]]
    ) -> None:
        WriteProxy.__init__(self, tx, obj_id, heads)

    """
    Represents a mutable collaborative text object in an Automerge document,
    available only within a `doc.change()` transaction block.

    This class inherits from `Text` and provides additional mutation methods
    like `insert`, `delete`, and `splice` for editing text content.

    Examples:
        >>> doc = Document()
        >>> with doc.change() as d:
        ...     d["message"] = "Hello"
        ...     d["message"].insert(5, ", world!")
        >>> str(doc["message"])
        'Hello, world!'

        >>> with doc.change() as d:
        ...     d["message"].delete(5, 8)  # Delete ", world!"
        >>> str(doc["message"])
        'Hello'

        >>> with doc.change() as d:
        ...     d["message"].splice(0, 5, "Goodbye")  # Replace "Hello" with "Goodbye"
        >>> str(doc["message"])
        'Goodbye'
    """

    def __str__(self) -> str:
        return self._tx.text(self._obj_id, self._heads)

    def __len__(self) -> int:
        return self._tx.length(self._obj_id, self._heads)

    def __eq__(self, other):
        if isinstance(other, MutableText):
            return self._obj_id == other._obj_id and self._heads == other._heads
        elif isinstance(other, Text):
            return self._tx.text(self._obj_id, self._heads) == other._tx.text(
                other._obj_id, other._heads
            )
        elif isinstance(other, str):
            return self._tx.text(self._obj_id, self._heads) == other
        return False

    @overload
    def __getitem__(self, key: int) -> str: ...
    @overload
    def __getitem__(self, key: slice) -> str: ...
    def __getitem__(self, key: Union[int, slice]) -> str:
        text = self._tx.text(self._obj_id, self._heads)
        return text[key]

    def __repr__(self) -> str:
        text = self._tx.text(self._obj_id, self._heads)
        return f"MutableText({text!r})"

    def splice(self, pos: int, delete_count: int, insert: str) -> None:
        """
        Low-level splice operation - delete characters and insert text at position.

        Args:
            pos: Character position to start the operation
            delete_count: Number of characters to delete
            insert: Text to insert at the position

        Example:
            >>> with doc.change() as d:
            ...     d["text"].splice(0, 5, "Goodbye")  # Replace first 5 chars
        """
        self._tx.splice_text(self._obj_id, pos, delete_count, insert)

    def insert(self, pos: int, text: str) -> None:
        """
        Insert text at the specified position.

        Args:
            pos: Character position where text should be inserted
            text: Text to insert

        Example:
            >>> with doc.change() as d:
            ...     d["text"].insert(5, ", world")
        """
        self._tx.splice_text(self._obj_id, pos, 0, text)

    def delete(self, pos: int, count: int) -> None:
        """
        Delete characters at the specified position.

        Args:
            pos: Character position to start deletion
            count: Number of characters to delete

        Example:
            >>> with doc.change() as d:
            ...     d["text"].delete(5, 7)  # Delete 7 chars starting at position 5
        """
        self._tx.splice_text(self._obj_id, pos, count, "")


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
