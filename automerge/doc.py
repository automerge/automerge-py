from typing import Optional, Any

import uuid
from collections.abc import MutableMapping
from .proxies import MapProxy
from .context import Context
from .apply_patch import apply_patch
from .datatypes import Map


class Doc(MutableMapping):
    def __init__(
        self,
        actor_id: Optional[str] = None,
        initial_data: Optional[dict[Any, Any]] = None,
    ) -> None:
        if actor_id is None:
            # QUESTION: Why do we remove "-"?
            actor_id = str(uuid.uuid4()).replace("-", "")

        self.actor_id = actor_id
        self.ctx = None
        self.seq = 0
        self.max_op = 0
        self.changes: list[Any] = []

        self.root_obj = Map([], "_root", {})
        if initial_data:
            with self as d:
                for (k, v) in initial_data.items():
                    d[k] = v

    def apply_patch(self, patch):
        self.root_obj = apply_patch(self.root_obj, patch["diffs"])

    def get_recent_ops(self, path):
        temp = self.root_obj
        for segment in path[:-1]:
            temp = temp[segment]
        return temp.recent_ops[path[-1]]

    def __getitem__(self, key):
        return self.root_obj[key]

    def __delitem__(self, key):
        raise Exception(
            f"Cannot delete directly on a document. Use a change block. (Tried deleting {key})"
        )

    def __iter__(self):
        return self.root_obj.__iter__()

    def __len__(self):
        return self.root_obj.__len__()

    def __setitem__(self, key, val):
        raise Exception(
            f"Cannot assign directly on a document. Use a change block. (Tried assigning {key} to {val})"
        )

    def __enter__(self):
        self.ctx = Context(0, self.actor_id, self.root_obj)
        return MapProxy(self.ctx, self.root_obj, [])

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.seq += 1
        change = {
            "actor": self.actor_id,
            "seq": self.seq,
            "startOp": self.max_op + 1,
            "deps": [],
            "ops": self.ctx.ops,
            "time": 12345,
            "message": "",
        }
        self.max_op = self.max_op + len(self.ctx.ops)
        self.changes.append(change)
