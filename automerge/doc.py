import uuid
from collections.abc import MutableMapping
from .proxies import MapProxy
from .context import Context
from .datatypes import Map


class Doc(MutableMapping):
    def __init__(self, actor_id=None, initial_data=None):
        if actor_id is None:
            # QUESTION: Why do we remove "-"?
            actor_id = str(uuid.uuid4()).replace("-", "")

        self.actor_id = actor_id
        self.ctx = None
        self.seq = 0
        self.max_op = 0

        self.root_obj = Map([], "_root", {})
        if initial_data:
            with self as d:
                for (k, v) in initial_data.items():
                    d[k] = v

    def __getitem__(self, key):
        return self.root_obj[key]

    def __delitem__(self, key):
        pass

    def __iter__(self):
        return self.root_obj.__iter__()

    def __len__(self):
        pass

    def __setitem__(self, key, val):
        pass

    def __enter__(self):
        self.ctx = Context(0, self.actor_id, self.root_obj)
        return MapProxy(self.ctx, self.root_obj, [])

    def __exit__(self, exc_type, exc_val, exc_tb):
        # print(self.ctx.ops)
        pass

    def change(self):
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
        return change


# return proxies
# proxies are just wrappers around the current object
