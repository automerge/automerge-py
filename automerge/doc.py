# Since Automerge operates on JSON-like data structures
# + a few special CRDT datatypes, an Automerge object
# has all its normal data accessible as a dict, e.g, `doc["foo"]`
# Modification of normal data must be done inside a `with` block
# Metadata that the user doesn't care about is stored as actual instance variables
# e.g `doc.actorId`

# abc = Abstract Base Class. Allows us to create our
# own class that simulates being a dict
# https://stackoverflow.com/questions/3387691/
from collections.abc import MutableMapping
import time
import uuid

from .proxies import MapProxy
from .context import Context


class Doc(MutableMapping):
    def __init__(self, actor_id=None, start_data=None):
        self.change = None
        self.root_proxy = None
        self.actor_id = actor_id if actor_id else uuid.uuid4()

        self.object_id = "_root"
        self.cache = {"_root": self}

        # corresponds to state in JS implementation
        self.seq = 0
        self.max_op = 0
        self.requests = []
        self.clock = {}
        self.deps = []

        # Assign initial state
        start_data = start_data if start_data else {}
        self.__start_change_block()
        for (key, value) in start_data.items():
            self.root_proxy[key] = value
        self.__finish_change_block()

    def __enter__(self):
        self.__start_change_block()
        return self.root_proxy

    def __start_change_block(self):
        if self.root_proxy:
            # TODO: Specific exception
            raise Exception("Cannot change doc while already in change")
        ctx = Context(self.actor_id, 0, self.cache)
        self.root_proxy = MapProxy(ctx, "_root", [])

    # https://book.pythontips.com/en/latest/context_managers.html
    def __exit__(self, typ, value, traceback):
        self.__finish_change_block()

        # There was an exception inside the with block.
        # Don't generate a patch.
        if traceback:
            assert typ and value and traceback
            return

    def __finish_change_block(self):
        assert self.root_proxy is not None
        ctx = self.root_proxy.context
        self.root_proxy = None

        if len(ctx.updated.keys()) > 0:
            self.__make_change(ctx)

    def __make_change(self, context):
        actor = self.actor_id
        if not actor:
            raise Exception("init actor id")

        # TODO: JS version clones state here. Why?
        self.seq += 1
        # TODO: Unclear if this gives UTC always
        t = time.time()
        change = {
            "actor": actor,
            "seq": self.seq,
            "startOp": self.max_op + 1,
            "deps": self.deps,
            "time": t,
            "message": "",
            "ops": context.ops,
        }

        if False:
            pass
        else:
            queued_request = {"actor": actor, "seq": change["seq"], "before": self}
            self.requests.append(queued_request)
            self.max_op = self.max_op + len(change["ops"])
            self.deps = []
            self.__update_self(context.updated)
            self.change = change

    def __update_self(self, updated):
        self.cache = updated
        for object_id in self.cache.keys():
            if object_id not in updated:
                updated[object_id] = self.cache[object_id]

    def __getitem__(self, key):
        pass

    def __delitem__(self, key):
        pass

    def __setitem__(self, key, value):
        pass

    def __iter__(self):
        pass

    def __len__(self):
        pass
