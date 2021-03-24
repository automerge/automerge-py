from .automerge import new_backend, new_frontend

from .Text import Text
from .Counter import Counter


from .Proxies import MapProxy, ListProxy


import collections
from typing import List, Any, Dict

import uuid


class Doc(collections.MutableMapping):

    actor_id: str
    object_id = '00000000-0000-0000-0000-000000000000'

    root_object_proxy: Any = None
    changing: bool = False
    changes: List[Any] = []
    cache: Dict = {}
    conflicts: Dict = {}
    inbound: Dict = {}

    state: Dict = {"seq": 0,
                   "requests": [],
                   "deps": {},
                   "can_undo": False,
                   "can_redo": False}

    backend: Any

    def __init__(self):
        self.actor_id = uuid.uuid4()
        self.backend = new_backend()

        self.cache[self.object_id] = self

    def apply_changes(self, changes):
        pass

    def __enter__(self):
        if self.changing:
            raise Exception(
                "Attempting to modify a document you are already modifying"
            )
        self.changing = True

        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.changing = False
        return False

    def __getitem__(self, key):
        print(f"Doc > get item {key}  ")

        return self.root_object_proxy[key]

        # path = [key]
        # # value = self.frontend.value_at_path([key])
        # if value == "map":
        #     return MapProxy(self, path)
        # elif value == "list":
        #     return ListProxy(self, path)
        # return value

    def __setitem__(self, key, item):
        print(f"Doc > set item {key} = {item} ")

        # Instead of using the rust frontend,
        # we want to store the item python-side
        # "apply that change to your python frontend"

        if not self.changing:
            raise Exception(
                "Attempting to modify a document prior to its opening for modification"
            )

        if self.root_object_proxy is None:
            self.root_object_proxy = MapProxy(self)

        self.root_object_proxy[key] = item

        # change = self.frontend.set_at_path(path, item)
        # self.changes.append(change)

    def __delitem__(self, key):
        pass

    def __iter__(self):
        MapProxy(self, []).__iter__()

    def __len__(self):
        pass
