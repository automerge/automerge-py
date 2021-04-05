import collections
from typing import List, Any
from .Context import Context


class MapProxy(collections.MutableMapping):

    def __init__(self, doc):
        self.context = Context(doc, doc.actor_id)
        self.object_id = doc.object_id
        self.keys = set()

    def __getitem__(self, key):
        print(f"MapProxy > get item {key} ")
        return self.context.get_object_field(self.object_id, key)

    def __setitem__(self, key, item):
        print(f"MapProxy > set item {key} = {item} ")
        self.context.set_map_key(self.object_id, 'map', key, item)
        self.keys.add(key)

    def __delitem__(self, key):
        # TODO pass the deletion to the context
        self.keys.discard(key)
        pass

    def __iter__(self):
        return iter(self.keys)

    def __len__(self):
        return len(self.keys)


# TODO
class ListProxy(collections.MutableSequence):

    def __init__(self, doc, path):
        self.doc = doc
        self.path = path
