from .automerge import new_backend, new_frontend
import collections
from typing import List, Any


class Doc(collections.MutableMapping):
    changing: bool
    changes: List[Any]
    backend: Any
    frontend: Any

    def __init__(self):
        self.changing = False
        self.changes = []
        self.frontend = new_frontend()
        self.backend = new_backend()

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
        path = [key]
        value = self.frontend.value_at_path([key])
        if value == "map":
            return MapProxy(self, path)
        elif value == "list":
            return ListProxy(self, path)
        return value

    def __setitem__(self, key, item):
        pass

    def __delitem__(self, key):
        pass

    def __iter__(self):
        MapProxy(self, []).__iter__()

    def __len__(self):
        pass


class MapProxy(collections.MutableMapping):

    def __init__(self, doc, path):
        self.doc = doc
        self.path = path

    def __getitem__(self, key):
        pass

    def __setitem__(self, key, item):
        if self.doc.changing:
            self.doc.frontend
        pass

    def __delitem__(self, key):
        pass

    def __iter__(self):
        pass

    def __len__(self):
        pass


class ListProxy(collections.MutableSequence):

    def __init__(self, doc, path):
        self.doc = doc
        self.path = path
