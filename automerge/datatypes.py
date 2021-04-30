from copy import deepcopy

# TODO: Maybe do this?
# https://stackoverflow.com/questions/6803597/how-to-fake-type-with-python


# TODO: Should probably use multiple inheritance for common attrs like "type"
class Map(dict):
    def __init__(self, iter_, object_id, recent_ops=None, **kwargs):
        super().__init__(iter_, **kwargs)
        if recent_ops is None:
            recent_ops = {}
        # TODO: Do we need type tags? (probably not)
        self.type = "map"
        self.object_id = object_id
        self.recent_ops = recent_ops
        self._frozen = False
        mutable_methods = [
            # TODO: Why can't we override operator methods using
            # this approach?
            # "__setitem__",
            # "__delitem__",
            "clear",
            "pop",
            "popitem",
            "setdefault",
            "update",
        ]
        override_mutable_methods(self, dict, mutable_methods)

    def get_pred(self, key):
        return list(self.recent_ops[key].keys()) if key in self.recent_ops else []

    def __setitem__(self, key, value):
        if self._frozen == True:
            raise Exception(f"Cannot set item on {self} outside of change block")
        return dict.__setitem__(self, key, value)

    def __delitem__(self, key):
        if self._frozen == True:
            raise Exception(f"Cannot delete item on {self} outside of change block")
        return dict.__delitem__(self, key)

    def __deepcopy__(self, memo):
        kvs = []
        for k, v in self.items():
            kvs.append(deepcopy((k, v)))
        copy = Map(
            kvs,
            deepcopy(self.object_id),
            deepcopy(self.recent_ops),
        )
        copy._frozen = self._frozen
        return copy


def override_mutable_methods(instance, super_class, names):
    super_attrs = vars(super_class)
    attrs = vars(instance)
    for name in names:
        old_method = attrs[name] if name in attrs else super_attrs[name]
        wrapper = create_no_mutation_wrapper(old_method)
        # https://stackoverflow.com/questions/1015307/python-bind-an-unbound-method
        # https://docs.python.org/2/howto/descriptor.html

        # This line "binds" (allows `wrapper` to accept `self`) `wrapper` to the current instance
        bound = wrapper.__get__(instance)
        attrs[name] = bound


def create_no_mutation_wrapper(method):
    def wrapper(self, *args, **kwargs):
        print("calling: " + method.__name__)
        if self._frozen == True:
            raise Exception(
                f'Cannot call mutating method: "{method.__name__}" on {self} outside of change block'
            )
        return method(self, *args, *kwargs)

    return wrapper


class List(list):
    def __init__(self, iter_, object_id, elem_ids=None, recent_ops=None, **kwargs):
        super().__init__(iter_, **kwargs)
        if elem_ids is None:
            elem_ids = []
        if recent_ops is None:
            recent_ops = []
        self.type = "list"
        self.object_id = object_id
        self.elem_ids = elem_ids
        self.recent_ops = recent_ops
        self._frozen = False

        # Taken from here: https://www.w3schools.com/python/python_ref_list.asp
        # Might not need to override *every* mutable method since (maybe), for example, `append` is implemented in terms of `insert`
        # But I suspect not since Python devs probably want `list` to be as fast as possible, which means inlining code?
        # (this comment also applies to the Map class)
        mutable_methods = [
            # "__setitem__",
            # "__delitem__",
            "append",
            "extend",
            "insert",
            "pop",
            "remove",
            "reverse",
        ]
        override_mutable_methods(self, list, mutable_methods)

    def get_pred(self, idx):
        return list(self.recent_ops[idx].keys()) if idx < len(self.recent_ops) else []

    def __setitem__(self, idx, value):
        if self._frozen == True:
            raise Exception(f"Cannot set item on {self} outside of change block")
        return list.__setitem__(self, idx, value)

    def __delitem__(self, key):
        if self._frozen == True:
            raise Exception(f"Cannot delete item on {self} outside of change block")
        return list.__delitem__(self, key)

    def __deepcopy__(self, memo):
        storage = []
        for val in self:
            storage.append(deepcopy(val))
        copy = List(
            storage,
            deepcopy(self.object_id),
            deepcopy(self.elem_ids),
            deepcopy(self.recent_ops),
        )
        copy._frozen = self._frozen
        return copy


# this is just a wrapper around ints to signal that we should use CounterProxy
class Counter(int):
    def __new__(cls, value, *args, **kwargs):
        return super(cls, cls).__new__(cls, value)

    # @staticmethod
    # def deny():
    #     raise Exception(f"Can only change counter inside change block through +=/-=")

    # def __add__(self, other):
    #     Counter.deny()

    # def __sub__(self, other):
    #     Counter.deny()

    # def __mul__(self, other):
    #     Counter.deny()

    # def __div__(self, other):
    #     Counter.deny()

    def __str__(self):
        return f"{int(self)}"

    def __repr__(self):
        return f"Counter({int(self)})"
