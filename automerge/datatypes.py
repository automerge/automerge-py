from copy import deepcopy

# TODO: Maybe do this?
# https://stackoverflow.com/questions/6803597/how-to-fake-type-with-python


# TODO: Should probably use multiple inheritance for common attrs like "type"
class Map(dict):
    def __init__(self, iter_, object_id, recent_ops=None, **kwargs):
        super().__init__(iter_, **kwargs)
        if recent_ops is None:
            recent_ops = {}
        # TODO: Do we need type tags?
        self.type = "map"
        self.object_id = object_id
        self.recent_ops = recent_ops

    def get_pred(self, key):
        return list(self.recent_ops[key].keys()) if key in self.recent_ops else []


def create_no_mutation_wrapper(method):
    def wrapper(self, *args, **kwargs):
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
        mutable_methods = [
            "append",
            "extend",
            "insert",
            "pop",
            "remove",
            "reverse",
            "__setitem__",
            "__delitem__",
        ]
        super_attrs = vars(list)
        attrs = vars(self)
        for name in mutable_methods:
            old_method = attrs[name] if name in attrs else super_attrs[name]
            wrapper = create_no_mutation_wrapper(old_method)
            # https://stackoverflow.com/questions/1015307/python-bind-an-unbound-method
            # https://docs.python.org/2/howto/descriptor.html

            # This line "binds" (allows `wrapper` to accept `self`) `wrapper` to the current instance
            bound = wrapper.__get__(self)
            attrs[name] = bound

    def get_pred(self, idx):
        return list(self.recent_ops[idx].keys()) if idx < len(self.recent_ops) else []

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
