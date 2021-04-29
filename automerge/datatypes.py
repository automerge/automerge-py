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

    def get_pred(self, idx):
        return list(self.recent_ops[idx].keys()) if idx < len(self.recent_ops) else []


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
