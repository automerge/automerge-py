# TODO: Maybe do this?
# https://stackoverflow.com/questions/6803597/how-to-fake-type-with-python


# TODO: Should probably use multiple inheritance for common attrs like "type"
class Map(dict):
    def __init__(self, iter_, object_id, recent_ops=None, **kwargs):
        super().__init__(iter_, **kwargs)
        if recent_ops is None:
            recent_ops = {}
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
        # TODO
        return []
