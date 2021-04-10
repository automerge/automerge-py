# The JS version of Automerge returns data
# as plain-old-Javascript-objects (POJOs) with the
# exception of 2 symbol keys (`OBJECT_ID` and `CONFLICTS`)

# We might need to do this:
# https://stackoverflow.com/questions/6803597/how-to-fake-type-with-python
# Because we want all Python POJO-equivalents to be of the same type
# even though thye inherit from different base types


class Map(dict):
    __slots__ = ["object_id", "conflicts"]

    def __init__(self, iter_, object_id, conflicts, **kwargs):
        super().__init__(iter_, **kwargs)
        self.object_id = object_id
        self.conflicts = conflicts
