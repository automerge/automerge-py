from collections.abc import MutableMapping


class MapProxy(MutableMapping):
    def __init__(self, context, object_id, path, readonly=None):
        self.context = context
        self.object_id = object_id
        self.path = path
        self.readonly = readonly

    def __getitem__(self, key):
        # As of commit 81079ff, the JS version has 3 checks
        # for key === <A JS Symbol Object>.
        # JS needs that to distinguish between user data
        # and CRDencode_changeT metadata. (CRDT metadata is stored behind Symbols)
        # We don't need those b/c all user data will be accessed
        # through dict-style indexing (implemented with __getitem__)
        # whereas CRDT metadata can be accessed as normal class fields
        return self.context.get_object_field(self.path, self.object_id, key)

    def __setitem__(self, key, value):
        # TODO: readonly check
        self.context.set_map_key(self.path, key, value)

    def __delitem__(self, key):
        pass

    def __iter__(self):
        pass

    def __len__(self):
        pass
