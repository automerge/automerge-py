from collections.abc import MutableMapping
from .datatypes import Map


class MapProxy(MutableMapping):
    def __init__(self, ctx, assoc_obj, path):
        self.assoc_obj = assoc_obj
        self.ctx = ctx
        # The path is used when mutating values through sets/deletes
        # It is the location of `assoc_obj` in the CRDT's state tree.
        self.path = path

    def __getitem__(self, key):
        val = self.assoc_obj[key]
        return get_maybe_proxy(self.ctx, key, val, self.path)

    def __setitem__(self, key, val):
        if not isinstance(key, str):
            raise Exception("TODO: msg")
        setting_new_value = key not in self.assoc_obj
        # TODO: Do better equals & implement conflict check
        if setting_new_value or self.assoc_obj[key] != val:

            def cb(subpatch):
                preds = self.assoc_obj.get_pred(key)
                (value_patch, op_id) = self.ctx.set_value(
                    self.assoc_obj.object_id, key, val
                )
                subpatch["props"][key] = {op_id: value_patch}

            self.ctx.apply_at_path(self.path, cb)

    def __delitem__(self, key):
        if not isinstance(key, str):
            raise Exception("TODO: msg")
        if key not in self.assoc_obj:
            raise KeyError(key)
        preds = self.assoc_obj.get_pred(key)
        self.ctx.add_op(
            action="del",
            obj=self.assoc_obj.object_id,
            key=key,
            insert=False,
            pred=preds,
        )

        def cb(subpatch):
            subpatch["props"][key] = {}

        self.ctx.apply_at_path(self.path, cb)

    def __iter__(self):
        raise Exception("not yet supported")

    def __len__(self):
        return self.assoc_obj.__len__()


def is_primitive(val):
    return (
        val is None
        or isinstance(val, str)
        or isinstance(val, int)
        or isinstance(val, bool)
    )


def get_maybe_proxy(context, key, val, old_path):
    new_path = old_path + [(key, val.object_id)]
    if isinstance(val, Map):
        return MapProxy(context, val, new_path)
    else:
        if not is_primitive(val):
            raise ValueError(
                f"Value: {val} is not a valid Automerge datatype (str, int, bool, None)"
            )
        # Primitives don't need proxies since you can't mutate them, only re-assign them
        return primitive
