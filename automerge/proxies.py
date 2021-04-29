import math
from collections.abc import MutableMapping, MutableSequence
from .datatypes import Map, List, Counter


class MapProxy(MutableMapping):
    def __init__(self, ctx, assoc_obj, path):
        self.assoc_obj = assoc_obj
        self.ctx = ctx
        # The path is used when mutating values through sets/deletes
        # It is the location of `assoc_obj` in the CRDT's state tree.
        self.path = path

    def __getitem__(self, key):
        val = self.assoc_obj[key]
        return get_maybe_proxy(self.assoc_obj, self.ctx, key, val, self.path)

    def __setitem__(self, key, val):
        if not isinstance(key, str):
            raise Exception("TODO: msg")
        setting_new_value = key not in self.assoc_obj
        if not setting_new_value and isinstance(self.assoc_obj[key], Counter):
            return

        # TODO: Do better equals & implement conflict check
        if setting_new_value or self.assoc_obj[key] != val:

            def cb(subpatch):
                preds = self.assoc_obj.get_pred(key)
                (value_patch, op_id) = self.ctx.set_value(
                    self.assoc_obj.object_id, key, val, pred=preds
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
        return self.assoc_obj.__iter__()

    def __len__(self):
        return self.assoc_obj.__len__()


class ListProxy(MutableSequence):
    def __init__(self, ctx, assoc_list, path):
        self.ctx = ctx
        self.assoc_list = assoc_list
        self.path = path

    def __getitem__(self, idx):
        val = self.assoc_list[idx]
        return get_maybe_proxy(self.assoc_list, self.ctx, idx, val, self.path)

    def __delitem__(self, idx):
        if idx < 0 or idx >= len(self.assoc_list):
            raise IndexError(idx)
        elem_id = self.assoc_list.elem_ids[idx]
        preds = self.assoc_list.get_pred(idx)

        self.ctx.add_op(
            action="del",
            obj=self.assoc_list.object_id,
            elemId=elem_id,
            insert=False,
            pred=preds,
        )

        def cb(subpatch):
            subpatch["edits"] = [{"action": "remove", "index": idx}]

        self.ctx.apply_at_path(self.path, cb)

    def __len__(self):
        return self.assoc_list.__len__()

    def __setitem__(self, idx, val):
        if idx < 0 or idx >= len(self.assoc_list):
            raise IndexError(idx)

        if isinstance(self.assoc_list[idx], Counter):
            return

        # TODO: check conflicts
        if self.assoc_list[idx] != val:
            elem_id = self.assoc_list.elem_ids[idx]
            preds = self.assoc_list.get_pred(idx)

            def cb(subpatch):
                (value_patch, op_id) = self.ctx.set_value(
                    self.assoc_list.object_id,
                    idx,
                    val,
                    elemId=elem_id,
                    insert=False,
                    pred=preds,
                )
                subpatch["props"][idx] = {op_id: value_patch}
                subpatch["edits"] = []

            self.ctx.apply_at_path(self.path, cb)

    # insert an element before the given idx
    # if idx >= len(self), insert at the end
    # if idx <= 0, insert at the start
    def insert(self, idx, val):
        slen = len(self)
        if idx > slen:
            idx = slen
        elif idx < 0:
            idx = 0
        # in the change format, when inserting, we give the `elemId` of the element *after*
        # which we are inserting. (If we are inserting at the first element, we use "_head")
        elem_id = "_head" if idx == 0 else self.assoc_list.elem_ids[idx - 1]
        preds = self.assoc_list.get_pred(idx)

        def cb(subpatch):
            (value_patch, op_id) = self.ctx.set_value(
                self.assoc_list.object_id,
                idx,
                val,
                elemId=elem_id,
                insert=True,
                pred=preds,
            )
            subpatch["props"][idx] = {op_id: value_patch}
            subpatch["edits"] = [{"action": "insert", "index": idx, "elemId": op_id}]

        self.ctx.apply_at_path(self.path, cb)


class CounterProxy(int):
    def __new__(cls, value, ctx, assoc_parent_obj, path, key):
        assert isinstance(value, int)
        v = super(cls, cls).__new__(cls, value)
        v.ctx = ctx
        v.assoc_parent_obj = assoc_parent_obj
        v.path = path
        v.key = key
        return v

    def __add__(self, delta):
        res = int(super(CounterProxy, self).__add__(delta))
        parent_obj_id = self.assoc_parent_obj.object_id
        assert isinstance(self.assoc_parent_obj[self.key], Counter)
        preds = self.assoc_parent_obj.get_pred(self.key)
        elem_id = {}
        if isinstance(self.assoc_parent_obj, List):
            elem_id["elemId"] = self.assoc_parent_obj.elem_ids[self.key]
        op_id = self.ctx.add_op(
            action="inc",
            obj=parent_obj_id,
            key=self.key,
            value=delta,
            insert=False,
            pred=preds,
            **elem_id,
        )

        def cb(subpatch):
            subpatch["props"][self.key] = {op_id: {"value": res, "datatype": "counter"}}
            if isinstance(self.assoc_parent_obj, List):
                subpatch["edits"] = []

        self.ctx.apply_at_path(self.path, cb)
        return self.__class__(res, self.ctx, self.assoc_parent_obj, self.path, self.key)

    def __sub__(self, other):
        return self.__add__(-other)

    def __mul__(self, other):
        raise Exception("Counters only support add/subtract")

    def __div__(self, other):
        raise Exception("Counters only support add/subtract")

    def __str__(self):
        return f"{int(self)}"

    def __repr__(self):
        return f"CounterProxy({int(self)})"


def is_primitive(val):
    return (
        val is None
        or isinstance(val, str)
        or isinstance(val, int)
        or isinstance(val, bool)
    )


def get_maybe_proxy(parent_obj, context, key, val, old_path):
    if isinstance(val, Counter):
        return CounterProxy(val, context, parent_obj, old_path, key)
    new_path = old_path + [(key, val.object_id)]
    if isinstance(val, Map):
        return MapProxy(context, val, new_path)
    elif isinstance(val, List):
        return ListProxy(context, val, new_path)
    else:
        if not is_primitive(val):
            raise ValueError(
                f"Value: {val} is not a valid Automerge datatype (str, int, bool, None, Counter)"
            )
        # Primitives don't need proxies since you can't mutate them, only re-assign them
        return primitive
