from typing import NamedTuple
from .proxies import MapProxy
from .data import Map
from .apply_patch import interpret_patch

PathFragment = NamedTuple("PathFragment", [("object_id", str), ("key", str)])


class Context:
    def __init__(self, actor_id, max_op, cache):
        self.actor_id = actor_id
        self.max_op = max_op
        self.cache = cache
        self.ops = []
        self.updated = {}

    def add_op(self, op):
        self.ops.append(op)

    def next_op_id(self):
        return f"{self.max_op + len(self.ops) + 1}@{self.actor_id}"

    def get_object(self, object_id):
        if object_id in self.updated:
            return self.updated[object_id]
        elif object_id in self.cache:
            return self.cache[object_id]
        else:
            # TODO: Use specific exception type
            raise Exception(f"Target object does not exist: {object_id}")

    def get_object_field(self, path, object_id, key):
        """
        Returns the value associated with the property named `key` on
        the object at path `path`. If the value is an object, returns a
        proxy (JS terminology) for it.

        @param path: Used to construct the new proxy's (if it exists) child path.
        @param object_id: Used to retrieve the raw object
        @param key: Used to index into the raw object
        """
        # TODO: Use TypeGuard
        #  https://typeguard.readthedocs.io/en/latest/index.html
        if not isinstance(key, str) and not isinstance(key, int):
            raise ValueError("Expect key to be string or int")

        obj = self.get_object(object_id)
        child = obj[key]
        if False:
            # TODO: Counter
            pass
        elif isinstance(child, Map):
            child_id = child.object_id
            subpath = path + [PathFragment(object_id=child_id, key=key)]
            self.instantiate_proxy(subpath, child_id)
        else:
            pass

    def instantiate_proxy(self, path, object_id, readonly=None):
        obj = self.get_object(object_id)
        if False:
            # TODO: Array
            pass
        elif False:
            # TODO: Text & Table
            pass
        else:
            return MapProxy(self, object_id, path, readonly)

    def set_map_key(self, path, key, value):
        if not isinstance(key, str):
            # TODO: Specific error type
            raise Exception(f"The key of a map entry must be a string, not {type(key)}")

        object_id = "_root" if len(path) == 0 else path[-1].object_id
        obj = self.get_object(object_id)

        # TODO: Counter

        # TODO: conflict check
        if (key not in obj) or obj[key] != value:

            def callback(subpatch):
                # pred = get_pred(obj, key)
                pred = []
                op_id = self.next_op_id()
                value_patch = self.set_value(object_id, key, value, False, pred)
                subpatch["props"][key] = {op_id: value_patch}

            self.apply_at_path(path, callback)

    def set_value(self, object_id, key, value, insert, pred, elem_id=None):
        if isinstance(value, dict):
            return self.create_nested_objects(
                object_id, key, value, insert, pred, elem_id
            )
        else:
            op = {
                "action": "set",
                "obj": object_id,
                "insert": insert,
                "pred": pred,
            }
            if elem_id:
                op[elem_id] = elem_id
            description = self.get_value_description(value)
            op = {**op, **description}
            self.add_op(op)
            return description

    @staticmethod
    def create_op(action, obj, insert, pred, key=None, elemId=None):
        op = {"action": action, "obj": obj, "insert": insert, "pred": pred}
        if elemId:
            op["elemId"] = elemId
        if key:
            op["key"] = key
        return op

    def create_nested_objects(self, obj, key, value, insert, pred, elemId=None):
        if isinstance(obj, object):
            # TODO: Correct type check
            # TODO: Specific Exception type
            raise Exception(
                f"Cannot create reference to an existing document object with id: {obj.object_id}"
            )

        object_id = self.next_op_id()
        if isinstance(value, dict):
            self.add_op(Context.create_op("makeMap", obj, insert, pred, key, elemId))
            props = {}
            for nested in sorted(value.keys()):
                op_id = self.next_op_id()
                value_patch = self.set_value(
                    object_id, nested, value[nested], False, []
                )
                props[nested] = {op_id: value_patch}
            return {"objectId": object_id, "type": "map", props: props}
        else:
            raise NotImplementedError()

    def get_value_description(self, value):
        if isinstance(value, dict):
            obj_id = value.object_id
            # TODO: getObjectType
            return {"objectId": obj_id, "type": "foobar"}
        else:
            return {"value": value}

    def apply_at_path(self, path, callback):
        patch = {"diffs": {"objectId": "_root", "type": "map"}}
        callback(self.get_subpatch(patch, path))
        interpret_patch(patch["diffs"], self.cache["_root"], self.updated)

    def get_subpatch(self, patch, path):
        subpatch = patch["diffs"]
        obj = self.get_object("_root")

        if not "props" in subpatch:
            subpatch["props"] = {}

        for path_elem in path:
            if path_elem.key not in subpatch.props:
                subpatch.props[path_elem.key] = self.get_values_descriptions(
                    path, obj, path_elem.key
                )

            next_op_id, values = None, subpatch.props[path_elem.key]
            for op_id in values.keys():
                if values[op_id]["objectId"] == path_elem.object_id:
                    next_op_id = op_id

            if not next_op_id:
                # TODO: Proper exception type
                raise Exception(
                    f"Cannot find path object with object_id {path_elem.object_id}"
                )

            subpatch = values[next_op_id]
            obj = self.get_property_value(obj, path_elem.key, next_op_id)
        return subpatch

    def get_property_value(self, obj, key, op_id):
        if False:
            # TODO: Table
            pass
        elif False:
            # TODO: Text
            pass
        else:
            return obj.conflicts[key][op_id]

    def get_values_descriptions(self, path, obj, key):
        if False:
            # TODO: Table
            pass
        elif False:
            # TODO: Text
            pass
        else:
            # Map or List objects
            conflicts = obj.conflicts[key]
            values = {}
            if not conflicts:
                raise Exception(f"No children at key {key} of path {path}")
            for op_id in conflicts.keys():
                values[op_id] = self.get_value_description(conflicts[op_id])
            return values
