# Extra context on how operations/changes/patches work.
# The change:
# ```
# root = {"a": {"b": 42}}
# ```
# will generate the following operations (ops):
# ```
# [
#   {
#     "action": "makeMap",
#     "obj": "_root",
#     "key": "a",
#     "insert": false,
#     "pred": []
#   },
#   {
#     "action": "set",
#     // This is the op id of the "makeMap" operation
#     // It is also the object id of the object created by that operation
#     "obj": "1@8d5ace3468f240618430325720ae9826",
#     "key": "b",
#     "insert": false,
#     "pred": [],
#     "value": 42
#   }
# ]
# ```
# Notes:
# - The root object of the CRDT is automatically created (no op required)
#   - This means the top-level of the CRDT must always be an object
# - The first operation ever has the id `1@<actor_id>` where seq = 1

# Here's a subset of the patch the backend will return:
#
# ```
#  "diffs": {
#    "objectId": "_root",
#    "type": "map",
#    "props": {
#      "a": {
#        #DESCRIPTION
#        // The op id of the op that creates the empty nested object
#        // that will eventually have b = 42
#        "1@8d5ace3468f240618430325720ae9826": {
#          // This makes it explicit that object ids and op ids are the same
#          "objectId": "1@8d5ace3468f240618430325720ae9826",
#          "type": "map",
#          "props": {
#            "b": {
#              // The op id of the operation that sets b = 42
#              "2@8d5ace3468f240618430325720ae9826": {
#                "value": 42
#  <a bunch of closing braces...>
# ```

from typing import Any, TypedDict

from .apply_patch import apply_patch
from .datatypes import Map, List


class Context:
    def __init__(self, max_op: int, actor_id: str, root_obj: Map):
        self.ops: list[Any] = []
        self.max_op = max_op
        self.actor_id = actor_id
        self.root_obj = root_obj

    def get_value_description(self, val):
        """
        Takes a value and returns an object describing the value (in the format used by patches).

        TODO: This is necessary b/c in apply_patch.py we convert values in a "patch" format
        to an "unwrapped" format (e.g `{value: 3}` becomes 3). Since we are generating a patch
        here, we need to "wrap" the values into patch format. It's slightly unclear why we
        do this wrapping/unwrapping since `recent_ops` is not visible to the user anyways, so
        we could just store straight-up patch data.
        """
        if isinstance(val, Map) or isinstance(val, List):
            return {"objectId": val.object_id, "type": val.type}
        else:
            return {"value": val}

    def get_values_descriptions(self, obj, key):
        """
        Builds the values structure describing a single property in a patch. Finds all
        the values of property `key` of `object` (there might be multiple values in the case
        of a conflict), and returns an object that maps operation IDs to descriptions of values.

        This will return data in the format of #DESCRIPTION in the header comment of this file.
        """
        if False:
            pass
        else:
            recent_ops, values = obj.recent_ops[key], {}
            for (op_id, val) in recent_ops.items():
                values[op_id] = self.get_value_description(val)
            return values

    def get_subpatch(self, patch, path):
        """
        Traverse along `path` into `patch`, creating nodes along the way as needed
        by mutating `patch`. Returns the subpatch at the given path.
        """
        subpatch, obj = patch["diffs"], self.root_obj
        for (key, object_id) in path:
            assert "props" not in subpatch
            subpatch["props"] = {key: self.get_values_descriptions(obj, key)}

            next_op_id, values = None, subpatch["props"][key]
            for op_id in values.keys():
                if op_id == object_id:
                    next_op_id = object_id
                    break

            subpatch = values[next_op_id]
            obj = obj.recent_ops[key][next_op_id]

        assert "props" not in subpatch
        subpatch["props"] = {}
        return subpatch

    def apply_at_path(self, path, init_subpatch):
        """
        Constructs a patch for a change at `path` and then immediately applies
        the patch to the document.

        NOTE: This strategy is inefficient b/c every update inside a change block
        causes a traversal from the root object to the location of the update in order
        to create a subpatch. And the subpatch application is slow as well.
        """
        patch = {"diffs": {"objectId": "_root", "type": "map"}}
        # If the change is `root.foo = "bar"` then
        # - `path` = []
        # - `get_subpatch` will do patch["diffs"]["props"] = {}

        # If change is `root.foo.bar = "baz"` then
        # - `path` = [(foo, "<foo object id>")]
        # - `get_subpatch` will do patch["diffs"]["props"]["foo"][<op_id>]["props"] = {}
        # (creating the entries along the way)
        subpatch = self.get_subpatch(patch, path)
        init_subpatch(subpatch)
        apply_patch(self.root_obj, patch["diffs"])

    def set_value(self, parent_obj_id, key, val, **op_params):
        """
        - Returns a patch that represents creating the Python value `val`
          that is immediately applied to the frontend. This is necessary so the following will work inside a change block:
          ```
          # `val` is {'b': 42}
          foo.a = {'b': 42}
          # `val` is 43
          # nothing has been sent to the backend but we can still access `foo.a.b`
          foo.a.b = 43
          ```
        - Also returns the op id of the op that created `val`
        - Adds the necessary ops to the change context to generate the final change that is sent to the backend

        `parent_obj_id`: The object id of the object that will contain this key/val pair
        `key`: The key to set at the path. In the case of a list, this will be a numerical index.
        `val`: The value to set at the key
        `op_params`: Data that will be in the final op
        """

        if isinstance(val, dict):
            if key == "":
                raise ValueError("The key of a map entry must not be an empty string")
            if hasattr(val, "object_id"):
                raise ValueError(
                    f"Cannot create a reference to existing document object: {val}. Found object_id: {val.object_id}"
                )
            create_obj_op_id = self.add_op(
                action="makeMap", obj=parent_obj_id, key=key, **op_params
            )
            props = {}
            # TODO: Why does the JS front-end sort the keys?
            for child_key in sorted(val.keys()):
                (value_patch, create_child_obj_op_id) = self.set_value(
                    create_obj_op_id, child_key, val[child_key]
                )
                props[child_key] = {create_child_obj_op_id: value_patch}
            # the patch we will immediately apply
            return (
                {"objectId": create_obj_op_id, "type": "map", "props": props},
                create_obj_op_id,
            )
        elif isinstance(val, list):
            edits = []
            props = {}
            create_list_op_id = self.add_op(
                action="makeList", obj=parent_obj_id, key=key, **op_params
            )
            elem_id = "_head"
            for idx, item in enumerate(val):
                # `list_element_op_id` is the elemId of the element we inserted
                # in `set_value`, `elem_id` is the id of the previous list element id
                # since insert ops must provide the elemId of the element after which they inserting
                (value_patch, list_element_op_id) = self.set_value(
                    create_list_op_id, idx, item, insert=True, elemId=elem_id
                )
                props[idx] = {list_element_op_id: value_patch}
                elem_id = list_element_op_id
                # patches use the actual elemId of the list element
                edits.append({"action": "insert", "index": idx, "elemId": elem_id})
            return (
                {
                    "objectId": create_list_op_id,
                    "type": "list",
                    "edits": edits,
                    "props": props,
                },
                create_list_op_id,
            )
        else:
            # it's a primitive
            description = self.get_value_description(val)
            set_value_op_id = self.add_op(
                action="set", obj=parent_obj_id, key=key, **description, **op_params
            )
            return (description, set_value_op_id)

    def add_op(self, **op):
        if "pred" not in op:
            op["pred"] = []
        if "insert" not in op:
            op["insert"] = False
        if "elemId" in op:
            if "key" in op:
                del op["key"]

        self.ops.append(op)
        return f"{self.max_op + len(self.ops)}@{self.actor_id}"
