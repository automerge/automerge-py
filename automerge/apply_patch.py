import re
from copy import deepcopy
from typing import NamedTuple
from functools import cmp_to_key

OpId = NamedTuple("OpId", [("counter", int), ("actorId", str)])

OP_ID_RE = re.compile("^(\d+)@(.*)$")


def parse_op_id(op_id):
    match = OP_ID_RE.match(op_id)
    if not match:
        # TODO: proper exception type
        raise Exception(f"Not a valid op_id: {op_id}")
    return OpId(int(match.group(1), match.group(2)))


def lamport_compare(ts1, ts2):
    time1 = parse_op_id(ts1)
    time2 = parse_op_id(ts2)
    if time1.counter < time2.counter:
        return -1
    if time1.counter > time2.counter:
        return 1
    if time1.actorId < time2.actorId:
        return -1
    if time1.actorId > time2.actorId:
        return 1
    return 0


def interpret_patch(patch, obj, updated):
    # Return original object if it already exists and isn't being modified
    # TODO: We don't do an equivalent to the JS `isObject` check here
    if (
        ("props" not in patch)
        and ("edits" not in patch)
        and (patch["objectId"] not in updated)
    ):
        return obj

    if patch["type"] == "map":
        return update_map_object(patch, obj, updated)
    else:
        assert False


def apply_properties(props, obj, conflicts, updated):
    """
    (Description from JS version, commit 81079ff)
    `props` is an object of the form:
    `{key1: {opId1: {...}, opId2: {...}}, key2: {opId3: {...}}}`
    where the outer object is a mapping from property names to inner objects,
    and the inner objects are a mapping from operation ID to sub-patch.
    This function interprets that structure and updates the objects `object` and
    `conflicts` to reflect it. For each key, the greatest opId (by Lamport TS
    order) is chosen as the default resolution; that op's value is assigned
    to `object[key]`. Moreover, all the opIds and values are packed into a
    conflicts object of the form `{opId1: value1, opId2: value2}` and assigned
    to `conflicts[key]`. If there is no conflict, the conflicts object contains
    just a single opId-value mapping.
    """
    pass

    # TODO: I skipped null props check

    for key in props.keys():
        values = {}
        op_ids = props.keys()
        op_ids.sort(key=cmp_to_key(lamport_compare))
        op_ids.reverse()
        for op_id in op_ids:
            subpatch = props[key][op_id]
            if key in conflicts and op_id in conflicts[key][op_id]:
                values[op_id] = get_value(subpatch, conflicts[key][op_id], updated)
            else:
                values[op_id] = get_value(subpatch, None, updated)

        if len(op_ids) == 0:
            del obj[key]
            del conflicts[key]
        else:
            obj[key] = values[op_ids[0]]
            conflicts[key] = values


def get_value(patch, obj, updated):
    if "objectId" in patch:
        # If the objectId of the existing object does not match the objectId in the patch,
        # that means the patch is replacing the object with a new one made from scratch
        if obj and obj.object_id != patch["objectId"]:
            obj = None
        return interpret_patch(patch, obj, updated)
    else:
        # TODO: All the other datatypes
        # Primitive value (number, string, boolean, or null)
        return patch["value"]


def update_map_object(patch, obj, updated):
    object_id = patch["objectId"]
    if object_id not in updated:
        # The JS version has a specialized `cloneMapObj`
        # (I think) that's unnecessary for the Python version b/c
        # `cloneMapObj` has the added property of cloning symbols
        updated[object_id] = deepcopy(obj)
