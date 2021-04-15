from functools import cmp_to_key
from typing import NamedTuple
import re
from .datatypes import Map

OpId = NamedTuple("OpId", [("counter", int), ("actorId", str)])
OP_ID_RE = re.compile("^(\d+)@(.*)$")


def parse_op_id(op_id):
    match = OP_ID_RE.match(op_id)
    if not match:
        # TODO: proper exception type
        raise Exception(f"Not a valid op_id: {op_id}")
    return OpId(int(match.group(1), match.group(2)))


def lamport_compare(ts1, ts2):
    """
    Compares two strings, interpreted as Lamport timestamps of the form
    'counter@actorId'. Returns a postive integer if ts1 is greater, or a negative integer if ts2 is greater.
    """
    time1 = parse_op_id(ts1)
    time2 = parse_op_id(ts2)
    if time1.counter != time2.counter:
        return time1.counter - time2.counter
    if time1.actorId != time2.actorId:
        return time1.actorId - time2.actorId
    return 0


def get_value(conflict, patch):
    if "objectId" in patch:
        if conflict and conflict.object_id != patch["objectId"]:
            # if the object ids are different then the patch is
            # replacing the object with a new one made from scratch
            conflict = Map([], patch["objectId"])
        return apply_patch(conflict, patch)
    elif "datatype" in patch:
        pass
    else:
        # primitive (number, string, boolean, null)
        return patch["value"]


def apply_properties(obj, props):
    recent_ops = obj.recent_ops
    for key, val in props.items():
        values, op_ids = {}, list(val.keys())
        op_ids.sort(key=cmp_to_key(lamport_compare))
        op_ids.reverse()

        for op_id in op_ids:
            subpatch = val[op_id]
            if key in recent_ops and op_id in recent_ops[key]:
                # Oh another question :stuck_out_tongue: , I'm trying to understand when the first case of this if statement would be taken.
                # Theoretically, there should never be 2 ops with the same id right? Since actor ids are supposed to be unique.
                # So not sure when a patch would have an op id that would already exist in an object's conflicts.
                values[op_id] = get_value(recent_ops[key][op_id], subpatch)
            else:
                values[op_id] = get_value(Map([], op_id), subpatch)

        if len(op_ids) == 0:
            # an empty subpatch signals "delete"
            del obj[key]
            del recent_ops[key]
        else:
            obj[key] = values[op_ids[0]]
            recent_ops[key] = values


def apply_patch(obj, patch):
    if obj is not None and "props" not in patch and "edits" not in patch:
        # TODO: null check?
        return obj

    if patch["type"] == "map":
        apply_properties(obj, patch["props"])
        return obj
    else:
        raise Exception(f"Unknown object type in patch: {patch['type']}")
