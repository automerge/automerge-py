import copy
from .Text import Text
from .Counter import Counter

import re


def parse_elem_id(elem_id):
    '''
    Takes a string in the form that is used to identify list elements (an actor
        ID concatenated with a counter, separated by a colon) and returns a dict `{'counter':counter, 'actor_id' : actor_id}`.
    '''

    if not re.match(r'^(.*):(\d+)$', elem_id):
        raise Exception(f"Not a valid elemId: {elem_id}")

    else:

        return {"counter": int(elem_id.split(':')[1]),
                "actor_id": elem_id.split(':')[0]}


def get_value(diff, cache, updated):
    '''
    Reconstructs the value from the diff object `diff`.
    '''
    if 'link' in diff and diff['link']:
        # Reference to another object; fetch it from the cache
        if diff['value'] in updated:
            return updated[diff['value']]
        elif diff['value'] in cache:
            return cache[diff['value']]
        else:
            raise Exception(
                f"Value missing from cache and updated : {diff['value']}")

    elif "datatype" in diff and diff["datatype"] == 'timestamp':
        # TODO
        # return new Date(diff.value)
        pass
    elif "datatype" in diff and diff["datatype"] == 'counter':
        return Counter(diff['value'])
    elif "datatype" in diff:
        raise Exception(f'Unknown datatype: {diff["datatype"]}')
    else:
        # Primitive value(number, string, boolean, or null)
        return diff['value']


def apply_diffs(diffs, cache, updated, inbound):

    start_idx = 0
    for end_idx in range(len(diffs)):

        diff = diffs[end_idx]

        if diff['type'] == 'map':
            update_map_object(diff, cache, updated, inbound)
            start_idx = end_idx + 1
        elif diff['type'] == 'table':
            # TODO
            # updateTableObject(diff, cache, updated, inbound)
            start_idx = end_idx + 1
        elif diff['type'] == 'list':
            # TODO
            # updateListObject(diff, cache, updated, inbound)
            start_idx = end_idx + 1
        elif diff['type'] == 'text':

            if end_idx == len(diffs)-1 or diffs[end_idx+1]['object_id'] != diff['object_id']:
                update_text_object(diffs, start_idx, end_idx, cache, updated)
                start_idx = end_idx + 1
        else:
            # TODO Error handling
            # throw new TypeError(`Unknown object type: ${diff.type}`)
            pass


def clone_map_object(original_object, object_id):
    '''
    Creates a writable copy of an immutable map object. If `original_object`
    is undefined, creates an empty object with ID `object_id`.
    '''

    if original_object is not None and original_object.object_id != object_id:
        raise Exception(
            f'cloneMapObject ID mismatch: {original_object.object_id} != {object_id}')

    # Don't copy the whole object, which is supposed to be a Proxy.
    # We only want its dict nature.

    # obj = dict(original_object)
    conflicts = copy.copy(
        original_object.conflicts) if original_object is not None else None

    return {"conflicts": conflicts, "object_id": object_id}
    obj.conflicts = conflicts
    obj.object_id = object_id

    return obj


def child_references(obj, key):
    '''
    Finds the object IDs of all child objects referenced under the key `key` of
    `object` (both `object[key]` and any conflicts under that key). Returns a map
    from those objectIds to the value `true`.
    '''

    refs = {}
    # conflicts = obj.conflicts[key] if key in obj.conflicts else {}
    conflicts = obj['conflicts'][key] if key in obj['conflicts'] else {}

    children = [obj[key] if key in obj else None] + list(conflicts.values())

    for c in children:
        if c is not None and c.object_id is not None:
            refs[c.object_id] = True

    return refs


def update_inbound(object_id, refs_before, refs_after, inbound):
    '''
    Updates `inbound` (a mapping from each child object ID to its parent) based
    on a change to the object with ID `object_id`. `refs_before` and `refs_after`
    are objects produced by the `childReferences()` function, containing the IDs
    of child objects before and after the change, respectively.
    '''

    for ref in refs_before.keys():
        if ref not in refs_after:
            del inbound[ref]

    for ref in refs_after.keys():
        if ref in inbound and inbound[ref] != object_id:
            raise Exception(f'Object {ref} has multiple parents')
        elif ref not in inbound or not inbound[ref]:
            inbound[ref] = object_id


def update_map_object(diff, cache, updated, inbound):
    '''
    Applies the change `diff` to a map object. `cache` and `updated` are indexed
    by objectId; the existing read-only object is taken from `cache`, and the
    updated writable object is written to `updated`. `inbound` is a mapping from
    child objectId to parent objectId; it is updated according to the change.
    '''

    object_id = diff['object_id']
    if object_id not in updated:
        print(f"DID CLONE {object_id}")
        updated[object_id] = clone_map_object(cache[object_id],  object_id)

    obj = updated[object_id]
    conflicts = obj['conflicts']
    refs_before = {}
    refs_after = {}
    key = diff['key']

    if diff['action'] == 'create':
        # do nothing
        pass
    elif diff['action'] == 'set':

        refs_before = child_references(obj, key)
        new_value = get_value(diff, cache, updated)

        obj[key] = new_value

        if 'conflicts' in diff:
            conflicts[key] = {}
            for c in diff['conflicts']:
                conflicts[key][c.actor] = get_value(c, cache, updated)
            # In the JS version, this object is frozen :
            # Object.freeze(conflicts[diff.key])
            # Should
            # conflicts[key].freeze()
        elif key in conflicts:
            del conflicts[key]

        refs_after = child_references(obj, key)
    elif diff['action'] == 'remove':
        refs_before = child_references(obj, key)
        del obj[key]
        del conflicts[key]
    else:
        raise Exception(f"Unknown action type: {diff['action']}")

    update_inbound(diff['object_id'], refs_before, refs_after, inbound)


def update_text_object(diffs, start_idx, end_idx, cache, updated):
    '''
    Applies the list of changes from `diffs[start_idx]` to `diffs[end_idx]`
    (inclusive the last element) to a Text object. `cache` and `updated` are
    indexed by objectId; the existing read-only object is taken from `cache`,
    and the updated object is written to `updated`.
    '''

    # print("ApplyPatch > update text object ", start_idx, diffs, cache, updated)
    object_id = diffs[start_idx]['object_id']

    # if the object has not been updated before,
    # let's add a copy into the `updated` dict.
    # If the object has actually not been created yet,
    # create one.
    if object_id not in updated:
        if object_id in cache:
            updated[object_id] = cache[object_id].copy()
        else:
            updated[object_id] = Text('', object_id=object_id)

    elems = updated[object_id].elems
    max_elem = updated[object_id].max_elem

    current_idx = None
    deletions = 0
    insertions = 0

    while start_idx <= end_idx:

        diff = diffs[start_idx]

        if diff['action'] == 'create':
            # Nothing to do, we have already created a Text object and set it into updated
            pass
        elif diff['action'] == 'insert':

            if current_idx is None:
                current_idx = diff['index']
                deletions = 0
                insertions = []

            max_elem = max(max_elem, parse_elem_id(diff['elem_id'])['counter'])
            value = get_value(diff, cache, updated)
            print("Value -> ", value)
            print(diff)
            # print("\n")

            insertions.append({"elem_id": diff["elem_id"],
                               "value": value,
                               "conflicts": diff["conflicts"] if "conflicts" in diff else []
                               })

            if start_idx == end_idx \
                    or diffs[start_idx + 1]['action'] != 'insert' \
                    or diffs[start_idx + 1]['index'] != diff['index'] + 1:

                # Reproduces the behavior of the Array.splice() method of JS.
                elems = elems[:current_idx] + \
                    insertions + \
                    elems[current_idx+deletions:]
                current_idx = None

        elif diff['action'] == 'set':
            elems[diff['index']] = {
                'elem_id': elems[diff['index']]['elem_id'],
                'value': get_value(diff, cache, updated),
                'conflicts': diff['conflicts']
            }
            pass
        elif diff['action'] == 'remove':

            if current_idx is None:
                current_idx = diff['index']
                deletions = 0
                insertions = []
            deletions += 1

            if start_idx == end_idx \
                    or diffs[start_idx+1]['action'] not in ['insert', 'remove'] \
                    or diffs[start_idx+1]['index'] != diff['index']:

                elems = elems[:current_idx] + elems[current_idx+deletions:]

        elif diff['action'] == 'maxElem':
            max_elem = max(max_elem, diff['value'])
        else:
            # TODO Error Handling
            pass

        start_idx += 1

    updated[object_id] = Text(elems, object_id=object_id, max_elem=max_elem)


def udpate_parent_objects(cache, updated, inbound):

    # shallow copy, as we modify this dict
    affected = dict(updated)

    while len(affected) > 0:
        parents = {}
        for child_id in affected.keys():
            if child_id in inbound:
                parents[inbound[child_id]] = True
        affected = parents

        for object_id in parents.keys():
            # obj = updated[object_id] if object_id in updated else cache[object_id]

            # TODO handle list objects and table objects
            # Considering it as a map by default
            parent_map_object(object_id, cache, updated)


def parent_map_object(object_id, cache, updated):
    '''
    Updates the map object with ID `objectId` such that all child objects that
    have been updated in `updated` are replaced with references to the updated
    version.
    '''

    if object_id not in updated:
        updated[object_id] = clone_map_object(cache[object_id], object_id)

    obj = updated[object_id]

    for key in obj.keys():

        if key in obj and hasattr(obj[key], 'object_id'):
            value = obj[key]
            if value.object_id in updated:
                obj[key] = updated[value.object_id]

        conflicts = obj['conflicts'][key] if key in obj['conflicts'] else {}
        conflicts_update = None

        for actor_id in conflicts.keys():

            if actor_id in conflicts:
                value = conflicts[actor_id]
                if value.object_id in updated:
                    if conflicts_update is None:
                        conflicts_update = dict(conflicts)
                        obj['conflicts'][key] = conflicts_update
                    conflicts_update[actor_id] = updated[value.object_id]

        # if (conflictsUpdate & & cache[ROOT_ID][OPTIONS].freeze) {
        #     Object.freeze(conflictsUpdate)
        # }
