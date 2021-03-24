import copy
from .Text import Text
from .Counter import Counter

import re

from pdb import set_trace as bp


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

    try:
        if diffs[0]['action'] == 'set':
            bp()
    except:
        pass

    start_idx = 0
    for end_idx in range(len(diffs)):

        diff = diffs[end_idx]

        if diff['type'] == 'map':
            # TODO
            # updateMapObject(diff, cache, updated, inbound)
            start_idx = end_idx + 1
            pass
        elif diff['type'] == 'table':
            # TODO
            # updateTableObject(diff, cache, updated, inbound)
            start_idx = end_idx + 1
            pass
        elif diff['type'] == 'list':
            # TODO
            # updateListObject(diff, cache, updated, inbound)
            start_idx = end_idx + 1
            pass
        elif diff['type'] == 'text':

            if end_idx == len(diffs)-1 or diffs[end_idx+1]['object_id'] != diff['object_id']:
                update_text_object(diffs, start_idx, end_idx, cache, updated)
                start_idx = end_idx + 1

        else:
            # TODO Error handling
            # throw new TypeError(`Unknown object type: ${diff.type}`)
            pass


def update_text_object(diffs, start_idx, end_idx, cache, updated):
    '''
    Applies the list of changes from `diffs[start_idx]` to `diffs[end_idx]`
    (inclusive the last element) to a Text object. `cache` and `updated` are
    indexed by objectId; the existing read-only object is taken from `cache`,
    and the updated object is written to `updated`.
    '''

    print("ApplyPatch > update text object ", start_idx, diffs, cache, updated)
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
            print("\n")

            insertions.append({"elem_id": diff["elem_id"],
                               "value": value,
                               "conflicts": diff["conflicts"] if "conflicts" in diff else []
                               })

            if start_idx == end_idx \
                    or diffs[start_idx + 1]['action'] != 'insert' \
                    or diffs[start_idx + 1]['index'] != diff['index'] + 1:

                # Reproduces the behavior of the Array.splice() method of JS.
                elems = elems[:current_idx] + \
                    elems[current_idx+deletions:] + insertions
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
