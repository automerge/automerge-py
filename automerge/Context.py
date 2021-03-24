from typing import List, Any, Dict
from .Text import Text
from .Counter import Counter
import uuid

from .ApplyPatch import apply_diffs

from pdb import set_trace as bp


class Context():
    actor_id: str = None
    cache: Any  # Document
    updated: Dict = {}
    inbound: Any = None
    ops: List = []
    diffs: List = []

    def __init__(self, doc, actor_id):
        self.cache = doc.cache

        self.actor_id = actor_id
        # TODO
        # self.inbound = copyObject(doc.inbound)

    def add_op(self, operation):
        self.ops.append(operation)

    def apply(self, diff):
        self.diffs.append(diff)
        apply_diffs([diff], self.cache, self.updated, self.inbound)

    def get_object(self, object_id):

        if object_id in self.updated:
            return self.updated[object_id]
        elif object_id in self.cache:
            return self.cache[object_id]
        else:
            raise Exception(f'Target object does not exist: {object_id}')

    def get_object_field(self, object_id, key):
        # TODO
        pass

    def create_nested_objects(self, value):

        # TODO : Error handling
        if value.object_id is not None:
            raise Exception('Cannot assign an object that already belongs to an Automerge document. '
                            + 'See https://github.com/automerge/automerge#making-fine-grained-changes')

        object_id = uuid.uuid4()

        if isinstance(value, Text):

            # Create a new Text object
            self.apply({'action': 'create',
                        'type': 'text',
                        'object_id': object_id})

            op = {'action': 'makeText',
                  'object_id': object_id}

            self.add_op(op)

            if len(value) > 0:
                self.splice(object_id, 0, 0, value)

            # // Set object properties so that any subsequent modifications of the Text
            # // object can be applied to the context
            text = self.get_object(object_id)
            value.object_id = object_id
            value.elems = text.elems
            value.max_elem = text.max_elem
            value.context = self

        return object_id

    def set_value(self, object_id, key, value):
        print("Context > set value ", key, value, object_id)

        # TODO:
        # if isinstance(value, datetime):
        if isinstance(value, Counter):
            # TODO
            pass
        elif type(value) in [str, int, float, bool, type(None)]:
            # Primitive value (number, string, boolean, or null)

            op = {'action': 'set',
                  'object_id': object_id,
                  'key': key,
                  'value': value
                  }
            self.add_op(op)
            return {"value": value}

        else:

            child_id = self.create_nested_objects(value)

            op = {'action': 'link',
                  'object_id': object_id,
                  'key': key,
                  'value': child_id
                  }
            self.add_op(op)
            return {'value': child_id, 'link': True}

    def set_map_key(self, object_id, type_, key: str, value):

        bp()
        # TODO : check that key is a non-empty str
        obj = self.get_object(object_id)
        # TODO : test that obj is not a Counter

        # If the assigned field value is the same as the existing value, and
        # the assignment does not resolve a conflict, do nothing

        if obj[key] != value or obj.conflicts[key] or value is None:
            # value_object is actually a dict. I just kept the same name than in the JS code.
            # TODO : rename value_object to value_dict
            value_object = self.set_value(object_id, key, value)
            value_object['action'] = 'set'
            value_object['type'] = type_
            value_object['object_id'] = object_id
            value_object['key'] = key

            self.apply(value_object)

    def delete_map_key(self, object_id, key):
        pass

    def insert_list_item(self, object_id, index, value):
        '''
        Inserts a new list element `value` at position `index` into the list with ID `objectId`.
        '''
        print("Context > insert list item", index, value, object_id)
        lst = self.get_object(object_id)
        if index < 0 or index > len(lst):
            raise Exception(
                f'List index {index} is out of bounds for list of length {len(lst)}')

        max_elem = lst.max_elem + 1
        type_str = 'text' if isinstance(lst, Text) else 'list'
        if index == 0:
            prev_id = '_head'
        elif isinstance(lst, Text):
            prev_id = lst[index-1]['elem_id']
        else:
            prev_id = lst['elem_ids'][index-1]

        elem_id = f'{self.actor_id}:{max_elem}'
        self.add_op({'action': 'ins',
                     'obj': object_id,
                     'key': prev_id,
                     'elem': max_elem})

        # value_object is actually a dict. I just kept the same name than in the JS code.
        # TODO : rename value_object to value_dict
        value_object = self.set_value(object_id, elem_id, value)
        value_object['action'] = 'insert'
        value_object['type'] = type_str
        value_object['object_id'] = object_id
        value_object['elem_id'] = elem_id
        value_object['index'] = index

        self.apply(value_object)
        self.get_object(object_id).max_elem = max_elem

    def set_list_index(self, object_id, index, value):
        pass

    def splice(self, object_id, start, deletions, insertions):
        '''
        Updates the list object with ID `objectId`, deleting `deletions` list
        elements starting from list index `start`, and inserting the list of new
        elements `insertions` at that position.
        '''

        lst = self.get_object(object_id)
        type_str = 'text' if isinstance(lst, Text) else 'list'

        if deletions > 0:

            if start is None or start > len(lst) - deletions:
                # TODO Error handling
                return

            for i in range(deletions):

                self.add_op({'action': 'del',
                             'object_id': object_id,
                             'key': lst[start]['elem_id']})
                self.apply({'action': 'remove',
                            'type': type_str,
                            'object_id': object_id,
                            'index': start})

                if i == 0:
                    lst = self.get_object(object_id)

        for i in range(len(insertions)):
            self.insert_list_item(object_id, start+i, insertions[i]['value'])

    def add_table_row(self, object_id, row):
        pass

    def delete_table_row(self, object_id, rowId):
        pass

    def increment(self, object_id, key, delta):
        pass
