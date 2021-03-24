from typing import List, Any, Dict
import copy


class Text():
    context: Any
    object_id: Any
    elems: List
    max_elem: Any

    def __init__(self, elems=[], context=None, object_id=None, max_elem=0):

        if isinstance(elems, str):
            self.elems = [{'value': v} for v in elems]
        elif isinstance(elems, list):
            self.elems = list(elems)

        self.object_id = object_id
        self.context = context
        self.max_elem = max_elem

    def __str__(self):

        # arbitraty limit at 30
        return ''.join(list(map(lambda x: x['value'], self.elems[:30]))) \
            + ('(...)' if len(self.elems) > 30 else '')

    def __repr__(self):
        return "Text(%s)" % (self.__str__())

    def __len__(self):
        return len(self.elems)

    def __getitem__(self, key):
        print("Text > get item ", key)
        # bp()
        return self.elems[key]

    def __setitem__(self, key, item):
        '''
        Updates the list item at position `index` to a new value `value`.
        '''
        print("Text > set item ", key, item)
        if self.context:
            # TODO Context
            # this.context.setListIndex(self.object_id, key, value)
            pass
        elif not self.object_id:
            self.elems[key] = item
        else:
            # TODO : Error handling
            # from JS : throw new TypeError('Automerge.Text object cannot be modified outside of a change block')
            pass

    def __delitem__(self, key):
        '''
        Deletes `numDelete` list items starting at position `index`.
        if `numDelete` is not given, one item is deleted.
        '''
        if self.context:
            # TODO Context
            # this.context.splice(this[OBJECT_ID], index, numDelete, [])
            pass
        elif self.object_id is None:
            # TODO : test performances
            del self.elems[key]
        else:
            # TODO : Error handling
            # throw new TypeError('Automerge.Text object cannot be modified outside of a change block')
            pass

    def insert(self, index, values):
        '''
        Inserts new list items `values` starting at position `index`.
        '''
        if self.context:
            # TODO Context
            # self.context.splice(self.object_id)
            pass
        elif self.object_id is None:
            # TODO : test performances

            if isinstance(values, list):
                self.elems = self.elems[:index] \
                    + [{'value': v} for v in values] \
                    + self.elems[index:]
            elif isinstance(values, str):
                self.elems = self.elems[:index] + \
                    [{'value': v} for v in values] + self.elems[index:]
        else:
            # TODO : Error handling
            # throw new TypeError('Automerge.Text object cannot be modified outside of a change block')
            pass

    def __copy__(self):
        ''' Design choice : shallow copy doesn't copy the context. TBD.'''
        return Text(''.join(self.elems),
                    context=None,
                    object_id=self.object_id,
                    max_elem=self.max_elem
                    )

    def __deepcopy__(self, memo):
        ''' Design choice : deep copy uses the same context. TBD. '''
        return Text(''.join(self.elems),
                    context=self.context,
                    object_id=self.object_id,
                    max_elem=self.max_elem
                    )

    def __iter__(self):
        return self.elems
