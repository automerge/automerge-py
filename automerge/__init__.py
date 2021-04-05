from .automerge import new_backend, new_frontend

from .Text import Text
from .Counter import Counter

from .Proxies import MapProxy, ListProxy

from .ApplyPatch import udpate_parent_objects

import collections
from typing import List, Any, Dict

import uuid


class Doc(collections.MutableMapping):

    actor_id: str = None
    object_id = '00000000-0000-0000-0000-000000000000'

    root_object_proxy: Any = None
    changing: bool = False
    cache: Dict = {}
    conflicts: Dict = {}

    state: Dict = {"seq": 0,
                   "requests": [],
                   "deps": {},
                   "can_undo": False,
                   "can_redo": False}

    backend: Any = None

    def __init__(self):
        self.actor_id = uuid.uuid4()
        self.backend = new_backend()

        self.cache[self.object_id] = self

    def apply_changes(self, changes):
        pass

    def __enter__(self):
        if self.changing:
            raise Exception(
                "Attempting to modify a document you are already modifying"
            )
        self.changing = True

        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.changing = False

        udpate_parent_objects(
            self.cache,
            self.root_object_proxy.context.updated,
            self.root_object_proxy.context.inbound)

        self.make_change('change', self.root_object_proxy.context)

        return False

    def changes(self):
        return self.root_object_proxy.context.diffs

    def make_change(self, request_type, context=None, options=None):
        '''
        Adds a new change request to the list of pending requests, and returns an
        updated document root object. `requestType` is a string indicating the type
        of request, which may be "change", "undo", or "redo". For the "change" request
        type, the details of the change are taken from the context object `context`.
        `options` contains properties that may affect how the change is processed; in
        particular, the `message` property of `options` is an optional human-readable
        string describing the change.
        '''

        # copy
        state = dict(self.state)

        state['seq'] += 1
        deps = dict(state['deps'])

        if self.actor_id in deps:
            del deps[self.actor_id]

        request = {"request_type": request_type,
                   "actor": self.actor_id, "seq": state['seq'], "deps": deps}

        if options is not None:

            if 'message' in options:
                request['message'] = options['message']

            if 'undoable' in options and not options['undoable']:
                request['undoable'] = False

        # TODO function ensure_single_assignment
        # if context is not None:
        #    request['ops'] = ensure_single_assignment(context['ops'])

        if self.backend is not None:
            # TODO backend function apply_local_change
            # (backend_state, patch) = doc.backend.apply_local_change(state['backend_state'], request)
            # state['backend_state'] = backend_state
            # state['request'] = []
            # TODO function apply_patch_to_doc
            # return [  apply_patch_to_doc(doc, patch, state, true), request ]
            pass
        else:

            # TODO translate this JS code to Python
            # if (!context) context = new Context(doc, actor)
            # const queuedRequest = copyObject(request)
            # queuedRequest.before = doc
            # queuedRequest.diffs = context.diffs
            # state.requests = state.requests.slice() // shallow clone
            # state.requests.push(queuedRequest)
            # return [updateRootObject(doc, context.updated, context.inbound, state), request]
            pass

    def __getitem__(self, key):
        print(f"Doc > get item {key}  ")

        return self.root_object_proxy[key]

        # path = [key]
        # # value = self.frontend.value_at_path([key])
        # if value == "map":
        #     return MapProxy(self, path)
        # elif value == "list":
        #     return ListProxy(self, path)
        # return value

    def __setitem__(self, key, item):
        print(f"Doc > set item {key} = {item} on {self} ")

        # Instead of using the rust frontend,
        # we want to store the item python-side
        # "apply that change to your python frontend"

        if not self.changing:
            raise Exception(
                "Attempting to modify a document prior to its opening for modification"
            )

        if self.root_object_proxy is None:
            self.root_object_proxy = MapProxy(self)

        self.root_object_proxy[key] = item

        # change = self.frontend.set_at_path(path, item)
        # self.changes.append(change)

    def __delitem__(self, key):
        # TODO
        pass

    def __iter__(self):
        return iter(self.root_object_proxy)

    def __len__(self):
        # TODO
        pass
