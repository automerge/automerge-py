from typing import Any


class Counter:
    '''
    The most basic CRDT: an integer value that can be changed only by
    incrementing and decrementing. Since addition of integers is commutative,
    the value trivially converges.
    '''
    value: int

    def __init__(self, value=0):
        self.value = value


class WriteableCounter(Counter):

    context: Any
    object_id: Any
    key: Any

    def __init__(self, value, context, object_id, key):
        '''
        Returns an instance of `WriteableCounter` for use in a change callback.
        `context` is the proxy context that keeps track of the mutations.
        `object_id` is the ID of the object containing the counter, and `key` is
        the property name (key in map, or index in list) where the counter is
        located.
        '''
        self.value = value
        self.context = context
        self.object_id = object_id
        self.key = key

    def increment(self, delta=1):
        ''' 
        Increases the value of the counter by `delta`. If `delta` is not given,
        increases the value of the counter by 1.
        '''
        self.context.increment(self.object_id, self.key, delta)
        self.value += delta
        return self.value

    def decrement(self, delta=1):
        '''
        Decreases the value of the counter by `delta`. If `delta` is not given,
        decreases the value of the counter by 1.
        '''
        return self.increment(-delta)
