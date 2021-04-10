import unittest
from automerge import doc

# apply_local_change
class Initializing(unittest.TestCase):
    def test_apply_change_requests(self):
        doc0 = doc.Doc(start_data={'foo': 'bar'})
        print(doc0.change)
