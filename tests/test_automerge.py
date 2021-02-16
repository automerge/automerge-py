import automerge
import unittest


class AutomergeTestCase(unittest.TestCase):

    def test_make_change(self):
        doc1 = automerge.Doc()
        doc2 = automerge.Doc()
        with doc1 as d:
            d["key"] = "value"
        doc2.apply_changes(doc1.changes)
        self.assertEqual(dict(doc2), {"key": "value"})
