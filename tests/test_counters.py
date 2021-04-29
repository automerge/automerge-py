import json
import unittest
from automerge import doc
from automerge.datatypes import Counter


class TestCounters(unittest.TestCase):
    def test_make_counters_behave_like_primitive_numbers(self):
        d0 = doc.Doc(initial_data={"birds": Counter(3)})
        self.assertEqual(d0["birds"], 3)
        self.assertTrue(d0["birds"] < 4)
        self.assertTrue(d0["birds"] >= 0)
        self.assertFalse(d0["birds"] <= 2)
        self.assertEqual(d0["birds"] + 10, 13)
        self.assertEqual(f"I saw {d0['birds']} birds", "I saw 3 birds")

    def test_allow_counters_to_be_serialized_to_JSON(self):
        d0 = doc.Doc(initial_data={"birds": Counter(0)})
        s = json.dumps(d0.get_active_root_obj())
        self.assertEqual(s, '{"birds": 0}')
