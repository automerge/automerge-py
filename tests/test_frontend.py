import unittest
import re
from automerge import doc


class Initializing(unittest.TestCase):
    def test_be_an_empty_object_by_default(self):
        doc0 = doc.Doc()
        self.assertEqual(doc0, {})
        uuid_pattern = re.compile(r"^[0-9a-f]{32}$")
        is_uuid = bool(uuid_pattern.match(doc0.actor_id))
        self.assertTrue(is_uuid)

    # SKIP deferred actorId test

    def test_allow_instantiating_from_an_existing_object(self):
        initial_state = {"birds": {"wrens": 3, "magpies": 4}}
        doc0 = doc.Doc(initial_data=initial_state)
        self.assertEqual(doc0, initial_state)

    # def test_set_root_object_properties(self):
    #    actor_id = "1111"
    #    doc0 = doc.Doc(actor_id=actor_id)
    #    with doc0 as d:
    #        d["bird"] = "magpie"
    #    change = doc0.change()
    #    self.assertEqual(
    #        change,
    #        {
    #            "actor": actor_id,
    #            "seq": 1,
    #            "time": change["time"],
    #            "message": "",
    #            "startOp": 1,
    #            "deps": [],
    #            "ops": [
    #                {
    #                    "obj": "_root",
    #                    "action": "set",
    #                    "key": "bird",
    #                    "insert": False,
    #                    "value": "magpie",
    #                    "pred": [],
    #                }
    #            ],
    #        },
    #    )
