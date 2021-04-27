import unittest
from automerge import doc
from automerge_backend import PyBackend, decode_change
from uuid import uuid4


class TestBackendConcurrency(unittest.TestCase):
    def test_not_allow_request_patches_to_be_applied_out_of_order(self):
        actor = "02ef21f3c9eb4087880ebedd7c4bbe43"
        d0 = doc.Doc(initial_data={"blackbirds": 24}, actor_id=actor)
        with d0 as d:
            d["patridges"] = 1
        diffs = {
            "objectId": "_root",
            "type": "map",
            "props": {"partridges": {actor: {"value": 1}}},
        }
        with self.assertRaises(Exception) as e:
            d0.apply_patch(
                {
                    "actor": actor,
                    "seq": 2,
                    "clock": {actor: 2},
                    "diffs": diffs,
                }
            )
        msg = str(e.exception)
        self.assertTrue("Out of order patch" in msg)

    def test_allow_interleaving_of_patches_and_changes(self):
        actor = "02ef21f3c9eb4087880ebedd7c4bbe43"
        d0 = doc.Doc(initial_data={"number": 1}, actor_id=actor)
        change0 = d0.local_changes.pop()
        self.assertEqual(
            change0,
            {
                "actor": actor,
                "deps": [],
                "startOp": 1,
                "seq": 1,
                "time": change0["time"],
                "message": "",
                "ops": [
                    {
                        "obj": "_root",
                        "action": "set",
                        "key": "number",
                        "insert": False,
                        "value": 1,
                        "pred": [],
                    },
                ],
            },
        )

        with d0 as d:
            d["number"] = 2

        change1 = d0.local_changes.pop()
        self.assertEqual(
            change1,
            {
                "actor": actor,
                "deps": [],
                "startOp": 2,
                "seq": 2,
                "time": change1["time"],
                "message": "",
                "ops": [
                    {
                        "obj": "_root",
                        "action": "set",
                        "key": "number",
                        "insert": False,
                        "value": 2,
                        "pred": [f"1@{actor}"],
                    },
                ],
            },
        )

        b0 = PyBackend.create()
        patch0, bin_change0 = b0.apply_local_change(change0)

        d0.apply_patch(patch0)
        with d0 as d:
            d["number"] = 3

        change2 = d0.local_changes.pop()
        self.assertEqual(
            change2,
            {
                "actor": actor,
                "seq": 3,
                "startOp": 3,
                "time": change2["time"],
                "message": "",
                "deps": [],
                "ops": [
                    {
                        "obj": "_root",
                        "action": "set",
                        "key": "number",
                        "insert": False,
                        "value": 3,
                        "pred": [f"2@{actor}"],
                    },
                ],
            },
        )

    def test_deps_are_filled_in_if_the_frontend_does_not_have_the_latest_patch(self):
        actor1 = "02ef21f3c9eb4087880ebedd7c4bbe43"
        actor2 = "2a1d376b24f744008d4af58252d644dd"

        doc1 = doc.Doc(initial_data={"number": 1}, actor_id=actor1)
        change1 = doc1.local_changes.pop()

        b1 = PyBackend.create()
        patch1, bin_change1 = b1.apply_local_change(change1)

        b2 = PyBackend.create()
        patch1a = b2.apply_changes([bin_change1])

        doc1a = doc.Doc(actor_id=actor2)
        doc1a.apply_patch(patch1a)

        with doc1a as d:
            d["number"] = 2
        change2 = doc1a.local_changes.pop()
        self.assertEqual(len(doc1a.local_changes), 0)

        self.assertEqual(
            change2,
            {
                "actor": actor2,
                "seq": 1,
                "startOp": 2,
                "deps": [decode_change(bin_change1)["hash"]],
                "time": change2["time"],
                "message": "",
                "ops": [
                    {
                        "obj": "_root",
                        "action": "set",
                        "key": "number",
                        "insert": False,
                        "value": 2,
                        "pred": [f"1@{actor1}"],
                    },
                ],
            },
        )

        with doc1a as d:
            d["number"] = 3
        change3 = doc1a.local_changes.pop()
        self.assertEqual(len(doc1a.local_changes), 0)

        self.assertEqual(
            change3,
            {
                "actor": actor2,
                "seq": 2,
                "startOp": 3,
                "deps": [],
                "time": change3["time"],
                "message": "",
                "ops": [
                    {
                        "obj": "_root",
                        "action": "set",
                        "key": "number",
                        "insert": False,
                        "value": 3,
                        "pred": [f"2@{actor2}"],
                    },
                ],
            },
        )

        patch2, bin_change2 = b2.apply_local_change(change2)
        patch3, bin_change3 = b2.apply_local_change(change3)

        self.assertEqual(
            decode_change(bin_change2)["deps"], [decode_change(bin_change1)["hash"]]
        )
        self.assertEqual(
            decode_change(bin_change3)["deps"], [decode_change(bin_change2)["hash"]]
        )

        self.assertEqual(patch1a["deps"], [decode_change(bin_change1)["hash"]])
        self.assertEqual(patch2["deps"], [])

        doc1a.apply_patch(patch2)
        doc1a.apply_patch(patch3)

        with doc1a as d:
            d["number"] = 4

        change4 = doc1a.local_changes.pop()
        self.assertEqual(
            change4,
            {
                "actor": actor2,
                "seq": 3,
                "startOp": 4,
                "time": change4["time"],
                "message": "",
                "deps": [],
                "ops": [
                    {
                        "obj": "_root",
                        "action": "set",
                        "key": "number",
                        "insert": False,
                        "value": 4,
                        "pred": [f"3@{actor2}"],
                    },
                ],
            },
        )

        patch4, bin_change4 = b2.apply_local_change(change4)
        self.assertEqual(decode_change(bin_change4)['deps'], [decode_change(bin_change3)['hash']])
