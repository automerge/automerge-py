import unittest
from automerge_backend import encode_change, decode_change, PyBackend


def change_hash(change):
    return decode_change(encode_change(change))["hash"]


# apply_local_change
class ApplyLocalChange(unittest.TestCase):
    def test_apply_change_requests(self):
        change1 = {
            "actor": "111111",
            "seq": 1,
            "time": 0,
            "startOp": 1,
            "deps": [],
            "ops": [
                {
                    "action": "set",
                    "obj": "_root",
                    "key": "bird",
                    "value": "magpie",
                    "pred": [],
                }
            ],
        }
        s0 = PyBackend.create()
        # patch1 is sent to frontend to update state
        # bin_change1 goes over network to peers
        (patch1, bin_change1) = s0.apply_local_change(change1)
        expected = {
            "actor": "111111",
            "seq": 1,
            "clock": {"111111": 1},
            "deps": [],
            "maxOp": 1,
            "diffs": {
                "objectId": "_root",
                "type": "map",
                "props": {"bird": {"1@111111": {"value": "magpie"}}},
            },
        }
        self.assertEqual(patch1, expected)


# apply_changes
class IncrementalDiffs(unittest.TestCase):
    def test_assign_to_a_key_in_a_map(self):
        actor = "111111"
        change1 = {
            "actor": actor,
            "seq": 1,
            "time": 0,
            "startOp": 1,
            "deps": [],
            "ops": [
                {
                    "action": "set",
                    "obj": "_root",
                    "key": "bird",
                    "value": "magpie",
                    "pred": [],
                }
            ],
        }
        s0 = PyBackend.create()
        encoded = encode_change(change1)
        patch1 = s0.apply_changes([encoded])
        self.assertEqual(
            patch1,
            {
                "clock": {actor: 1},
                "deps": [change_hash(change1)],
                "maxOp": 1,
                "diffs": {
                    "objectId": "_root",
                    "type": "map",
                    "props": {"bird": {f"1@{actor}": {"value": "magpie"}}},
                },
            },
        )


# save,load
class SaveAndLoad(unittest.TestCase):
    def test_reconstruct_changes_that_resolve_conflicts(self):
        actor1, actor2 = "8765", "1234"
        change1 = {
            "actor": actor1,
            "seq": 1,
            "startOp": 1,
            "time": 0,
            "deps": [],
            "ops": [
                {
                    "action": "set",
                    "obj": "_root",
                    "key": "bird",
                    "value": "magpie",
                    "pred": [],
                }
            ],
        }
        # TODO: If I change this to "actor1" the program crashes with an `unwrap` error in change.rs
        # Is this ok? (Should we be printing a nicer error)
        change2 = {
            "actor": actor2,
            "seq": 1,
            "startOp": 1,
            "time": 0,
            "deps": [],
            "ops": [
                {
                    "action": "set",
                    "obj": "_root",
                    "key": "bird",
                    "value": "blackbird",
                    "pred": [],
                }
            ],
        }
        change3 = {
            "actor": actor1,
            "seq": 2,
            "startOp": 2,
            "time": 0,
            "deps": [change_hash(change1), change_hash(change2)],
            "ops": [
                {
                    "action": "set",
                    "obj": "_root",
                    "key": "bird",
                    "value": "robin",
                    "pred": [f"1@{actor1}", f"1@{actor2}"],
                }
            ],
        }
        encoded = []
        for change in [change1, change2, change3]:
            encoded.append(encode_change(change))
        s1 = PyBackend.create()
        s1.load_changes(encoded)
        s2 = PyBackend.load(s1.save())
        self.assertEqual(s2.get_heads(), [change_hash(change3)])

    def test_compress_columns_with_DEFLATE(self):
        long_string = ""
        for _ in range(0, 1024):
            long_string += "a"

        change1 = {
            "actor": "111111",
            "seq": 1,
            "time": 0,
            "startOp": 1,
            "deps": [],
            "ops": [
                {
                    "action": "set",
                    "obj": "_root",
                    "key": "longString",
                    "value": long_string,
                    "pred": [],
                }
            ],
        }
        backend = PyBackend.create()
        backend.load_changes([encode_change(change1)])
        doc = backend.save()
        backend2 = PyBackend.load(doc)
        patch = backend2.get_patch()
        self.assertTrue(len(doc) < 200)
        self.assertEqual(
            patch,
            {
                "clock": {"111111": 1},
                "deps": [encode_change(change1)],
                "maxOp": 1,
                "diffs": {
                    "objectId": "_root",
                    "type": "map",
                    "props": {"longString": {"1@111111": {"value": long_string}}},
                },
            },
        )


# get_patch
class GetPatch(unittest.TestCase):
    def test_get_patch(self):
        actor = "1234"
        change1 = {
            "actor": actor,
            "seq": 1,
            "startOp": 1,
            "time": 0,
            "deps": [],
            "ops": [
                {
                    "action": "set",
                    "obj": "_root",
                    "key": "bird",
                    "value": "magpie",
                    "pred": [],
                }
            ],
        }
        change2 = {
            "actor": actor,
            "seq": 2,
            "startOp": 2,
            "time": 0,
            "deps": [change_hash(change1)],
            "ops": [
                {
                    "action": "set",
                    "obj": "_root",
                    "key": "bird",
                    "value": "blackbird",
                    "pred": [f"1@{actor}"],
                }
            ],
        }
        s0 = PyBackend.create()
        encoded = []
        for change in [change1, change2]:
            encoded.append(encode_change(change))
        s0.load_changes(encoded)
        self.assertEqual(
            s0.get_patch(),
            {
                "clock": {actor: 2},
                "deps": [change_hash(change2)],
                "maxOp": 2,
                "diffs": {
                    "objectId": "_root",
                    "type": "map",
                    "props": {"bird": {f"2@{actor}": {"value": "blackbird"}}},
                },
            },
        )
