
import json
import unittest
from automerge_backend import Backend, default_sync_state
from automerge import doc
from automerge.datatypes import Counter


# we want changes to have the same timestamp to make the tests deterministic
# (prevents the bloom filter from failing)
ts = lambda: 0

# TODO: Some tests have `0` timestamps but no actor ids, so they won't be deterministic
# What's going on here?
ai1 = "02ef21f3c9eb4087880ebedd7c4bbe43"
ai2 = "2a1d376b24f744008d4af58252d644dd"

class AlreadyInSync(unittest.TestCase):
    def test_not_reply_if_we_have_no_data_as_well(self):
        n1, n2 = doc.Doc(backend=Backend()), doc.Doc(backend=Backend())
        s1, s2 = default_sync_state(), default_sync_state()
        m1 = m2 = None

        m1 = n1.generate_sync_message(s1)
        n2.receive_sync_message(s2, m1)
        m2 = n2.generate_sync_message(s2)

        self.assertEqual(m2, None)

    def test_repos_with_equal_heads_do_not_need_a_reply_message(self):
        n1, n2 = doc.Doc(backend=Backend(), timestamper=ts), doc.Doc(backend=Backend(), timestamper=ts)
        s1, s2 = default_sync_state(), default_sync_state()

        with n1 as d:
            d["n"] = []
            d["x"] = Counter(0)

        #for i in range(0, 10):
        #    with n1 as d:
        #        d["n"].append(i)

        with n1 as d:
            d["x"] += 1
            d["n"].append("h")

        print(n1.get_active_root_obj())

        foo = doc.Doc(initial_data = {"nx": []})
        with foo as d:
            foo["nx"].append(1)
        print(foo.get_active_root_obj())
        b = Backend()
        (c1, c2) = foo.local_changes
        print(c1)
        print(c2)
        p0, bs = b.apply_local_change(c1)
        p1, bs = b.apply_local_change(c2)

        print(p0, p1)
        foo.apply_patch(p0)
        foo.apply_patch(p1)
        print(foo.get_active_root_obj())
        print("===")

        patch = n2.apply_changes(n1.get_all_changes())
        self.assertEqual(n1.get_active_root_obj(), n2.get_active_root_obj())
        #self.assertEqual(n1, n2)

        m1 = n1.generate_sync_message(s1)
        self.assertEqual(s1.last_sent_heads, n1.get_heads())

        n2.receive_sync_message(s2, m1)
        m2 = n2.generate_sync_message(s2)

        self.assertEqual(m2, None)
