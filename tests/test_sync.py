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


def sync(nA, nB, a_sync_state=None, b_sync_state=None):
    if a_sync_state is None:
        a_sync_state = default_sync_state()
    if b_sync_state is None:
        b_sync_state = default_sync_state()

    i = MAX_ITER = 10
    a_to_b_msg = b_to_a_msg = None

    while True:
        if i == 0:
            raise Exception(
                f"Did not synchronize within {MAX_ITER} iterations. Do you have a bug causing an infinite loop?"
            )
        a_to_b_msg = nA.generate_sync_message(a_sync_state)
        b_to_a_msg = nB.generate_sync_message(b_sync_state)

        if a_to_b_msg:
            nB.receive_sync_message(b_sync_state, a_to_b_msg)

        if b_to_a_msg:
            nA.receive_sync_message(a_sync_state, b_to_a_msg)

        if not a_to_b_msg and not b_to_a_msg:
            break

        i -= 1

    return a_sync_state, b_sync_state


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
        n1, n2 = (
            doc.Doc(backend=Backend(), timestamper=ts, initial_data={"n": []}),
            doc.Doc(backend=Backend(), timestamper=ts),
        )
        s1, s2 = default_sync_state(), default_sync_state()

        for i in range(0, 10):
            with n1 as d:
                d["n"].append(i)

        patch = n2.apply_changes(n1.get_all_changes())
        self.assertEqual(n1, n2)

        m1 = n1.generate_sync_message(s1)
        self.assertEqual(s1.last_sent_heads, n1.get_heads())

        n2.receive_sync_message(s2, m1)
        m2 = n2.generate_sync_message(s2)

        self.assertEqual(m2, None)

    def test_offer_all_changes_to_n2_when_starting_from_nothing(self):
        n1, n2 = (
            doc.Doc(backend=Backend(), timestamper=ts, initial_data={"n": []}),
            doc.Doc(backend=Backend(), timestamper=ts),
        )
        for i in range(0, 10):
            with n1 as d:
                d["n"].append(i)

        self.assertNotEqual(n1, n2)
        sync(n1, n2)
        self.assertEqual(n1, n2)

    # SKIPPED: Isn't this identical to the previous test?
    # def test_sync_peers_when_one_has_commits_the_other_does_not(self):

    def test_work_with_prior_sync_state(self):
        n1, n2 = doc.Doc(backend=Backend(), timestamper=ts), doc.Doc(
            backend=Backend(), timestamper=ts
        )
        s1, s2 = default_sync_state(), default_sync_state()

        for i in range(0, 5):
            with n1 as d:
                d["x"] = i

        for i in range(5, 10):
            with n1 as d:
                d["x"] = i

        self.assertNotEqual(n1, n2)
        sync(n1, n2, s1, s2)
        self.assertEqual(n1, n2)
