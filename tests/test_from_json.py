import re
from automerge import doc
from automerge.datatypes import Counter
from uuid import uuid4
import json

from .from_json_utils import traverse, goto_path, run_tests_from_json

KEY_TO_INT_SIGNAL = "KEYTOINT:"
VALUE_TO_COUNTER_SIGNAL = "VALUETOCOUNTER:"

def destringify_keys(d):
    def cb(d, k, v):
        if not isinstance(k, str):
            return
        if k.startswith(KEY_TO_INT_SIGNAL):
            s_int = k[len(KEY_TO_INT_SIGNAL) :]
            k_as_int = int(s_int)
            d[k_as_int] = v
            del d[k]
    return traverse(d, cb)


def deserialize_counters(d):
    def cb(d, k, v):
        if isinstance(v, str) and v.startswith(VALUE_TO_COUNTER_SIGNAL):
            counter = Counter(int(v[len(VALUE_TO_COUNTER_SIGNAL) :]))
            d[k] = counter
    return traverse(d, cb)


def run_test(self, steps, name):
    doc_ = change = None

    for idx, step in enumerate(steps):
        try:
            typ = step["type"]
            if typ == "create_doc":
                params = step["params"] if "params" in step else {}
                kwargs = {}
                if "data" in params:
                    kwargs["initial_data"] = deserialize_counters(params["data"])
                if "actor_id" in params:
                    kwargs["actor_id"] = params["actor_id"]
                doc_ = doc.Doc(**kwargs)
            elif typ == "assert_doc_equal":
                # Could use `doc_` instead `get_active_root_obj`
                # but this would make error messages less nice
                self.assertEqual(
                    doc_.get_active_root_obj(), deserialize_counters(step["to"])
                )
            elif typ == "change_doc":
                with doc_ as d:
                    for edit in step["trace"]:
                        edit_typ = edit["type"]
                        if edit_typ == "set":
                            path, value = edit["path"], edit["value"]
                            goto_path(d, path)[path[-1]] = deserialize_counters(value)
                        elif edit_typ == "delete":
                            path = edit["path"]
                            del goto_path(d, path)[path[-1]]
                        elif edit_typ == "insert":
                            path = edit["path"]
                            array = goto_path(d, path)
                            array.insert(path[-1], edit["value"])
                        elif edit_typ == "increment":
                            path = edit["path"]
                            goto_path(d, path)[path[-1]] += edit["delta"]
                        elif edit_typ == "decrement":
                            path = edit["path"]
                            goto_path(d, path)[path[-1]] -= edit["delta"]
                        else:
                            raise Exception(f"Unexpected edit type: {edit_typ}")
                change = doc_.local_changes.pop()
            elif typ == "assert_change_equal":
                # The time is the one value that is non-deterministic
                change["time"] = 0
                self.assertEqual(change, step["to"])
            elif typ == "apply_patch":
                doc_.apply_patch(destringify_keys(step["patch"]))
            elif typ == "assert_conflicts_equal":
                to, path = step["to"], step["path"]
                self.assertEqual(doc_.get_recent_ops(path), to)
            elif typ == "assert_in_flight_equal":
                expected = step["to"]
                self.assertEqual(len(expected), len(doc_.in_flight_local_changes))
                for (a, b) in zip(expected, doc_.in_flight_local_changes):
                    self.assertEqual(a["seq"], b)
            else:
                raise Exception(f"Unknown step type: {typ}")
        except Exception as e:
            print(f"Exception in test: {name} on step #{idx}: {step}")
            raise e

run_tests_from_json("frontend_tests.json", run_test, globals(), SECTION_MATCH=None, TEST_MATCH=None, PRINT_NAME=True)
