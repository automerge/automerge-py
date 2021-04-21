import unittest
import re
from automerge import doc
from uuid import uuid4
import json


def goto_path(obj, path):
    temp = obj
    for segment in path[:-1]:
        temp = temp[segment]
    return temp


KEY_TO_INT_SIGNAL = "KEYTOINT:"


def destringify_keys(d):
    if isinstance(d, dict):
        for k in list(d.keys()):
            v = d[k]
            if k.startswith(KEY_TO_INT_SIGNAL):
                s_int = k[len(KEY_TO_INT_SIGNAL) :]
                k_as_int = int(s_int)
                d[k_as_int] = v
                del d[k]
            destringify_keys(v)
    elif isinstance(d, list):
        for v in d:
            destringify_keys(v)


def run_test(self, steps, name):
    doc_ = change = None

    for step in steps:
        typ = step["type"]
        if typ == "create_doc":
            params = step["params"] if "params" in step else {}
            kwargs = {}
            if "data" in params:
                kwargs["initial_data"] = params["data"]
            if "actor_id" in params:
                kwargs["actor_id"] = params["actor_id"]
            doc_ = doc.Doc(**kwargs)
        elif typ == "assert_doc_equal":
            # Could use `doc_` instead of `doc_.root_obj`
            # but this would make error messages less nice
            self.assertEqual(doc_.root_obj, step["to"])
        elif typ == "change_doc":
            with doc_ as d:
                for edit in step["trace"]:
                    edit_typ = edit["type"]
                    if edit_typ == "set":
                        path, value = edit["path"], edit["value"]
                        goto_path(d, path)[path[-1]] = value
                    elif edit_typ == "delete":
                        path = edit["path"]
                        del goto_path(d, path)[path[-1]]
                    elif edit_typ == "insert":
                        path = edit["path"]
                        array = goto_path(d, path)
                        array.insert(path[-1], edit["value"])
                    else:
                        raise Exception(f"Unexpected edit type: {edit_typ}")
            change = doc_.changes.pop()
        elif typ == "assert_change_equal":
            # The time is the one value that is non-deterministic
            change["time"] = 0
            self.assertEqual(change, step["to"])
        elif typ == "apply_patch":
            # breakpoint()
            destringify_keys(step["patch"])
            doc_.apply_patch(step["patch"])
        elif typ == "assert_conflicts_equal":
            to, path = step["to"], step["path"]
            self.assertEqual(doc_.get_recent_ops(path), to)
        else:
            raise Exception(f"Unknown step type: {typ}")


def create_test_wrapper(steps, name):
    def wrapper(self):
        self.maxDiff = None
        run_test(self, steps, name)

    return wrapper


# SECTION_MATCH = "apply"
# TEST_MATCH = "inside_list_element_conflicts"
SECTION_MATCH = None
TEST_MATCH = None

with open("frontend_tests.json", "r") as f:
    tests = json.loads(f.read())
    for (section_name, section_tests) in tests.items():
        if SECTION_MATCH and SECTION_MATCH not in section_name:
            continue
        test_methods = {}
        for test in section_tests:
            name = test["name"].replace(" ", "_")
            name = f"test_{name}"
            if (not TEST_MATCH) or (TEST_MATCH in name):
                test_methods[name] = create_test_wrapper(test["steps"], name)
        section_name = section_name.replace(" ", "_")
        section_name = section_name.upper()
        test_klass = type(section_name, (unittest.TestCase,), test_methods)
        globs = globals()
        globs[section_name] = test_klass
        # Prevent the tests in the last test group from running twice.
        test_klass = None
