import unittest
import re
from automerge import doc
from uuid import uuid4
import json


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
            self.assertEqual(doc_, step["to"])
        elif typ == "change_doc":
            with doc_ as d:
                for edit in step["trace"]:
                    edit_typ = edit["type"]
                    if edit_typ == "set":
                        path, value = edit["path"], edit["value"]
                        temp = d
                        for segment in path[:-1]:
                            temp = temp[segment]
                        temp[path[-1]] = value
                    elif edit_typ == "delete":
                        path = edit["path"]
                        temp = d
                        for segment in path[:-1]:
                            temp = temp[segment]
                        del temp[path[-1]]
                    else:
                        raise Exception(f"Unexpected edit type: {edit_typ}")
            change = doc_.changes.pop()
        elif typ == "assert_change_equal":
            # The time is the one value that is non-deterministic
            change["time"] = 0
            self.assertEqual(change, step["to"])
        else:
            raise Exception(f"Unknown step type: {typ}")


def create_test_wrapper(steps, name):
    def wrapper(self):
        run_test(self, steps, name)

    return wrapper


with open("frontend_tests.json", "r") as f:
    tests = json.loads(f.read())
    skip = False
    for (section_name, section_tests) in tests.items():
        if not skip:
            skip = True
            continue
        test_methods = {}
        for test in section_tests:
            name = test["name"].replace(" ", "_")
            name = f"test_{name}"
            test_methods[name] = create_test_wrapper(test["steps"], name)
        section_name = section_name.replace(" ", "_")
        section_name = section_name.upper()
        assert len(test_methods) == len(section_tests)
        test_klass = type(section_name, (unittest.TestCase,), test_methods)
        globs = globals()
        globs[section_name] = test_klass
        # Prevent the tests in the last test group from running twice.
        test_klass = None
