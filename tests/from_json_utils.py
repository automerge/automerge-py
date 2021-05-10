import json
import unittest


def goto_path(obj, path):
    temp = obj
    for segment in path[:-1]:
        temp = temp[segment]
    return temp


def traverse(d, cb):
    # the wrapper handles the case where d is a primitive like "VALUETOCOUNTER:0"
    wrapper = {"temp": d}
    _traverse(wrapper, cb)
    return wrapper["temp"]


def _traverse(d, cb):
    if isinstance(d, dict):
        for k in list(d.keys()):
            v = d[k]
            cb(d, k, v)
            _traverse(v, cb)
    elif isinstance(d, list):
        for idx, v in enumerate(d):
            cb(d, idx, v)
            _traverse(v, cb)


# The wrapper needs to exist b/c I think otherwise there are variable scoping issues
# that cause every test to be, for example, the first test in an array of JSON tests
def create_test_wrapper(run_test, steps, name, PRINT_NAME):
    def wrapper(self):
        if PRINT_NAME:
            print(name)
        run_test(self, steps, name)

    return wrapper


def run_tests_from_json(
    fname, test_func, globs, SECTION_MATCH=None, TEST_MATCH=None, PRINT_NAME=False
):
    with open(fname, "r") as f:
        tests = json.loads(f.read())
        for (section_name, section_tests) in tests.items():
            if SECTION_MATCH and SECTION_MATCH not in section_name:
                continue
            test_methods = {}
            for test in section_tests:
                name = test["name"].replace(" ", "_")
                name = f"test_{name}"
                if (not TEST_MATCH) or (TEST_MATCH in name):
                    test_methods[name] = create_test_wrapper(
                        test_func, test["steps"], name, PRINT_NAME
                    )
            section_name = section_name.replace(" ", "_")
            section_name = section_name.upper()
            test_klass = type(section_name, (unittest.TestCase,), test_methods)
            # globs = globals()
            globs[section_name] = test_klass
            # Prevent the tests in the last test group from running twice.
            # TODO: This line might be irrelevant since we extracted this code into its own file
            test_klass = None
