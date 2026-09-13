import base64

from automerge.core import Change, Document, ROOT, ScalarType, extract


JS_CHANGES_B64 = [
    "hW9Kg/GUbzcBuQEAEKlIzXm880+qlxS6C8/+OBQBAYLXt9MGAAAIAQQCCBVyNAFCB1YEVwFwAgABDAAAAQcBBAh/AXMHY29udGVudAdtYXJrZXJzBnRyYWNrcwhwb2x5Z29ucwZldmVudHMGcnVsZXJzDmRpc3RhbmNlX3JpbmdzDXRyaXBfc2V0dGluZ3MEdGFncwRuYW1lCnRyaXBfc3RhcnQIdHJpcF9lbmQHdmVyc2lvbg0IAH4CBAMBDAB/FAENAA==",
    "hW9KgxShaIsB5wMB8ZRvNzCK+JTu6zu69dhXAzg2WTb2sMg109ifg77EvGgQqUjNebzzT6qXFLoLz/44FAIOgte30wYAAAoBAgIUEQ4TFRWkATQJQhpWLFdocAIsAH8DAw4KEQYOfyEHIn8hByp/IQcyAAUJAAAOfwAAB38AAAcABH4AEggBAAZ/ZgAHfyIAB38IAAd8CXRya190ZXN0MQN2ZXIFY29sb3IEbmFtZQAKegVkZXNjcglkaXJlY3Rpb24Kc3RhcnRfdGltZQhlbmRfdGltZQR0YWdzBnBvaW50cwABeQJpZANsYXQDbG5nAnRzA2FsdAFwBXdwdF90AAF5AmlkA2xhdANsbmcCdHMDYWx0AXAFd3B0X3QAAXkCaWQDbGF0A2xuZwJ0cwNhbHQBcAV3cHRfdAQKBgEHAQcBB38AAgF/BAoBfwQDAQICfwAHAX8ABwF/AAcBfAAUdgAKFn4AFAJUAwB/JgKFAX5UJAMAfyYChQF+VCQDAH8mAoUBflQkAgABI0ZGMDAwMFRlc3QgVHJhY2sBgOLPqgbI48+qBnAxuY0G8BbAS0DP91PjpdtCQIDiz6oG+ABwMtV46SYxwEtAsp3vp8bbQkDk4s+qBvkAcDPxY8xdS8BLQJZDi2zn20JAyOPPqgb6ACwA",
    "hW9KgyJ0NiIBrwIBFKFoi390JVKGmeB379p4/pz71Tf22P3p2QMlAWB9iV0QqUjNebzzT6qXFLoLz/44FAM6gte30wYAAAoBAgISEQoTExVNNAVCEFYRVzlwAiYAfgI6Ajt+Oj4LP38+EEsDPn86AAcKAAACDwAABAAGfgDAAAkBAAF+t3/MAA4BAAR6DG1hcmtlcl90ZXN0MQZjb29yZHMDbGF0A2xuZwZwYXJhbXMEbmFtZQALfwVkZXNjcgAQfAVjb2xvcgRpY29uBHRhZ3MHcmVtb3ZlZAYLARAEAgACAX4ABAsBfwQSAX4CAQIAAoUBAgALFn8AEBYCdgIA4XoUrkfBS0Bcj8L1KNxCQFRlc3QgTWFya2Vyc29tZSBkZXNjcmlwdGlvbiMwMEZGMDBkZWZhdWx0JgA=",
    "hW9Kg3+h4sIBpgMBInQ2Ig9RTkb2G+pKrgRlEEEbEYlhCY5nJN/1FUC1uHMQqUjNebzzT6qXFLoLz/44FARggte30wYAAAoBAgIYERITHBVsNAtCG1YfV1twAiMAfwQCYAxiBGB/cgNzf3IDd39yA3t/cgN/AAQLAAAIfwAAA38AAAN/AAADAAN+AOMACgEABH+TfwADf/MAAAN/BAADfwQAA30KcG9seV90ZXN0MQVjb2xvcgRuYW1lAAx8BWRlc2NyB3JlbW92ZWQEdGFncwZwb2ludHMAAX0CaWQDbGF0A2xuZwABfQJpZANsYXQDbG5nAAF9AmlkA2xhdANsbmcAAX0CaWQDbGF0A2xuZwMMBAEDAQMBAwEDfQABBAwBfgQBAgJ/AAMBfwADAX8AAwF/AAMBfQB2AAwWBQB/JgKFAX4AJgKFAX4AJgKFAX4AJgKFASMwMDAwRkZUZXN0IFBvbHlnb25xMeF6FK5HwUtAXI/C9SjcQkBxMsP1KFyPwktAPQrXo3DdQkBxM+F6FK5HwUtAH4XrUbjeQkBxNAAAAAAAwEtAPQrXo3DdQkAjAA==",
]


def test_apply_javascript_changes() -> None:
    doc = Document()
    changes = [Change.from_bytes(base64.b64decode(value)) for value in JS_CHANGES_B64]

    doc.apply_changes(changes)

    content = extract(doc)["content"]
    assert content["tracks"]["trk_test1"]["name"] == "Test Track"
    assert content["markers"]["marker_test1"]["params"]["name"] == "Test Marker"
    assert content["polygons"]["poly_test1"]["name"] == "Test Polygon"
    assert content["version"] == 1


def test_change_from_bytes_and_missing_dependencies() -> None:
    source = Document(actor_id=b"source")
    with source.transaction() as tx:
        tx.put(ROOT, "first", ScalarType.Str, "one")
    first = source.get_last_local_change()
    assert first is not None

    with source.transaction() as tx:
        tx.put(ROOT, "second", ScalarType.Str, "two")
    second = source.get_last_local_change()
    assert second is not None

    target = Document(actor_id=b"target")
    target.apply_changes([Change.from_bytes(second.raw_bytes)])
    assert target.get_missing_deps([]) == [first.hash]

    target.apply_changes([Change.from_bytes(first.raw_bytes)])
    assert target.get_missing_deps([]) == []
    assert extract(target) == {"first": "one", "second": "two"}


def test_counter_round_trip_and_increment() -> None:
    doc = Document(actor_id=b"counter")
    with doc.transaction() as tx:
        tx.put(ROOT, "value", ScalarType.Counter, 4)

    value = doc.get(ROOT, "value")
    assert value is not None
    assert value[0] == (ScalarType.Counter, 4)

    with doc.transaction() as tx:
        tx.increment(ROOT, "value", 5)

    assert doc.get(ROOT, "value")[0] == (ScalarType.Counter, 9)  # type: ignore[index]
    assert extract(doc) == {"value": 9}
