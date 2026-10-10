from datetime import datetime, timedelta, timezone

import automerge.core as core

from automerge import Document


def test_doc() -> None:
    doc = Document()
    with doc.change() as c:
        c["hello"] = "world"

    assert str(doc["hello"]) == "world"


def test_doc_import_obj() -> None:
    doc = Document()
    with doc.change() as c:
        c["hello"] = {"this": {"is": "a", "whole": "tree"}, "of": [{"obj": "ect"}, "s"]}

    assert doc.to_py() == {
        "hello": {"this": {"is": "a", "whole": "tree"}, "of": [{"obj": "ect"}, "s"]}
    }


def test_nested_import() -> None:
    doc = Document()
    with doc.change() as c:
        c["hello"] = {}
        c["hello"]["foo"] = "bar"

    assert doc.to_py() == {"hello": {"foo": "bar"}}
    {}.__getitem__


def test_iterable() -> None:
    doc = Document()
    with doc.change() as c:
        c["list"] = [1, 2, 3]
        c["map"] = {
            "k": "v",
            "x": "y",
        }
    assert len(doc["list"]) == 3
    assert list(doc["list"]) == [1, 2, 3]
    assert len(doc["map"]) == 2
    assert list(doc["map"]) == ["k", "x"]
    assert list(doc["map"].items()) == [("k", "v"), ("x", "y")]


def test_bool_round_trip() -> None:
    doc = Document()
    with doc.change() as c:
        c["flag"] = True
        c["off"] = False
    assert doc["flag"] is True
    assert doc["off"] is False


def test_change_message() -> None:
    doc = Document()
    with doc.change(message="greeting", time=1234) as c:
        c["hello"] = "world"

    change = doc._doc.get_last_local_change()
    assert change is not None
    assert change.message == "greeting"
    # `time` is unix seconds, stored as-is (JS bindings parity)
    assert change.timestamp.timestamp() == 1234.0


def test_scalar_timestamp_is_utc_aware() -> None:
    """Scalar Timestamp values read back as timezone-aware UTC datetimes.

    Naive datetimes would make `.timestamp()` fail with OSError on Windows
    for values before 1970 local time, and shift by the machine's timezone
    elsewhere.
    """
    doc = Document()
    when = datetime(2024, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    with doc.change() as c:
        c["when"] = when

    value = doc["when"]
    assert isinstance(value, datetime)
    assert value.tzinfo is not None
    assert value.utcoffset() == timedelta(0)
    assert value.timestamp() == when.timestamp()

    # Round-trips through save/load
    core_loaded = core.Document.load(doc._doc.save())
    value, _ = core_loaded.get(core.ROOT, "when")
    assert value[1].timestamp() == when.timestamp()


def test_change_time_defaults_to_now() -> None:
    """Like @automerge/automerge, change() defaults `time` to now"""
    import time

    doc = Document()
    before = time.time()
    with doc.change() as c:
        c["hello"] = "world"
    after = time.time()

    change = doc._doc.get_last_local_change()
    assert change is not None
    assert before - 1 <= change.timestamp.timestamp() <= after + 1

    # Pass time=0 to suppress the timestamp, like JS `time: undefined`
    with doc.change(time=0) as c:
        c["hello"] = "again"
    change = doc._doc.get_last_local_change()
    assert change is not None
    assert change.timestamp.timestamp() == 0.0
