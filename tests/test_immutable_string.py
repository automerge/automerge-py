import pytest
from automerge import ImmutableString


def test_immutable_string_creation():
    """Test that ImmutableString can be created"""
    s = ImmutableString("test")
    assert s == "test"


def test_immutable_string_is_instance_of_str():
    """Test that ImmutableString is an instance of str"""
    s = ImmutableString("test")
    assert isinstance(s, str)
    assert isinstance(s, ImmutableString)


def test_immutable_string_behaves_like_string():
    """Test that ImmutableString has normal string behavior"""
    s = ImmutableString("hello")

    # Test basic string operations
    assert len(s) == 5
    assert s.upper() == "HELLO"
    assert s[0] == "h"
    assert s[1:3] == "el"
    assert s + " world" == "hello world"
    assert "ell" in s


def test_immutable_string_with_unicode():
    """Test that ImmutableString works with Unicode strings"""
    s = ImmutableString("hello 世界")
    assert s == "hello 世界"
    assert isinstance(s, ImmutableString)


def test_immutable_string_empty():
    """Test that ImmutableString works with empty strings"""
    s = ImmutableString("")
    assert s == ""
    assert len(s) == 0
    assert isinstance(s, ImmutableString)
