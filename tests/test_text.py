from automerge import Document, ImmutableString


class TestTextCreation:
    """Test creating Text objects via maps and lists"""

    def test_map_string_creates_text(self):
        """Assigning a string to a map creates a Text object"""
        doc = Document()
        with doc.change() as d:
            d["content"] = "Hello, world!"

        text = doc["content"]
        assert type(text).__name__ == "Text"
        assert str(text) == "Hello, world!"

    def test_map_immutable_string_creates_scalar(self):
        """Assigning an ImmutableString to a map creates a scalar string"""
        doc = Document()
        with doc.change() as d:
            d["version"] = ImmutableString("1.0.0")

        version = doc["version"]
        assert type(version).__name__ == "str"
        assert version == "1.0.0"

    def test_list_string_creates_text(self):
        """Assigning a string to a list creates a Text object"""
        doc = Document()
        with doc.change() as d:
            d["items"] = []
            d["items"][0] = "First item"

        text = doc["items"][0]
        assert type(text).__name__ == "Text"
        assert str(text) == "First item"

    def test_list_immutable_string_creates_scalar(self):
        """Assigning an ImmutableString to a list creates a scalar string"""
        doc = Document()
        with doc.change() as d:
            d["tags"] = []
            d["tags"][0] = ImmutableString("alpha")

        tag = doc["tags"][0]
        assert type(tag).__name__ == "str"
        assert tag == "alpha"

    def test_list_insert_string_creates_text(self):
        """Inserting a string into a list creates a Text object"""
        doc = Document()
        with doc.change() as d:
            d["messages"] = []
            d["messages"].insert(0, "Hello")

        text = doc["messages"][0]
        assert type(text).__name__ == "Text"
        assert str(text) == "Hello"

    def test_assigning_write_proxy_to_map_field(self):
        """Assigning a Text object to a new field creates a Text object"""
        doc = Document()
        with doc.change() as d:
            # create a text object
            d["text"] = "Hello"
            d["text2"] = d["text"]

    def test_assigning_read_proxy_to__map_field_works(self):
        """Assigning a Text object to a new field creates a Text object"""
        otherdoc = Document()
        with otherdoc.change() as d:
            d["text"] = "hello"

        doc = Document()
        with doc.change() as d2:
            d2["text"] = otherdoc["text"]

        assert doc["text"] == "hello"

    def test_appending_write_proxy_to_string(self):
        """Assigning a Text object to a new field creates a Text object"""
        doc = Document()
        with doc.change() as d:
            d["text"] = "hello"
            d["list"] = []
            d["list"][0] = d["text"]
            assert d["list"][0] == "hello"

        assert doc["list"][0] == "hello"

    def test_inserting_write_proxy_in_string(self):
        """Assigning a Text object to a new field creates a Text object"""
        doc = Document()
        with doc.change() as d:
            d["text"] = "hello"
            d["list"] = []
            d["list"].insert(0, d["text"])
            assert d["list"][0] == "hello"

        assert doc["list"][0] == "hello"

    def test_updating_write_proxy_in_string(self):
        otherdoc = Document()
        with otherdoc.change() as d:
            d["text"] = "hello"
        doc = Document()
        with doc.change() as d:
            d["text"] = "hello"
            d["list"] = []
            d["list"][0] = 1
            d["list"].insert(0, otherdoc["text"])
            assert d["list"][0] == "hello"
            assert len(d["list"]) == 2

        assert doc["list"][0] == "hello"

    def test_inserting_read_proxy_in_string(self):
        """Assigning a Text object to a new field creates a Text object"""
        otherdoc = Document()
        with otherdoc.change() as d:
            d["text"] = "hello"

        doc = Document()
        with doc.change() as d:
            d["text"] = "hello"
            d["list"] = []
            d["list"][0] = otherdoc["text"]
            assert d["list"][0] == "hello"

        assert doc["list"][0] == "hello"

    def test_updating_read_proxy_in_string(self):
        """Assigning a Text object to a new field creates a Text object"""
        otherdoc = Document()
        with otherdoc.change() as d:
            d["text"] = "hello"

        doc = Document()
        with doc.change() as d:
            d["text"] = "hello"
            d["list"] = []
            d["list"][0] = 1
            d["list"][0] = otherdoc["text"]
            assert d["list"][0] == "hello"
            assert len(d["list"]) == 1

        assert doc["list"][0] == "hello"


class TestTextReadOperations:
    """Test reading Text objects"""

    def test_text_str(self):
        """Text objects can be converted to strings"""
        doc = Document()
        with doc.change() as d:
            d["text"] = "Hello, world!"

        assert str(doc["text"]) == "Hello, world!"

    def test_text_eq_str(self):
        """Text objects can be compared to strings"""
        doc = Document()
        with doc.change() as d:
            d["text"] = "Hello, world!"

        assert doc["text"] == "Hello, world!"
        assert doc["text"] != "something else"

    def test_text_len(self):
        """Text objects have length"""
        doc = Document()
        with doc.change() as d:
            d["text"] = "Hello"

        assert len(doc["text"]) == 5

    def test_text_indexing(self):
        """Text objects support indexing"""
        doc = Document()
        with doc.change() as d:
            d["text"] = "Hello"

        assert doc["text"][0] == "H"
        assert doc["text"][1] == "e"
        assert doc["text"][-1] == "o"

    def test_text_slicing(self):
        """Text objects support slicing"""
        doc = Document()
        with doc.change() as d:
            d["text"] = "Hello, world!"

        assert doc["text"][0:5] == "Hello"
        assert doc["text"][7:12] == "world"
        assert doc["text"][:5] == "Hello"
        assert doc["text"][7:] == "world!"

    def test_text_repr(self):
        """Text objects have a useful repr"""
        doc = Document()
        with doc.change() as d:
            d["text"] = "Hello"

        repr_str = repr(doc["text"])
        assert "Text(" in repr_str
        assert "Hello" in repr_str


class TestTextWriteOperations:
    """Test mutating Text objects"""

    def test_text_insert(self):
        """Text objects support insert operation"""
        doc = Document()
        with doc.change() as d:
            d["text"] = "Hello, world!"

        with doc.change() as d:
            d["text"].insert(7, "Python ")

        assert str(doc["text"]) == "Hello, Python world!"

    def test_text_eq_str(self):
        """Text objects can be compared to strings"""
        doc = Document()
        with doc.change() as d:
            d["text"] = "Hello, world!"
            assert d["text"] == "Hello, world!"
            assert d["text"] != "something else"

    def test_text_delete(self):
        """Text objects support delete operation"""
        doc = Document()
        with doc.change() as d:
            d["text"] = "Hello, world!"

        with doc.change() as d:
            d["text"].delete(5, 8)  # Delete ", world!"

        assert str(doc["text"]) == "Hello"

    def test_text_splice(self):
        """Text objects support splice operation"""
        doc = Document()
        with doc.change() as d:
            d["text"] = "Hello, world!"

        with doc.change() as d:
            d["text"].splice(0, 5, "Goodbye")  # Replace "Hello" with "Goodbye"

        assert str(doc["text"]) == "Goodbye, world!"

    def test_multiple_edits_in_transaction(self):
        """Multiple text edits in one transaction"""
        doc = Document()
        with doc.change() as d:
            d["text"] = "Hello"
            d["text"].insert(5, ", world")
            d["text"].insert(12, "!")

        assert str(doc["text"]) == "Hello, world!"

    def test_text_mutation_within_list(self):
        """Text objects in lists can be mutated"""
        doc = Document()
        with doc.change() as d:
            d["items"] = []
            d["items"][0] = "First"

        with doc.change() as d:
            d["items"][0].insert(5, " Item")

        assert str(doc["items"][0]) == "First Item"


class TestTextEdgeCases:
    """Test edge cases with Text objects"""

    def test_empty_string(self):
        """Empty strings create valid Text objects"""
        doc = Document()
        with doc.change() as d:
            d["text"] = ""

        text = doc["text"]
        assert type(text).__name__ == "Text"
        assert str(text) == ""
        assert len(text) == 0

    def test_unicode_text(self):
        """Unicode strings work correctly"""
        doc = Document()
        with doc.change() as d:
            d["text"] = "Hello 世界 🌍"

        assert str(doc["text"]) == "Hello 世界 🌍"

    def test_unicode_insert(self):
        """Inserting unicode text works"""
        doc = Document()
        with doc.change() as d:
            d["text"] = "Hello"

        with doc.change() as d:
            d["text"].insert(5, " 世界")

        assert str(doc["text"]) == "Hello 世界"

    def test_very_long_string(self):
        """Long strings work correctly"""
        long_text = "a" * 10000
        doc = Document()
        with doc.change() as d:
            d["text"] = long_text

        assert str(doc["text"]) == long_text
        assert len(doc["text"]) == 10000

    def test_newlines_in_text(self):
        """Text with newlines works"""
        doc = Document()
        with doc.change() as d:
            d["text"] = "Line 1\nLine 2\nLine 3"

        assert str(doc["text"]) == "Line 1\nLine 2\nLine 3"

    def test_special_characters(self):
        """Special characters work correctly"""
        doc = Document()
        with doc.change() as d:
            d["text"] = "Special: \t\n\r\\ \"quotes\" 'apostrophes'"

        assert "\t" in str(doc["text"])
        assert "\n" in str(doc["text"])


class TestTextRemoteEdits:
    """Test Text objects reflect changes"""

    def test_proxy_reflects_subsequent_edits(self):
        """Text proxy reflects edits made after getting the proxy"""
        doc = Document()
        with doc.change() as d:
            d["notes"] = "Original text"

        # Get a reference to the text proxy
        my_notes = doc["notes"]
        assert str(my_notes) == "Original text"

        # Make another edit
        with doc.change() as d:
            d["notes"].insert(0, "Updated: ")

        # The original proxy should reflect the new state
        assert str(my_notes) == "Updated: Original text"

    def test_multiple_proxies_same_text(self):
        """Multiple proxies to same text all reflect current state"""
        doc = Document()
        with doc.change() as d:
            d["text"] = "Hello"

        # Get multiple references
        ref1 = doc["text"]
        ref2 = doc["text"]

        # Edit the text
        with doc.change() as d:
            d["text"].insert(5, " World")

        # Both references should show the update
        assert str(ref1) == "Hello World"
        assert str(ref2) == "Hello World"


class TestImmutableStringVsText:
    """Test differences between ImmutableString and Text"""

    def test_immutable_string_is_regular_str(self):
        """ImmutableString values are plain Python strings"""
        doc = Document()
        with doc.change() as d:
            d["scalar"] = ImmutableString("test")

        value = doc["scalar"]
        assert isinstance(value, str)
        assert type(value).__name__ == "str"

    def test_text_is_proxy(self):
        """Regular string values are Text proxies"""
        doc = Document()
        with doc.change() as d:
            d["text"] = "test"

        value = doc["text"]
        assert type(value).__name__ == "Text"
        assert not isinstance(value, str)

    def test_immutable_string_cannot_be_mutated(self):
        """ImmutableString values don't have mutation methods"""
        doc = Document()
        with doc.change() as d:
            d["scalar"] = ImmutableString("test")

        value = doc["scalar"]
        assert not hasattr(value, "insert")
        assert not hasattr(value, "delete")
        assert not hasattr(value, "splice")

    def test_text_can_be_mutated(self):
        """Text values have mutation methods"""
        doc = Document()
        with doc.change() as d:
            d["text"] = "test"

        with doc.change() as d:
            text = d["text"]
            assert hasattr(text, "insert")
            assert hasattr(text, "delete")
            assert hasattr(text, "splice")

    def test_mixed_strings_in_document(self):
        """Document can contain both Text and scalar strings"""
        doc = Document()
        with doc.change() as d:
            d["text"] = "Editable text"
            d["version"] = ImmutableString("1.0.0")
            d["items"] = []
            d["items"][0] = "Editable item"
            d["items"][1] = ImmutableString("Immutable item")

        assert type(doc["text"]).__name__ == "Text"
        assert type(doc["version"]).__name__ == "str"
        assert type(doc["items"][0]).__name__ == "Text"
        assert type(doc["items"][1]).__name__ == "str"

    def test_text_inheritance_and_types(self):
        """Verify the inheritance and types of Text and MutableText."""
        from automerge import (
            MutableText,
            Text,
        )  # Import here to avoid circular dependencies if Text is not fully defined yet

        doc = Document()
        with doc.change() as d:
            d["read_text"] = "hello"
            d["mutable_text"] = "world"

        read_text_obj = doc["read_text"]
        with doc.change() as d:
            mutable_text_obj = d["mutable_text"]

        # Verify Text object
        assert isinstance(read_text_obj, Text)
        assert not isinstance(read_text_obj, MutableText)

        # Verify MutableText object
        assert isinstance(mutable_text_obj, MutableText)
        assert isinstance(mutable_text_obj, Text)  # Key assertion for inheritance

        # Verify attributes exist on MutableText
        assert hasattr(mutable_text_obj, "insert")
        assert hasattr(mutable_text_obj, "delete")
        assert hasattr(mutable_text_obj, "splice")

        # Verify attributes do not exist on read_text_obj (read-only)
        assert not hasattr(read_text_obj, "insert")
        assert not hasattr(read_text_obj, "delete")
        assert not hasattr(read_text_obj, "splice")
