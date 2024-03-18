# Automerge-py

Python bindings for [Automerge](https://github.com/automerge/automerge).

This is a low-level library with few concessions to ergonomics, meant to interact directly with the low-level Automerge API.
Additional API that is more ergonomic is being added into the repository as this project evolves.

Note: There is an earlier set of Python bindings for Automerge at
[automerge/automerge-py](https://github.com/automerge/automerge-py), but that
project binds to a very outdated version of Automerge.

## Quickstart

Install the bindings with `pip install automerge`.

Now you can create a document and do all sorts of Automerge things with it!

> Note: this package currently only contains `automerge.core`, which is a
> low-level wrapper around the [Rust automerge
> library](https://docs.rs/automerge/latest/automerge/index.html).
>
> A more ergonomic wrapper is currently in the works.

```py
from automerge.core import Document, ROOT, ObjType, ScalarType

doc = Document()
with doc.transaction() as tx:
    list = tx.put_object(ROOT, "colours", ObjType.List)
    tx.insert(list, 0, ScalarType.Str, "blue")
    tx.insert(list, 1, ScalarType.Str, "red")

doc2 = doc.fork()
with doc2.transaction() as tx:
    tx.insert(list, 0, ScalarType.Str, "green")

with doc.transaction() as tx:
    tx.delete(list, 0)

doc.merge(doc2)  # `doc` now contains {"colours": ["green", "red"]}
```

## Developing

```bash
# Create venv in "env" folder (required by maturin)
python3 -m venv env
# Activate venv
source ./env/bin/activate
# Install maturin
pip install maturin
# Build the bindings
maturin develop
```
