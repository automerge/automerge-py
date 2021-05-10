# Python Frontend for Automerge

The Python frontend for [automerge-rs](https://github.com/automerge/automerge-rs)

## Build

```sh
# Install maturin
pip install maturin
# Create venv in "env" folder (required by maturin)
python3 -m venv env
# Activate venv
source ./env/bin/activate

# Build automerge_backend (Python bindings to Rust) and install it as a Python module
maturin develop
```

## Usage

### With Integrated Backend

In this mode, the front end (pure Python) and backend (Python-Rust bindings) run on
the same thread. The user does not need to interact directly with the backend.

```python3
from automerge_backend import Backend
from automerge import doc

d0 = doc.Doc(backend=Backend(), initial_data={'foo': 'bar'})
bin_change_1 = d0.local_bin_changes.pop()

with d0 as d:
  d['foo'] = 'baz'

bin_change_2 = d0.local_bin_changes.pop()

# Send the binary changes over the network to other Automerge clients...
```

#### Without Integrated Backend

Sometimes you want to run the frontend on a UI thread, but let the backend run on a
different thread that has less latency requirements. The Python frontend supports this.

```python3
from automerge import doc

d0 = doc.Doc(initial_data={'foo': 'bar'})
with d0 as d:
  d0['foo'] = 'baz'

# Send this change to the backend
local_change = d0.local_changes.pop()

# Somewhere on the backend thread
from automerge_backend import Backend
b = Backend()
patch, change_encoded_as_bin = b.apply_local_change(local_change)

# Back on the UI thread
d0.apply_patch(patch)
```
