use automerge::{Backend, Change};
use automerge_backend::{AutomergeError, SyncMessage, SyncState};
use automerge_protocol::{ChangeHash, UncompressedChange};

use pyo3::create_exception;
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyBytes, PyList};
use pyo3::wrap_pyfunction;
use pythonize::{depythonize, pythonize};

// PyBackend and PySyncState are opaque pointers
#[pyclass(unsendable, name = "Backend")]
struct PyBackend {
    backend: Backend,
}

#[pyclass(unsendable, name = "SyncState")]
struct PySyncState {
    state: SyncState,
}

#[pymethods]
impl PyBackend {
    pub fn apply_local_change(&mut self, change: &PyAny) -> PyResult<(Py<PyAny>, Py<PyBytes>)> {
        let change: UncompressedChange = depythonize(&change)?;
        let (patch, change) = self.backend.apply_local_change(change).to_py_err()?;
        let gil = Python::acquire_gil();
        let py = gil.python();
        let bytes = PyBytes::new(py, change.raw_bytes()).into_py(py);
        Ok((pythonize(py, &patch)?, bytes))
    }

    pub fn apply_changes(&mut self, changes: &PyList) -> PyResult<Py<PyAny>> {
        let changes = import_changes(changes)?;
        let patch = self.backend.apply_changes(changes).to_py_err()?;
        let gil = Python::acquire_gil();
        let py = gil.python();
        Ok(pythonize(py, &patch)?)
    }

    pub fn load_changes(&mut self, changes: &PyList) -> PyResult<()> {
        let changes = import_changes(changes)?;
        self.backend.load_changes(changes).to_py_err()?;
        Ok(())
    }

    #[new]
    fn new() -> Self {
        PyBackend {
            backend: Backend::init(),
        }
    }

    #[staticmethod]
    fn load(data: &PyBytes) -> PyResult<PyBackend> {
        let data = data.as_bytes().to_vec();
        let backend = Backend::load(data).to_py_err()?;
        Ok(PyBackend { backend })
    }

    pub fn get_patch(&self) -> PyResult<Py<PyAny>> {
        let patch = self.backend.get_patch().to_py_err()?;
        let gil = Python::acquire_gil();
        let py = gil.python();
        Ok(pythonize(py, &patch)?)
    }

    pub fn clone(&self) -> PyResult<PyBackend> {
        let cloned = self.backend.clone();
        Ok(PyBackend { backend: cloned })
    }

    pub fn save(&self) -> PyResult<Py<PyBytes>> {
        let bytes = self.backend.save().to_py_err()?;
        let gil = Python::acquire_gil();
        let py = gil.python();
        let bytes = PyBytes::new(py, &bytes).into_py(py);
        Ok(bytes)
    }

    pub fn get_all_changes(&self) -> PyResult<Vec<Py<PyBytes>>> {
        self.get_changes(None)
    }

    // I don't really want the API to accept an `Option` but its the easiest
    // way to implement `get_all_changes` (otherwise, I'd need to construct an empty `PyList`)
    pub fn get_changes(&self, deps: Option<&PyList>) -> PyResult<Vec<Py<PyBytes>>> {
        let deps: Vec<ChangeHash> = match deps {
            Some(deps) => depythonize(&deps)?,
            None => vec![],
        };
        let changes = self.backend.get_changes(&deps);
        Ok(export_changes(changes))
    }

    pub fn get_missing_deps(&self) -> PyResult<Py<PyAny>> {
        // TODO: ensure this usage of `get_missing_deps` is correct
        // (we previously did not need to pass in heads)
        let heads = self.backend.get_heads();
        let deps = self.backend.get_missing_deps(&heads);
        let gil = Python::acquire_gil();
        let py = gil.python();
        Ok(pythonize(py, &deps)?)
    }

    pub fn get_heads(&self) -> PyResult<Py<PyAny>> {
        let gil = Python::acquire_gil();
        let py = gil.python();
        let heads = pythonize(py, &self.backend.get_heads())?;
        Ok(heads)
    }

    // methods for the sync protocol
    pub fn generate_sync_message(
        &self,
        sync_state: &mut PySyncState,
    ) -> PyResult<Option<Py<PyBytes>>> {
        let msg = self.backend.generate_sync_message(&mut sync_state.state);
        Ok(match msg {
            Some(m) => {
                // The JS version returns the SyncMessage as a binary blob to improve pef
                // The Rust API doesn't b/c there's no perf improvement to encoding a Rust struct
                // as a binary blob. We use the Rust API & encode to a binary blob b/c
                // 1. improves perf??
                // 2. simplicity -- don't need to deal with sending complex objects between Rust &
                //  Python
                let bytes = m.encode().to_py_err()?;
                let gil = Python::acquire_gil();
                let py = gil.python();
                let bytes = PyBytes::new(py, &bytes);
                Some(bytes.into_py(py))
            }
            None => None,
        })
    }

    pub fn receive_sync_message(
        &mut self,
        sync_state: &mut PySyncState,
        msg: &[u8],
    ) -> PyResult<Option<Py<PyAny>>> {
        let msg = SyncMessage::decode(msg).to_py_err()?;
        let patch = self
            .backend
            .receive_sync_message(&mut sync_state.state, msg)
            .to_py_err()?;
        Ok(match patch {
            Some(p) => {
                let gil = Python::acquire_gil();
                let py = gil.python();
                let patch = pythonize(py, &p)?;
                Some(patch)
            }
            None => None,
        })
    }
}

// Even though this function is never called directly from Python, it
// returns a `PyResult` so its error can be passed easily to Python using the `?` operator.
fn import_changes(py_changes: &PyList) -> PyResult<Vec<Change>> {
    let mut changes = Vec::with_capacity(py_changes.len() as usize);
    for py_change in py_changes.iter() {
        let bytes: &PyBytes = py_change.downcast()?;
        let c = Change::from_bytes(bytes.as_bytes().to_vec()).to_py_err()?;
        changes.push(c);
    }
    Ok(changes)
}

fn export_changes(changes: Vec<&Change>) -> Vec<Py<PyBytes>> {
    let mut result = Vec::new();
    for c in changes {
        // QUESTION: Are there perf issues with this lock-acquisition inside a loop?
        let gil = Python::acquire_gil();
        let py = gil.python();
        let bytes = PyBytes::new(py, c.raw_bytes());
        result.push(bytes.into_py(py));
    }
    result
}

create_exception!(
    automerge_backend,
    PyAutomergeError,
    pyo3::exceptions::PyException
);

// See: https://users.rust-lang.org/t/convert-between-error-types-in-different-crates/58033/4
trait ResultExt<T> {
    fn to_py_err(self) -> PyResult<T>;
}

impl<T> ResultExt<T> for Result<T, AutomergeError> {
    fn to_py_err(self) -> PyResult<T> {
        match self {
            Ok(x) => Ok(x),
            Err(e) => Err(PyAutomergeError::new_err(format!("Automerge error: {}", e))),
        }
    }
}

// Turns a change object into compressed binary format
#[pyfunction]
fn encode_change(change: &PyAny) -> PyResult<Py<PyBytes>> {
    let change: UncompressedChange = depythonize(&change)?;
    let change: Change = change.into();
    let gil = Python::acquire_gil();
    let py = gil.python();
    // QUESTION: We could also just do change.raw_bytes().into(py)
    // Not sure if that would also return a PyBytes
    let bytes = PyBytes::new(py, change.raw_bytes());
    Ok(bytes.into_py(py))
}

#[pyfunction]
fn decode_change(change: &PyBytes) -> PyResult<Py<PyAny>> {
    let bytes = change.as_bytes();
    let change = Change::from_bytes(bytes.to_vec()).to_py_err()?;
    // TODO: For some reason I can't use `change.into`
    let change: UncompressedChange = change.decode();
    let gil = Python::acquire_gil();
    let py = gil.python();
    Ok(pythonize(py, &change)?)
}

// for sync protocol
#[pyfunction]
fn default_sync_state() -> PySyncState {
    PySyncState {
        state: SyncState {
            ..Default::default()
        },
    }
}

// TODO: wait for serde to be implemented on sync message
//#[pyfunction]
//fn decode_sync_message(bytes: &[u8]) -> PyResult<Py<PyAny>> {
//    let msg = SyncMessage::decode(bytes).to_py_err()?;
//    let gil = Python::acquire_gil();
//    let py = gil.python();
//    //let msg = pythonize(py, &msg);
//}

#[pymodule(automerge_backend)]
fn automerge_backend(py: Python, m: &PyModule) -> PyResult<()> {
    //m.add_function(wrap_pyfunction!(init, m)?)?;
    //m.add_function(wrap_pyfunction!(load, m)?)?;
    m.add_function(wrap_pyfunction!(encode_change, m)?)?;
    m.add_function(wrap_pyfunction!(decode_change, m)?)?;
    m.add_function(wrap_pyfunction!(default_sync_state, m)?)?;
    m.add_class::<PyBackend>()?;
    m.add("AutomergeError", py.get_type::<PyAutomergeError>())?;
    Ok(())
}
