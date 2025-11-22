//! Document actor types
//!
//! This module contains types related to document actors,
//! including SpawnArgs, message types, and the DocumentActor itself.

use pyo3::prelude::*;
use std::sync::{Arc, Mutex};

use super::io::PyIoTask;
use super::types::{PyDocumentActorId, PyDocumentId};
use crate::CURRENT_DOC_CONTEXT;

/// Wrapper for samod_core::actors::document::SpawnArgs
///
/// Arguments for spawning a new document actor.
#[pyclass(name = "SpawnArgs")]
#[derive(Clone)]
pub struct PySpawnArgs {
    inner: samod_core::actors::document::SpawnArgs,
}

#[pymethods]
impl PySpawnArgs {
    /// Get the document actor ID
    #[getter]
    fn actor_id(&self) -> PyDocumentActorId {
        PyDocumentActorId(self.inner.actor_id())
    }

    /// Get the document ID
    #[getter]
    fn document_id(&self) -> PyDocumentId {
        PyDocumentId(self.inner.document_id().clone())
    }

    fn __repr__(&self) -> String {
        format!(
            "SpawnArgs(actor_id={}, document_id={})",
            self.inner.actor_id(),
            self.inner.document_id()
        )
    }
}

impl From<samod_core::actors::document::SpawnArgs> for PySpawnArgs {
    fn from(args: samod_core::actors::document::SpawnArgs) -> Self {
        PySpawnArgs { inner: args }
    }
}

/// Wrapper for samod_core::actors::HubToDocMsg
///
/// Message sent from the Hub to a document actor.
#[pyclass(name = "HubToDocMsg")]
#[derive(Clone)]
pub struct PyHubToDocMsg {
    inner: samod_core::actors::HubToDocMsg,
}

#[pymethods]
impl PyHubToDocMsg {
    fn __repr__(&self) -> String {
        "HubToDocMsg(...)".to_string()
    }
}

impl From<samod_core::actors::HubToDocMsg> for PyHubToDocMsg {
    fn from(msg: samod_core::actors::HubToDocMsg) -> Self {
        PyHubToDocMsg { inner: msg }
    }
}

/// Wrapper for samod_core::actors::DocToHubMsg
///
/// Message sent from a document actor to the Hub.
#[pyclass(name = "DocToHubMsg")]
#[derive(Clone)]
pub struct PyDocToHubMsg {
    pub(crate) inner: samod_core::actors::DocToHubMsg,
}

#[pymethods]
impl PyDocToHubMsg {
    fn __repr__(&self) -> String {
        "DocToHubMsg(...)".to_string()
    }
}

impl From<samod_core::actors::DocToHubMsg> for PyDocToHubMsg {
    fn from(msg: samod_core::actors::DocToHubMsg) -> Self {
        PyDocToHubMsg { inner: msg }
    }
}

// ===== Document Actor Results =====

/// Wrapper for samod_core::actors::document::DocActorResult
///
/// Result from processing a message in a document actor.
#[pyclass(name = "DocActorResult")]
pub struct PyDocActorResult {
    pub(crate) inner: samod_core::actors::document::DocActorResult,
}

#[pymethods]
impl PyDocActorResult {
    /// Get IO tasks that need to be executed
    #[getter]
    fn io_tasks<'py>(&self, py: Python<'py>) -> Vec<PyIoTask> {
        self.inner
            .io_tasks
            .iter()
            .map(|task| super::io::document_io_task_ref_to_py(py, task.task_id, &task.action))
            .collect()
    }

    /// Get messages to send to the Hub (outgoing messages)
    #[getter]
    fn outgoing_messages(&self) -> Vec<PyDocToHubMsg> {
        self.inner
            .outgoing_messages
            .iter()
            .map(|msg| PyDocToHubMsg::from(msg.clone()))
            .collect()
    }

    /// Get ephemeral messages to send to the Hub
    ///
    /// These are serialized messages (Vec<u8>) for ephemeral data
    #[getter]
    fn ephemeral_messages<'py>(&self, py: Python<'py>) -> Vec<Bound<'py, pyo3::types::PyBytes>> {
        self.inner
            .ephemeral_messages
            .iter()
            .map(|msg_bytes| pyo3::types::PyBytes::new(py, msg_bytes))
            .collect()
    }

    /// Check if the actor is stopped
    #[getter]
    fn stopped(&self) -> bool {
        self.inner.stopped
    }

    fn __repr__(&self) -> String {
        format!(
            "DocActorResult(io_tasks={}, outgoing_messages={}, ephemeral_messages={}, stopped={})",
            self.inner.io_tasks.len(),
            self.inner.outgoing_messages.len(),
            self.inner.ephemeral_messages.len(),
            self.inner.stopped
        )
    }
}

// ===== WithDoc Result =====

/// Result from accessing a document via with_document
#[pyclass(name = "WithDocResult")]
pub struct PyWithDocResult {
    /// The return value from the user's function (as a PyObject)
    #[pyo3(get)]
    pub return_value: PyObject,

    /// Patches describing changes made during the callback
    #[pyo3(get)]
    pub patches: Vec<crate::PyPatch>,

    pub(crate) inner: samod_core::actors::document::WithDocResult<()>,
}

#[pymethods]
impl PyWithDocResult {
    /// Get IO tasks that need to be executed
    #[getter]
    fn io_tasks<'py>(&self, py: Python<'py>) -> Vec<PyIoTask> {
        self.inner
            .actor_result
            .io_tasks
            .iter()
            .map(|task| super::io::document_io_task_ref_to_py(py, task.task_id, &task.action))
            .collect()
    }

    /// Get outgoing messages to send to the Hub
    #[getter]
    fn outgoing_messages(&self) -> Vec<PyDocToHubMsg> {
        self.inner
            .actor_result
            .outgoing_messages
            .iter()
            .map(|msg| PyDocToHubMsg::from(msg.clone()))
            .collect()
    }

    /// Get ephemeral messages to send to the Hub
    ///
    /// These are serialized messages (Vec<u8>) for ephemeral data
    #[getter]
    fn ephemeral_messages<'py>(&self, py: Python<'py>) -> Vec<Bound<'py, pyo3::types::PyBytes>> {
        self.inner
            .actor_result
            .ephemeral_messages
            .iter()
            .map(|msg_bytes| pyo3::types::PyBytes::new(py, msg_bytes))
            .collect()
    }

    fn __repr__(&self) -> String {
        format!(
            "WithDocResult(io_tasks={}, outgoing={}, ephemeral={})",
            self.inner.actor_result.io_tasks.len(),
            self.inner.actor_result.outgoing_messages.len(),
            self.inner.actor_result.ephemeral_messages.len()
        )
    }
}

// ===== Document Actor =====

/// Wrapper for samod_core::actors::document::DocumentActor
///
/// The document actor manages a single Automerge document, handling
/// persistence and synchronization for that document.
#[pyclass(name = "DocumentActor")]
pub struct PyDocumentActor {
    // Use Arc<Mutex<...>> for thread safety across Python's async runtime
    inner: Arc<Mutex<samod_core::actors::document::DocumentActor>>,
}

#[pymethods]
impl PyDocumentActor {
    /// Create a new document actor from spawn arguments
    ///
    /// Returns tuple of (DocumentActor, initial DocActorResult)
    #[staticmethod]
    fn new(now: f64, spawn_args: &PySpawnArgs) -> PyResult<(Self, PyDocActorResult)> {
        let timestamp = samod_core::UnixTimestamp::from_millis((now * 1000.0) as u128);
        let (actor, initial_result) =
            samod_core::actors::document::DocumentActor::new(timestamp, spawn_args.inner.clone());
        Ok((
            PyDocumentActor {
                inner: Arc::new(Mutex::new(actor)),
            },
            PyDocActorResult {
                inner: initial_result,
            },
        ))
    }

    /// Handle a message from the Hub
    fn handle_message(&self, now: f64, msg: &PyHubToDocMsg) -> PyResult<PyDocActorResult> {
        let timestamp = samod_core::UnixTimestamp::from_millis((now * 1000.0) as u128);
        let mut actor = self.inner.lock().unwrap();
        let result = actor
            .handle_message(timestamp, msg.inner.clone())
            .map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                    "Document actor error: {:?}",
                    e
                ))
            })?;
        Ok(PyDocActorResult { inner: result })
    }

    /// Handle completion of an IO operation
    fn handle_io_complete(
        &self,
        now: f64,
        io_result: &super::io::PyIoResult,
    ) -> PyResult<PyDocActorResult> {
        let timestamp = samod_core::UnixTimestamp::from_millis((now * 1000.0) as u128);
        let mut actor = self.inner.lock().unwrap();

        // Convert PyIoResult to Rust IoResult<DocumentIoResult>
        let rust_io_result = io_result.to_document_io_result()?;

        let result = actor
            .handle_io_complete(timestamp, rust_io_result)
            .map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                    "Document actor error: {:?}",
                    e
                ))
            })?;
        Ok(PyDocActorResult { inner: result })
    }

    /// Get a read-only document reference backed by this DocumentActor
    ///
    /// Returns a Document that acquires the actor's mutex on each read operation.
    /// This allows direct property access without callbacks, with the trade-off
    /// of mutex acquisition overhead per operation.
    ///
    /// The returned document is read-only. Any attempt to create a transaction
    /// or modify it will result in an error.
    ///
    /// # Example
    /// ```python
    /// doc = actor.get_document()
    /// value = doc.get(ROOT, "key")  # Acquires lock, reads, releases lock
    /// ```
    fn get_document(&self) -> PyResult<crate::Document> {
        Ok(crate::Document::new_from_actor(self.inner.clone()))
    }

    /// Access the document with a Python callable
    ///
    /// The callable will receive a borrowed document reference that can be used
    /// to read and modify the document. The callable's return value is captured
    /// and returned in the WithDocResult.
    fn with_document(&self, py: Python, now: f64, func: PyObject) -> PyResult<PyWithDocResult> {
        use std::cell::RefCell;

        let timestamp = samod_core::UnixTimestamp::from_millis((now * 1000.0) as u128);

        // Check if we're already in a change callback - nested changes are not supported
        let already_in_callback = CURRENT_DOC_CONTEXT.with(|ctx| ctx.borrow().is_some());
        if already_in_callback {
            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "Cannot call change() inside another change() callback. \
                 Nested changes are not supported. Please restructure your code \
                 to perform changes sequentially rather than nesting them.",
            ));
        }

        // Capture the Arc identity for this actor
        let arc_id = Arc::as_ptr(&self.inner) as usize;

        let mut actor = self.inner.lock().unwrap();

        // Use RefCell to capture the return value, patches, and potential errors from the Python function
        let return_value_cell: RefCell<Option<PyObject>> = RefCell::new(None);
        let patches_cell: RefCell<Vec<crate::PyPatch>> = RefCell::new(Vec::new());
        let error_cell: RefCell<Option<PyErr>> = RefCell::new(None);

        // Call with_document with a closure that invokes the Python function
        let result = actor
            .with_document(timestamp, |doc| {
                // Set thread-local context for reentrant access
                // This allows doc() references to work inside change() callbacks
                CURRENT_DOC_CONTEXT.with(|ctx| {
                    *ctx.borrow_mut() = Some((arc_id, doc as *const automerge::Automerge));
                });

                // Capture heads before the callback
                let before_heads: Vec<_> = doc.get_heads().iter().cloned().collect();

                // We need to acquire the GIL to call Python code
                Python::with_gil(|py| {
                    // Create a borrowed document wrapper around the mutable reference
                    // SAFETY: The Document only exists within this callback scope,
                    // and the pointer is guaranteed valid for the duration of the Python call.
                    // The Python callback executes synchronously and cannot store the reference.
                    let borrowed_doc = unsafe { crate::Document::new_borrowed(doc) };

                    // Convert to Python object
                    let py_doc_obj = match Py::new(py, borrowed_doc) {
                        Ok(obj) => obj,
                        Err(e) => {
                            *error_cell.borrow_mut() = Some(e);
                            return;
                        }
                    };

                    // Call the Python function with the borrowed document
                    let ret = match func.call1(py, (&py_doc_obj,)) {
                        Ok(r) => r,
                        Err(e) => {
                            // Capture the error to be raised after the closure completes
                            *error_cell.borrow_mut() = Some(e);
                            // Invalidate the borrowed document before returning
                            py_doc_obj.borrow(py).invalidate();
                            return;
                        }
                    };

                    // Store the return value
                    *return_value_cell.borrow_mut() = Some(ret);

                    // Invalidate the borrowed document to prevent use-after-callback
                    // Even if someone holds a reference to py_doc_obj after this,
                    // they'll get a clear panic instead of undefined behavior
                    py_doc_obj.borrow(py).invalidate();

                    // py_doc_obj is dropped here, which is safe because we're still in the callback
                });

                // Clear thread-local context immediately after the Python callback
                // This must happen whether the callback succeeded or failed
                CURRENT_DOC_CONTEXT.with(|ctx| {
                    *ctx.borrow_mut() = None;
                });

                // Check if an error occurred during the Python callback
                if error_cell.borrow().is_some() {
                    // Don't generate patches if there was an error
                    return ();
                }

                // Capture heads after the callback
                let after_heads: Vec<_> = doc.get_heads().iter().cloned().collect();

                // Generate patches if heads changed
                if before_heads != after_heads {
                    let patches = doc.diff(&before_heads, &after_heads);
                    *patches_cell.borrow_mut() = patches
                        .into_iter()
                        .map(|p| crate::PyPatch(p.clone()))
                        .collect();
                }

                // Return unit type for Rust
                ()
            })
            .map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                    "Document actor error: {:?}",
                    e
                ))
            })?;

        // Check if an error occurred during the Python callback and raise it
        if let Some(err) = error_cell.into_inner() {
            return Err(err);
        }

        // Extract the return value and patches
        let return_value = return_value_cell.into_inner().unwrap_or_else(|| py.None());
        let patches = patches_cell.into_inner();

        Ok(PyWithDocResult {
            return_value,
            patches,
            inner: result,
        })
    }

    fn __repr__(&self) -> String {
        "DocumentActor(...)".to_string()
    }
}
