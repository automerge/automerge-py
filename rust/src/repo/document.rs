//! Document actor types
//!
//! This module contains types related to document actors,
//! including SpawnArgs, message types, and the DocumentActor itself.

use pyo3::prelude::*;
use samod_core::actors::document::document_actor::WithDocGuard;
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

    fn __repr__(&self) -> String {
        "DocumentActor(...)".to_string()
    }

    /// Begin a change session for the context manager API.
    ///
    /// Returns a ChangeGuard that holds the mutex and allows modifications.
    /// The guard must be ended via end_change() to commit or rollback.
    fn begin_change(&self, py: Python) -> PyResult<ChangeGuard> {
        // Check if we're already in a change - nested changes are not supported
        let already_in_change = CURRENT_DOC_CONTEXT.with(|ctx| ctx.borrow().is_some());
        if already_in_change {
            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "Cannot nest change() calls. Please restructure your code \
                 to perform changes sequentially rather than nesting them.",
            ));
        }

        let arc_id = Arc::as_ptr(&self.inner) as usize;

        // Create the session wrapper which handles all unsafe lifetime management
        let mut session = ChangeSession::new(self.inner.clone())?;

        // Get the document pointer from the session
        let doc_ptr = session.doc_ptr();

        // Capture heads before modification for patch generation
        // SAFETY: doc_ptr is valid as long as session is alive
        let before_heads: Vec<_> = unsafe { &*doc_ptr }.get_heads().iter().cloned().collect();

        // Create borrowed Document wrapper
        // SAFETY: The document pointer is valid as long as the session is held
        // because the session is holding a reference to the DocumentActor
        let borrowed_doc = unsafe { crate::Document::new_borrowed(&mut *doc_ptr) };
        let py_doc = Py::new(py, borrowed_doc)?;

        // Create transaction on the borrowed document
        let mut doc_ref = py_doc.borrow_mut(py);
        let py_tx = doc_ref.transaction()?;
        drop(doc_ref);
        let py_tx = Py::new(py, py_tx)?;

        // Set thread-local context to mark this actor as in-change
        // change() calls can detect and error
        CURRENT_DOC_CONTEXT.with(|ctx| {
            *ctx.borrow_mut() = Some(arc_id);
        });

        Ok(ChangeGuard {
            session: Some(session),
            document: Some(py_doc),
            transaction: Some(py_tx),
            before_heads,
            arc_id,
        })
    }
}

/// A safety wrapper ensuring correct lifetime management for the actor lock and document guard.
///
/// # Safety Invariants
///
/// This struct maintains several critical safety invariants that must be preserved to avoid
/// Undefined Behavior (UB), primarily Use-After-Free (UAF) and Data Races.
///
/// 1. **Anchor Liveness**: The `_anchor` field holds a strong `Arc` reference to the `Mutex`.
///    This ensures the `Mutex`'s memory is not deallocated while `actor_guard` references it.
///    Even if all external references to the actor are dropped (e.g., Python `actor` object is GC'd),
///    this session keeps the underlying Mutex alive.
///
/// 2. **Stable Guard Address**: The `actor_guard` is boxed immediately after locking.
///    This pins the `MutexGuard` to a stable heap address. This is required because
///    `with_doc_guard` borrows from it. If `actor_guard` moved, the borrow would be invalidated.
///
/// 3. **Drop Order (Borrower before Owner)**: `with_doc_guard` borrows from `actor_guard`.
///    Therefore, `with_doc_guard` MUST be dropped before `actor_guard`.
///    - The `Drop` implementation explicitly drops `with_doc_guard` first.
///    - The `commit` method consumes `with_doc_guard` before `actor_guard` is dropped.
///
/// 4. **Lifetime Confinement**: We use `unsafe` to transmute lifetimes to `'static`.
///    This is valid ONLY because:
///    - We own all the involved objects in this struct.
///    - We never expose the `'static` references outside this struct's interface.
///    - We strictly enforce the internal dependency order in `Drop` and `commit`.
struct ChangeSession {
    /// The anchor keeping the Mutex alive.
    _anchor: Arc<Mutex<samod_core::actors::document::DocumentActor>>,

    /// The owner of the lock. Dropped LAST.
    /// Wrapped in Option to allow taking in Drop/commit.
    actor_guard:
        Option<Box<std::sync::MutexGuard<'static, samod_core::actors::document::DocumentActor>>>,

    /// The borrower of the lock. Dropped FIRST.
    with_doc_guard: Option<WithDocGuard<'static>>,
}

impl ChangeSession {
    /// Create a new session. This locks the mutex and prepares the guards.
    fn new(anchor: Arc<Mutex<samod_core::actors::document::DocumentActor>>) -> PyResult<Self> {
        // 1. Acquire the lock
        let guard = anchor.lock().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to acquire actor lock: {}",
                e
            ))
        })?;

        // 2. Box to pin memory address
        let boxed_guard = Box::new(guard);

        // 3. Transmute guard to 'static
        // SAFETY: We hold `_anchor` which ensures the Mutex outlives this struct.
        // We hold `boxed_guard` which ensures the Guard outlives the fields that borrow from it.
        let mut static_actor_guard: Box<
            std::sync::MutexGuard<'static, samod_core::actors::document::DocumentActor>,
        > = unsafe { std::mem::transmute(boxed_guard) };

        // 4. Create the borrower
        // Note: begin_modification() takes &mut DocumentActor
        let with_doc_guard = static_actor_guard.begin_modification().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to begin modification: {:?}",
                e
            ))
        })?;

        // 5. Transmute borrower to 'static
        // SAFETY: We ensure in `Drop` and `commit` that this field is destroyed before `actor_guard`.
        let static_with_doc_guard: WithDocGuard<'static> =
            unsafe { std::mem::transmute(with_doc_guard) };

        Ok(Self {
            _anchor: anchor,
            actor_guard: Some(static_actor_guard),
            with_doc_guard: Some(static_with_doc_guard),
        })
    }

    /// Access the raw document pointer.
    fn doc_ptr(&mut self) -> *mut automerge::Automerge {
        // SAFETY: Unwrapping is safe because None only happens during Drop/Commit
        self.with_doc_guard.as_mut().unwrap().doc() as *mut automerge::Automerge
    }

    /// Commit the changes and consume the session.
    fn commit(
        mut self,
        timestamp: samod_core::UnixTimestamp,
    ) -> samod_core::actors::document::DocActorResult {
        // 1. Take borrower
        let with_doc = self.with_doc_guard.take().expect("Session already closed");

        // 2. Commit (consumes borrower)
        let result = with_doc.commit(timestamp);

        // 3. `self` drops here.
        // `actor_guard` is still in `self.actor_guard`.
        // `Drop` will run. It will see `with_doc_guard` is None (we took it), so it will just drop `actor_guard`.
        // Then `_anchor` drops.
        // Everything happens in safe order.
        result
    }
}

impl Drop for ChangeSession {
    fn drop(&mut self) {
        // If we are dropping (e.g. rollback or panic), we must ensure order.
        if self.with_doc_guard.is_some() {
            // 1. Drop borrower
            self.with_doc_guard.take();
            // 2. Drop owner (releases lock)
            self.actor_guard.take();
            // 3. Anchor drops naturally
        }
    }
}

/// Guard for a document change session (context manager support).
///
/// This holds the mutex lock and allows modifications to the document.
/// Must be ended via end_change() to commit or rollback the changes.
///
/// This type is not thread-safe (unsendable) because it holds a MutexGuard.
#[pyclass(name = "ChangeGuard", unsendable)]
pub struct ChangeGuard {
    /// The session wrapper handling lifetime safety
    session: Option<ChangeSession>,

    /// Borrowed document wrapping the guard's document_mut()
    document: Option<Py<crate::Document>>,

    /// Transaction on the borrowed document
    transaction: Option<Py<crate::Transaction>>,

    /// Heads before modification, for generating patches
    before_heads: Vec<automerge::ChangeHash>,

    /// Arc pointer address for thread-local cleanup
    #[allow(dead_code)]
    arc_id: usize,
}

#[pymethods]
impl ChangeGuard {
    /// Get the transaction for this change session.
    fn transaction(&self, py: Python) -> PyResult<Py<crate::Transaction>> {
        match &self.transaction {
            Some(tx) => Ok(tx.clone_ref(py)),
            None => Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "Change already ended",
            )),
        }
    }

    /// End the change session, committing or rolling back.
    ///
    /// Args:
    ///     commit: If true, commit the transaction. If false, rollback.
    ///     now: Current timestamp as seconds since epoch.
    ///
    /// Returns:
    ///     WithDocResult containing patches, IO tasks, and messages.
    fn end_change(&mut self, py: Python, commit: bool, now: f64) -> PyResult<PyWithDocResult> {
        // Take the transaction first
        let transaction = self.transaction.take();
        let document = self.document.take();

        // Clear thread-local context immediately
        CURRENT_DOC_CONTEXT.with(|ctx| {
            *ctx.borrow_mut() = None;
        });

        // Commit or rollback the automerge transaction
        if let Some(tx) = &transaction {
            let tx_ref = tx.bind(py);
            if commit {
                tx_ref.call_method0("commit")?;
            } else {
                tx_ref.call_method0("rollback")?;
            }
        }

        // Generate patches by diffing before/after heads
        let patches = if let Some(ref doc) = document {
            let doc_ref = doc.bind(py);
            let after_heads: Vec<crate::PyChangeHash> =
                doc_ref.call_method0("get_heads")?.extract()?;
            let before: Vec<crate::PyChangeHash> = self
                .before_heads
                .iter()
                .map(|h| crate::PyChangeHash(*h))
                .collect();

            if before.iter().map(|h| h.0).collect::<Vec<_>>()
                != after_heads.iter().map(|h| h.0).collect::<Vec<_>>()
            {
                let patches: Vec<crate::PyPatch> = doc_ref
                    .call_method1("diff", (before, after_heads))?
                    .extract()?;
                patches
            } else {
                vec![]
            }
        } else {
            vec![]
        };

        // Invalidate borrowed document
        if let Some(doc) = &document {
            doc.borrow(py).invalidate();
        }

        // Commit the session
        // This consumes the WithDocGuard and releases the lock
        let session = self.session.take().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Change already ended")
        })?;

        let timestamp = samod_core::UnixTimestamp::from_millis((now * 1000.0) as u128);
        let actor_result = session.commit(timestamp);

        // Build result - we use a dummy WithDocResult since we generated patches ourselves
        let inner =
            samod_core::actors::document::WithDocResult::with_side_effects((), actor_result);

        Ok(PyWithDocResult {
            return_value: py.None(),
            patches,
            inner,
        })
    }
}

impl Drop for ChangeGuard {
    fn drop(&mut self) {
        // Panic if session still exists - end_change must always be called
        // The Python __exit__ implementation is responsible for ensuring this
        if self.session.is_some() {
            // Clear thread-local context before panicking to avoid leaving stale state
            CURRENT_DOC_CONTEXT.with(|ctx| {
                *ctx.borrow_mut() = None;
            });
            panic!(
                "ChangeGuard dropped without calling end_change(). \
                 The ChangeContext.__exit__ must always call end_change()."
            );
        }
    }
}
