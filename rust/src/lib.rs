use std::{
    cell::{Cell, RefCell},
    mem::transmute,
    sync::{Arc, Mutex, RwLock},
};

use ::automerge::{
    self as am, transaction::Transactable, ChangeHash, ObjType, Prop, ReadDoc, ScalarValue,
};
use am::{
    marks::{ExpandMark, Mark},
    sync::SyncDoc,
    ActorId,
};
use pyo3::{
    exceptions::{PyException, PyValueError},
    prelude::*,
    types::{PyBool, PyBytes, PyDateTime, PyTuple},
};

mod repo;

/// Reference to an Automerge document - either owned, borrowed, or actor-backed
enum DocumentRef {
    /// An owned document
    Owned(am::Automerge),
    /// A borrowed document (raw pointer that can be temporarily removed)
    /// Set to None when the callback completes or needs to be invalidated
    Borrowed(Cell<Option<*mut am::Automerge>>),
    /// A reference to a document managed by a DocumentActor
    /// Each access acquires the mutex lock
    Actor(Arc<Mutex<samod_core::actors::document::DocumentActor>>),
}

// SAFETY: DocumentRef is Send + Sync because:
// - Owned variant contains am::Automerge which is Send + Sync
// - Borrowed variant is only used within with_document callbacks which execute synchronously
//   and the pointer is never sent across threads or accessed concurrently
unsafe impl Send for DocumentRef {}
unsafe impl Sync for DocumentRef {}

thread_local! {
    /// Tracks the current document context when inside a with_document() callback.
    /// Stores (Arc pointer address as identity, document pointer).
    /// This allows reentrant read access without deadlocking on the mutex.
    static CURRENT_DOC_CONTEXT: RefCell<Option<(usize, *const am::Automerge)>> =
        RefCell::new(None);
}

pub(crate) struct Inner {
    pub(crate) doc_ref: DocumentRef,
    pub(crate) tx: Option<am::transaction::Transaction<'static>>,
}

fn get_heads(heads: Option<Vec<PyChangeHash>>) -> Option<Vec<ChangeHash>> {
    heads.map(|heads| heads.iter().map(|h| h.0).collect())
}

impl Inner {
    pub(crate) fn new(doc: am::Automerge) -> Self {
        Self {
            doc_ref: DocumentRef::Owned(doc),
            tx: None,
        }
    }

    pub(crate) fn new_borrowed(doc_ptr: *mut am::Automerge) -> Self {
        Self {
            doc_ref: DocumentRef::Borrowed(Cell::new(Some(doc_ptr))),
            tx: None,
        }
    }

    /// Execute a callback with an immutable reference to the document
    ///
    /// For Owned and Borrowed documents, provides direct reference.
    /// For Actor-backed documents, acquires mutex lock before calling callback.
    ///
    /// Returns error if:
    /// - Called on a borrowed document that has been invalidated
    /// - Called while a transaction is active (transaction has exclusive access)
    fn with_doc<F, R>(&self, f: F) -> PyResult<R>
    where
        F: FnOnce(&am::Automerge) -> R,
    {
        if self.tx.is_some() {
            return Err(PyException::new_err(
                "Cannot access document while transaction is active",
            ));
        }

        match &self.doc_ref {
            DocumentRef::Owned(doc) => Ok(f(doc)),
            DocumentRef::Borrowed(cell) => {
                let ptr = cell.get().ok_or_else(|| {
                    PyException::new_err(
                        "Document cannot be accessed: either used after with_document callback completed, or invalidated"
                    )
                })?;
                // SAFETY: The pointer is valid because:
                // 1. It was created from a valid reference
                // 2. We're within the callback scope (otherwise ptr would be None)
                // 3. No transaction is active (checked above)
                Ok(f(unsafe { &*ptr }))
            }
            DocumentRef::Actor(arc_mutex) => {
                // Get the identity of this actor (Arc pointer address)
                let arc_id = Arc::as_ptr(arc_mutex) as usize;

                // Check if we're currently inside a callback for THIS actor
                let context = CURRENT_DOC_CONTEXT.with(|ctx| *ctx.borrow());

                if let Some((ctx_arc_id, doc_ptr)) = context {
                    if ctx_arc_id == arc_id {
                        // We're in a callback for this same actor!
                        // Use the existing document reference instead of locking
                        // SAFETY: The pointer is valid because:
                        // 1. We're on the same thread (thread-local storage)
                        // 2. The callback is still executing (context would be None otherwise)
                        // 3. Python GIL ensures single-threaded execution
                        // 4. The document reference outlives this operation
                        return Ok(f(unsafe { &*doc_ptr }));
                    }
                }

                // Not in a callback, or different actor: use normal mutex path
                let actor = arc_mutex.lock().unwrap();
                let doc = actor.document();
                Ok(f(doc))
            }
        }
    }

    /// Get a mutable reference to the document
    fn doc_mut(&mut self) -> PyResult<&mut am::Automerge> {
        if self.tx.is_some() {
            return Err(PyException::new_err(
                "cannot mutate docuemnt with an active transaction",
            ));
        }
        match &mut self.doc_ref {
            DocumentRef::Owned(doc) => Ok(doc),
            DocumentRef::Borrowed(cell) => {
                let ptr = cell.get().ok_or_else(|| {
                    PyException::new_err(
                        "Document cannot be accessed: either used after with_document callback completed, or invalidated"
                    )
                })?;
                // SAFETY: We're about to create a transaction which will have exclusive access
                Ok(unsafe { &mut *ptr })
            },
            DocumentRef::Actor(_) => {
                Err(PyException::new_err(
                    "Cannot mutate read-only document from DocHandle.doc(). Use DocHandle.change() instead."
                ))
            }
        }
    }

    /// Invalidate a borrowed document (no-op for owned documents)
    pub(crate) fn invalidate(&self) {
        if let DocumentRef::Borrowed(ref cell) = self.doc_ref {
            cell.set(None);
        }
    }

    /// Check if this is a borrowed document
    fn is_borrowed(&self) -> bool {
        matches!(self.doc_ref, DocumentRef::Borrowed(_))
    }

    // Read methods go on Inner as they're callable from either Transaction or Document.
    fn object_type(&self, obj_id: PyObjId) -> PyResult<PyObjType> {
        if let Some(tx) = self.tx.as_ref() {
            tx.object_type(obj_id.0)
        } else {
            self.with_doc(|doc| doc.object_type(obj_id.0))?
        }
        .map_err(|e| PyException::new_err(e.to_string()))
        .map(PyObjType::from_objtype)
    }

    fn get<'py>(
        &self,
        obj_id: PyObjId,
        prop: PyProp,
        heads: Option<Vec<PyChangeHash>>,
    ) -> PyResult<Option<(PyValue<'py>, PyObjId)>> {
        if let Some(tx) = self.tx.as_ref() {
            let res = match get_heads(heads) {
                Some(heads) => tx.get_at(obj_id.0, prop.0, &heads),
                None => tx.get(obj_id.0, prop.0),
            }
            .map_err(|e| PyException::new_err(e.to_string()))?;
            Ok(res.map(|(v, id)| (PyValue(v.into_owned()), PyObjId(id))))
        } else {
            self.with_doc(|doc| {
                let res = match get_heads(heads) {
                    Some(heads) => doc.get_at(obj_id.0, prop.0, &heads),
                    None => doc.get(obj_id.0, prop.0),
                }
                .map_err(|e| PyException::new_err(e.to_string()))?;
                Ok(res.map(|(v, id)| (PyValue(v.into_owned()), PyObjId(id))))
            })?
        }
    }

    fn keys(&self, obj_id: PyObjId, heads: Option<Vec<PyChangeHash>>) -> PyResult<Vec<String>> {
        if let Some(tx) = self.tx.as_ref() {
            let res = match get_heads(heads) {
                Some(heads) => tx.keys_at(obj_id.0, &heads),
                None => tx.keys(obj_id.0),
            };
            Ok(res.collect())
        } else {
            self.with_doc(|doc| {
                let res = match get_heads(heads) {
                    Some(heads) => doc.keys_at(obj_id.0, &heads),
                    None => doc.keys(obj_id.0),
                };
                res.collect()
            })
        }
    }

    fn values<'py>(
        &self,
        obj_id: PyObjId,
        heads: Option<Vec<PyChangeHash>>,
    ) -> PyResult<Vec<(PyValue<'py>, PyObjId)>> {
        if let Some(tx) = self.tx.as_ref() {
            let res = match get_heads(heads) {
                Some(heads) => tx.values_at(obj_id.0, &heads),
                None => tx.values(obj_id.0),
            };
            Ok(res
                .map(|(v, id)| (PyValue(v.into_owned()), PyObjId(id)))
                .collect())
        } else {
            self.with_doc(|doc| {
                let res = match get_heads(heads) {
                    Some(heads) => doc.values_at(obj_id.0, &heads),
                    None => doc.values(obj_id.0),
                };
                res.map(|(v, id)| (PyValue(v.into_owned()), PyObjId(id)))
                    .collect()
            })
        }
    }

    fn get_heads(&self) -> PyResult<Vec<PyChangeHash>> {
        let heads = if let Some(tx) = self.tx.as_ref() {
            tx.get_heads()
        } else {
            self.with_doc(|doc| doc.get_heads())?
        };
        Ok(heads.into_iter().map(|c| PyChangeHash(c)).collect())
    }

    fn length(&self, obj_id: PyObjId, heads: Option<Vec<PyChangeHash>>) -> PyResult<usize> {
        Ok(if let Some(tx) = self.tx.as_ref() {
            match get_heads(heads) {
                Some(heads) => tx.length_at(obj_id.0, &heads),
                None => tx.length(obj_id.0),
            }
        } else {
            self.with_doc(|doc| match get_heads(heads) {
                Some(heads) => doc.length_at(obj_id.0, &heads),
                None => doc.length(obj_id.0),
            })?
        })
    }

    fn text(&self, obj_id: PyObjId, heads: Option<Vec<PyChangeHash>>) -> PyResult<String> {
        if let Some(tx) = self.tx.as_ref() {
            match get_heads(heads) {
                Some(heads) => tx.text_at(obj_id.0, &heads),
                None => tx.text(obj_id.0),
            }
        } else {
            self.with_doc(|doc| match get_heads(heads) {
                Some(heads) => doc.text_at(obj_id.0, &heads),
                None => doc.text(obj_id.0),
            })?
        }
        .map_err(|e| PyException::new_err(e.to_string()))
    }

    fn marks(&self, obj_id: PyObjId, heads: Option<Vec<PyChangeHash>>) -> PyResult<Vec<PyMark>> {
        let res = if let Some(tx) = self.tx.as_ref() {
            match get_heads(heads) {
                Some(heads) => tx.marks_at(obj_id.0, &heads),
                None => tx.marks(obj_id.0),
            }
        } else {
            self.with_doc(|doc| match get_heads(heads) {
                Some(heads) => doc.marks_at(obj_id.0, &heads),
                None => doc.marks(obj_id.0),
            })?
        }
        .map_err(|e| PyException::new_err(e.to_string()))?;
        Ok(res
            .into_iter()
            .map(|m| PyMark {
                start: m.start,
                end: m.end,
                name: m.name().to_owned(),
                value: PyScalarValue(m.value().clone()),
            })
            .collect())
    }
}

// A unified Automerge document that can be owned, borrowed, or part of a DocumentActor.
//
// Owned documents are created with `Document::new()` and own their Automerge document.
// Borrowed documents are created within `with_document` callbacks and wrap a raw pointer
// to a document that exists elsewhere. The document actor version is created
// with Document::new_from_actor
#[pyclass(name = "Document")]
pub(crate) struct Document {
    inner: Arc<RwLock<Inner>>,
}

// SAFETY: Document is Send + Sync because it uses Arc<RwLock<Inner>>
// which properly synchronizes access across threads
unsafe impl Send for Document {}
unsafe impl Sync for Document {}

impl Document {
    /// Create a new borrowed Document from a mutable reference
    ///
    /// SAFETY: The caller must ensure that:
    /// 1. The document reference remains valid for the lifetime of this Document
    /// 2. invalidate() is called before the document reference becomes invalid
    /// 3. Only one borrowed Document exists for a given document at a time
    pub(crate) unsafe fn new_borrowed(doc: &mut am::Automerge) -> Self {
        Self {
            inner: Arc::new(RwLock::new(Inner::new_borrowed(doc as *mut am::Automerge))),
        }
    }

    /// Invalidate a borrowed document (no-op for owned documents)
    ///
    /// This should be called after the with_document callback completes to ensure
    /// that any further attempts to use a borrowed document will panic instead of causing UB.
    pub(crate) fn invalidate(&self) {
        let inner = self.inner.read().expect("Failed to acquire read lock");
        inner.invalidate();
    }

    /// Create a new Document backed by a DocumentActor
    ///
    /// The returned document is read-only and acquires the actor's mutex on each access.
    /// This allows direct property access without callbacks, but with the trade-off of
    /// mutex acquisition overhead per operation.
    ///
    /// Transactions cannot be created on actor-backed documents.
    pub(crate) fn new_from_actor(
        actor: Arc<Mutex<samod_core::actors::document::DocumentActor>>,
    ) -> Self {
        Self {
            inner: Arc::new(RwLock::new(Inner {
                doc_ref: DocumentRef::Actor(actor),
                tx: None,
            })),
        }
    }
}

#[pymethods]
impl Document {
    #[new]
    #[pyo3(signature=(actor_id=None))]
    fn new(actor_id: Option<&[u8]>) -> Self {
        let mut doc = am::Automerge::new();
        if let Some(id) = actor_id {
            doc.set_actor(ActorId::from(id));
        }
        Document {
            inner: Arc::new(RwLock::new(Inner::new(doc))),
        }
    }

    fn get_actor<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        let inner = self
            .inner
            .read()
            .map_err(|e| PyException::new_err(e.to_string()))?;
        inner.with_doc(|doc| PyBytes::new(py, doc.get_actor().to_bytes()))
    }

    fn set_actor(&mut self, actor_id: &[u8]) -> PyResult<()> {
        let inner = self
            .inner
            .read()
            .map_err(|e| PyException::new_err(e.to_string()))?;

        // Check if this is a borrowed document
        if inner.is_borrowed() {
            return Err(PyException::new_err(
                "cannot set actor id on a borrowed document",
            ));
        }

        // Drop the read lock before acquiring write lock
        drop(inner);

        let mut inner = self
            .inner
            .write()
            .map_err(|e| PyException::new_err(format!("error getting write lock: {}", e)))?;

        // Get mutable reference to the document
        let doc = inner.doc_mut()?;
        doc.set_actor(ActorId::from(actor_id));
        Ok(())
    }

    fn transaction(&mut self) -> PyResult<Transaction> {
        let mut inner = self
            .inner
            .write()
            .map_err(|e| PyException::new_err(format!("error getting write lock: {}", e)))?;

        if inner.tx.is_some() {
            return Err(PyException::new_err("transaction already active"));
        }

        // Get mutable reference to the document and create transaction
        let doc = inner.doc_mut()?;

        // SAFETY: We're transmuting the lifetime of the transaction to `static`, which is okay
        // because we are then storing the transaction in `Inner` which means the document will
        // live as long as the transaction.
        let tx = unsafe { transmute(doc.transaction()) };
        inner.tx = Some(tx);

        Ok(Transaction {
            inner: Arc::clone(&self.inner),
        })
    }

    fn get_heads(&self) -> PyResult<Vec<PyChangeHash>> {
        let inner = self
            .inner
            .read()
            .map_err(|e| PyException::new_err(e.to_string()))?;
        inner.get_heads()
    }

    fn object_type(&self, obj_id: PyObjId) -> PyResult<PyObjType> {
        let inner = self
            .inner
            .read()
            .map_err(|e| PyException::new_err(e.to_string()))?;
        inner.object_type(obj_id)
    }

    #[pyo3(signature=(obj_id, prop, heads=None))]
    fn get(
        &self,
        obj_id: PyObjId,
        prop: PyProp,
        heads: Option<Vec<PyChangeHash>>,
    ) -> PyResult<Option<(PyValue<'_>, PyObjId)>> {
        let inner = self
            .inner
            .read()
            .map_err(|e| PyException::new_err(e.to_string()))?;
        inner.get(obj_id, prop, heads)
    }

    #[pyo3(signature=(obj_id, heads=None))]
    fn keys(&self, obj_id: PyObjId, heads: Option<Vec<PyChangeHash>>) -> PyResult<Vec<String>> {
        let inner = self
            .inner
            .read()
            .map_err(|e| PyException::new_err(e.to_string()))?;
        inner.keys(obj_id, heads)
    }

    #[pyo3(signature=(obj_id, heads=None))]
    fn values(
        &self,
        obj_id: PyObjId,
        heads: Option<Vec<PyChangeHash>>,
    ) -> PyResult<Vec<(PyValue<'_>, PyObjId)>> {
        let inner = self
            .inner
            .read()
            .map_err(|e| PyException::new_err(e.to_string()))?;
        inner.values(obj_id, heads)
    }

    #[pyo3(signature=(obj_id, heads=None))]
    fn length(&self, obj_id: PyObjId, heads: Option<Vec<PyChangeHash>>) -> PyResult<usize> {
        let inner = self
            .inner
            .read()
            .map_err(|e| PyException::new_err(e.to_string()))?;
        inner.length(obj_id, heads)
    }

    #[pyo3(signature=(obj_id, heads=None))]
    fn text(&self, obj_id: PyObjId, heads: Option<Vec<PyChangeHash>>) -> PyResult<String> {
        let inner = self
            .inner
            .read()
            .map_err(|e| PyException::new_err(e.to_string()))?;
        inner.text(obj_id, heads)
    }

    #[pyo3(signature=(obj_id, heads=None))]
    fn marks(&self, obj_id: PyObjId, heads: Option<Vec<PyChangeHash>>) -> PyResult<Vec<PyMark>> {
        let inner = self
            .inner
            .read()
            .map_err(|e| PyException::new_err(e.to_string()))?;
        inner.marks(obj_id, heads)
    }

    fn fork(&self) -> PyResult<Self> {
        let inner = self
            .inner
            .read()
            .map_err(|e| PyException::new_err(e.to_string()))?;

        inner.with_doc(|doc| Document {
            inner: Arc::new(RwLock::new(Inner::new(doc.fork()))),
        })
    }

    fn fork_at(&self, heads: Vec<PyChangeHash>) -> PyResult<Self> {
        let inner = self
            .inner
            .read()
            .map_err(|e| PyException::new_err(e.to_string()))?;

        let heads: Vec<_> = heads.iter().map(|h| h.0).collect();
        inner.with_doc(|doc| {
            doc.fork_at(&heads)
                .map(|forked| Document {
                    inner: Arc::new(RwLock::new(Inner::new(forked))),
                })
                .map_err(|e| PyException::new_err(e.to_string()))
        })?
    }

    fn merge(&mut self, other: &Document) -> PyResult<Vec<PyChangeHash>> {
        let mut inner = self
            .inner
            .write()
            .map_err(|e| PyException::new_err(format!("error getting write lock: {}", e)))?;

        if inner.tx.is_some() {
            return Err(PyException::new_err(
                "cannot merge with an active transaction",
            ));
        }

        let mut other_inner = other
            .inner
            .write()
            .map_err(|e| PyException::new_err(e.to_string()))?;

        // Get mutable references to both documents for merging
        let doc = match &mut inner.doc_ref {
            DocumentRef::Owned(ref mut d) => d,
            DocumentRef::Borrowed(ref cell) => {
                let ptr = cell.get().ok_or_else(|| {
                    PyException::new_err("Document cannot be accessed: invalidated")
                })?;
                unsafe { &mut *ptr }
            }
            DocumentRef::Actor(_) => {
                return Err(PyException::new_err(
                    "Cannot merge actor-backed document. Use DocHandle.change() for modifications.",
                ));
            }
        };

        let other_doc = match &mut other_inner.doc_ref {
            DocumentRef::Owned(ref mut d) => d,
            DocumentRef::Borrowed(ref cell) => {
                let ptr = cell.get().ok_or_else(|| {
                    PyException::new_err("Document cannot be accessed: invalidated")
                })?;
                unsafe { &mut *ptr }
            }
            DocumentRef::Actor(_) => {
                return Err(PyException::new_err(
                    "Cannot merge actor-backed document. Use DocHandle.change() for modifications.",
                ));
            }
        };

        let heads = doc
            .merge(other_doc)
            .map_err(|e| PyException::new_err(e.to_string()))?
            .into_iter()
            .map(PyChangeHash)
            .collect();
        Ok(heads)
    }

    fn save(&self) -> PyResult<Vec<u8>> {
        let inner = self
            .inner
            .read()
            .map_err(|e| PyException::new_err(e.to_string()))?;

        inner.with_doc(|doc| doc.save())
    }

    #[staticmethod]
    fn load(data: &[u8]) -> PyResult<Self> {
        let doc = am::Automerge::load(data).map_err(|e| PyException::new_err(e.to_string()))?;
        Ok(Document {
            inner: Arc::new(RwLock::new(Inner::new(doc))),
        })
    }

    fn generate_sync_message(&self, sync_state: &mut PySyncState) -> PyResult<Option<PyMessage>> {
        let inner = self
            .inner
            .read()
            .map_err(|e| PyException::new_err(e.to_string()))?;

        inner.with_doc(|doc| doc.generate_sync_message(&mut sync_state.0).map(PyMessage))
    }

    fn receive_sync_message(
        &mut self,
        sync_state: &mut PySyncState,
        message: &PyMessage,
    ) -> PyResult<()> {
        let mut inner = self
            .inner
            .write()
            .map_err(|e| PyException::new_err(format!("error getting write lock: {}", e)))?;

        if inner.tx.is_some() {
            return Err(PyException::new_err(
                "cannot receive sync message with an active transaction",
            ));
        }

        let doc = inner.doc_mut()?;
        doc.receive_sync_message(&mut sync_state.0, message.0.clone())
            .map_err(|e| PyException::new_err(e.to_string()))?;
        Ok(())
    }

    fn get_changes(&self, heads: Vec<PyChangeHash>) -> PyResult<Vec<PyChange>> {
        let inner = self
            .inner
            .read()
            .map_err(|e| PyException::new_err(e.to_string()))?;

        let heads: Vec<_> = heads.iter().map(|h| h.0).collect();
        inner.with_doc(|doc| {
            doc.get_changes(&heads)
                .into_iter()
                .map(|c| PyChange(c.clone()))
                .collect()
        })
    }

    fn get_last_local_change(&self) -> PyResult<Option<PyChange>> {
        let inner = self
            .inner
            .read()
            .map_err(|e| PyException::new_err(e.to_string()))?;

        inner.with_doc(|doc| doc.get_last_local_change().map(|c| PyChange(c.clone())))
    }

    fn diff(&self, before: Vec<PyChangeHash>, after: Vec<PyChangeHash>) -> PyResult<Vec<PyPatch>> {
        let inner = self
            .inner
            .read()
            .map_err(|e| PyException::new_err(e.to_string()))?;

        let before: Vec<_> = before.iter().map(|h| h.0).collect();
        let after: Vec<_> = after.iter().map(|h| h.0).collect();
        inner.with_doc(|doc| doc.diff(&before, &after).into_iter().map(PyPatch).collect())
    }
}

#[derive(Clone)]
#[pyclass]
struct Transaction {
    inner: Arc<RwLock<Inner>>,
}

#[pymethods]
impl Transaction {
    #[pyo3(name = "__enter__")]
    fn enter(&self) -> PyResult<Transaction> {
        Ok(self.clone())
    }

    #[pyo3(name = "__exit__")]
    fn exit(
        &self,
        exc_type: Option<&Bound<'_, PyAny>>,
        _exc_value: Option<&Bound<'_, PyAny>>,
        _traceback: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<()> {
        let mut inner = self
            .inner
            .write()
            .map_err(|e| PyException::new_err(format!("error getting write lock: {}", e)))?;
        if let Some(tx) = inner.tx.take() {
            if let Some(_exc_type) = exc_type {
                tx.rollback();
            } else {
                tx.commit();
            }
        }
        Ok(())
    }

    fn get_heads(&self) -> PyResult<Vec<PyChangeHash>> {
        let inner = self
            .inner
            .read()
            .map_err(|e| PyException::new_err(e.to_string()))?;
        inner.get_heads()
    }

    fn object_type(&self, obj_id: PyObjId) -> PyResult<PyObjType> {
        let inner = self
            .inner
            .read()
            .map_err(|e| PyException::new_err(e.to_string()))?;
        inner.object_type(obj_id)
    }

    #[pyo3(signature=(obj_id, prop, heads=None))]
    fn get(
        &self,
        obj_id: PyObjId,
        prop: PyProp,
        heads: Option<Vec<PyChangeHash>>,
    ) -> PyResult<Option<(PyValue<'_>, PyObjId)>> {
        let inner = self
            .inner
            .read()
            .map_err(|e| PyException::new_err(e.to_string()))?;
        inner.get(obj_id, prop, heads)
    }

    #[pyo3(signature=(obj_id, heads=None))]
    fn keys(&self, obj_id: PyObjId, heads: Option<Vec<PyChangeHash>>) -> PyResult<Vec<String>> {
        let inner = self
            .inner
            .read()
            .map_err(|e| PyException::new_err(e.to_string()))?;
        inner.keys(obj_id, heads)
    }

    #[pyo3(signature=(obj_id, heads=None))]
    fn values(
        &self,
        obj_id: PyObjId,
        heads: Option<Vec<PyChangeHash>>,
    ) -> PyResult<Vec<(PyValue<'_>, PyObjId)>> {
        let inner = self
            .inner
            .read()
            .map_err(|e| PyException::new_err(e.to_string()))?;
        inner.values(obj_id, heads)
    }

    #[pyo3(signature=(obj_id, heads=None))]
    fn length(&self, obj_id: PyObjId, heads: Option<Vec<PyChangeHash>>) -> PyResult<usize> {
        let inner = self
            .inner
            .read()
            .map_err(|e| PyException::new_err(e.to_string()))?;
        inner.length(obj_id, heads)
    }

    #[pyo3(signature=(obj_id, heads=None))]
    fn text(&self, obj_id: PyObjId, heads: Option<Vec<PyChangeHash>>) -> PyResult<String> {
        let inner = self
            .inner
            .read()
            .map_err(|e| PyException::new_err(e.to_string()))?;
        inner.text(obj_id, heads)
    }

    #[pyo3(signature=(obj_id, heads=None))]
    fn marks(&self, obj_id: PyObjId, heads: Option<Vec<PyChangeHash>>) -> PyResult<Vec<PyMark>> {
        let inner = self
            .inner
            .read()
            .map_err(|e| PyException::new_err(e.to_string()))?;
        inner.marks(obj_id, heads)
    }

    fn put(
        &mut self,
        obj_id: PyObjId,
        prop: PyProp,
        value_type: &PyScalarType,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let mut inner = self
            .inner
            .write()
            .map_err(|e| PyException::new_err(format!("error getting write lock: {}", e)))?;
        let Some(tx) = inner.tx.as_mut() else {
            return Err(PyException::new_err("transaction no longer active"));
        };
        tx.put(obj_id.0, prop.0, import_scalar(value, value_type)?)
            .map_err(|e| PyException::new_err(format!("error putting: {}", e)))
    }

    fn put_object(
        &mut self,
        obj_id: PyObjId,
        prop: PyProp,
        objtype: &PyObjType,
    ) -> PyResult<PyObjId> {
        let mut inner = self
            .inner
            .write()
            .map_err(|e| PyException::new_err(format!("error getting write lock: {}", e)))?;
        let Some(tx) = inner.tx.as_mut() else {
            return Err(PyException::new_err("transaction no longer active"));
        };
        tx.put_object(obj_id.0, prop.0, objtype.into())
            .map_err(|e| PyException::new_err(format!("error putting: {}", e)))
            .map(PyObjId)
    }

    fn insert(
        &mut self,
        obj_id: PyObjId,
        index: usize,
        value_type: &PyScalarType,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let mut inner = self
            .inner
            .write()
            .map_err(|e| PyException::new_err(format!("error getting write lock: {}", e)))?;
        let Some(tx) = inner.tx.as_mut() else {
            return Err(PyException::new_err("transaction no longer active"));
        };
        tx.insert(obj_id.0, index, import_scalar(value, value_type)?)
            .map_err(|e| PyException::new_err(format!("error putting: {}", e)))
    }

    fn insert_object(
        &mut self,
        obj_id: PyObjId,
        index: usize,
        objtype: &PyObjType,
    ) -> PyResult<PyObjId> {
        let mut inner = self
            .inner
            .write()
            .map_err(|e| PyException::new_err(format!("error getting write lock: {}", e)))?;
        let Some(tx) = inner.tx.as_mut() else {
            return Err(PyException::new_err("transaction no longer active"));
        };
        tx.insert_object(obj_id.0, index, objtype.into())
            .map_err(|e| PyException::new_err(format!("error putting: {}", e)))
            .map(PyObjId)
    }

    fn increment(&mut self, obj_id: PyObjId, prop: PyProp, value: i64) -> PyResult<()> {
        let mut inner = self
            .inner
            .write()
            .map_err(|e| PyException::new_err(format!("error getting write lock: {}", e)))?;
        let Some(tx) = inner.tx.as_mut() else {
            return Err(PyException::new_err("transaction no longer active"));
        };
        tx.increment(obj_id.0, prop.0, value)
            .map_err(|e| PyException::new_err(format!("error putting: {}", e)))
    }

    fn delete(&mut self, obj_id: PyObjId, prop: PyProp) -> PyResult<()> {
        let mut inner = self
            .inner
            .write()
            .map_err(|e| PyException::new_err(format!("error getting write lock: {}", e)))?;
        let Some(tx) = inner.tx.as_mut() else {
            return Err(PyException::new_err("transaction no longer active"));
        };
        tx.delete(obj_id.0, prop.0)
            .map_err(|e| PyException::new_err(format!("error putting: {}", e)))
    }

    fn mark(
        &mut self,
        obj_id: PyObjId,
        start: usize,
        end: usize,
        name: &str,
        value_type: &PyScalarType,
        value: &Bound<'_, PyAny>,
        expand: &Bound<'_, PyExpandMark>,
    ) -> PyResult<()> {
        let mut inner = self
            .inner
            .write()
            .map_err(|e| PyException::new_err(format!("error getting write lock: {}", e)))?;
        let Some(tx) = inner.tx.as_mut() else {
            return Err(PyException::new_err("transaction no longer active"));
        };
        let value = import_scalar(value, value_type)?;
        tx.mark(
            obj_id.0,
            Mark::new(name.to_owned(), value, start, end),
            (&*expand.borrow()).into(),
        )
        .map_err(|e| PyException::new_err(e.to_string()))
    }

    fn unmark(
        &mut self,
        obj_id: PyObjId,
        start: usize,
        end: usize,
        name: &str,
        expand: &PyExpandMark,
    ) -> PyResult<()> {
        let mut inner = self
            .inner
            .write()
            .map_err(|e| PyException::new_err(format!("error getting write lock: {}", e)))?;
        let Some(tx) = inner.tx.as_mut() else {
            return Err(PyException::new_err("transaction no longer active"));
        };
        tx.unmark(obj_id.0, name, start, end, expand.into())
            .map_err(|e| PyException::new_err(e.to_string()))
    }

    fn splice_text(
        &mut self,
        obj_id: PyObjId,
        pos: usize,
        delete_count: isize,
        text: &str,
    ) -> PyResult<()> {
        let mut inner = self
            .inner
            .write()
            .map_err(|e| PyException::new_err(format!("error getting write lock: {}", e)))?;
        let Some(tx) = inner.tx.as_mut() else {
            return Err(PyException::new_err("transaction no longer active"));
        };
        tx.splice_text(obj_id.0, pos, delete_count, text)
            .map_err(|e| PyException::new_err(e.to_string()))
    }
}

fn datetime_to_timestamp(datetime: &Bound<'_, PyDateTime>) -> PyResult<i64> {
    Ok((datetime.call_method0("timestamp")?.extract::<f64>()? * 1000.0).round() as i64)
}

fn import_scalar(
    value: &Bound<'_, PyAny>,
    scalar_type: &PyScalarType,
) -> Result<ScalarValue, PyErr> {
    Ok(match scalar_type {
        PyScalarType::Bytes => ScalarValue::Bytes(value.extract::<&[u8]>()?.to_owned()),
        PyScalarType::Str => ScalarValue::Str(value.extract::<String>()?.into()),
        PyScalarType::Int => ScalarValue::Int(value.extract::<i64>()?),
        PyScalarType::Uint => ScalarValue::Uint(value.extract::<u64>()?),
        PyScalarType::F64 => ScalarValue::F64(value.extract::<f64>()?),
        PyScalarType::Counter => todo!(),
        PyScalarType::Timestamp => {
            ScalarValue::Timestamp(datetime_to_timestamp(value.cast::<PyDateTime>()?)?)
        }
        PyScalarType::Boolean => ScalarValue::Boolean(value.extract::<bool>()?),
        PyScalarType::Unknown => todo!(),
        PyScalarType::Null => ScalarValue::Null,
    })
}

#[pyclass(name = "SyncState")]
struct PySyncState(am::sync::State);

#[pymethods]
impl PySyncState {
    #[new]
    pub fn new() -> PySyncState {
        PySyncState(am::sync::State::new())
    }
}

#[pyclass(name = "Message")]
struct PyMessage(am::sync::Message);

#[pymethods]
impl PyMessage {
    pub fn encode<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.0.clone().encode())
    }

    #[staticmethod]
    pub fn decode(bytes: &[u8]) -> PyResult<PyMessage> {
        Ok(PyMessage(
            am::sync::Message::decode(bytes).map_err(|e| PyException::new_err(e.to_string()))?,
        ))
    }
}

#[pyfunction]
fn random_actor_id<'py>(py: Python<'py>) -> Bound<'py, PyBytes> {
    PyBytes::new(py, ActorId::random().to_bytes())
}

/// A Python module implemented in Rust.
#[pymodule]
fn _automerge(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Document classes
    m.add_class::<Document>()?;
    m.add_class::<Transaction>()?;
    m.add_class::<PySyncState>()?;
    m.add_class::<PyMessage>()?;

    // Enums
    m.add_class::<PyObjType>()?;
    m.add_class::<PyScalarType>()?;
    m.add_class::<PyExpandMark>()?;

    // Constants
    m.add("ROOT", PyObjId(am::ROOT))?;

    // Functions
    m.add_function(wrap_pyfunction!(random_actor_id, m)?)?;
    m.add_function(wrap_pyfunction!(enable_tracing, m)?)?;

    // Repo types
    repo::register_types(m)?;

    Ok(())
}

/// Enable tracing output for debugging samod-core internals
///
/// This initializes the tracing subscriber to output logs to stderr.
/// Call this before creating repos to see internal samod-core logs.
///
/// Args:
///     level: Optional log level filter (e.g., "trace", "debug", "info"). Defaults to "debug".
#[pyfunction]
fn enable_tracing(level: Option<String>) -> PyResult<()> {
    use tracing_subscriber::{fmt, EnvFilter};

    let filter = match level {
        Some(l) => EnvFilter::new(l),
        None => EnvFilter::new("info"),
    };

    fmt()
        .with_env_filter(filter)
        .with_target(true)
        .with_line_number(true)
        .try_init()
        .map_err(|e| PyException::new_err(format!("Failed to initialize tracing: {}", e)))?;

    Ok(())
}

#[derive(Debug)]
pub struct PyProp(Prop);

impl<'a, 'py> FromPyObject<'a, 'py> for PyProp {
    type Error = PyErr;

    fn extract(prop: Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        Ok(PyProp(match prop.extract::<String>() {
            Ok(s) => Ok(Prop::Map(s)),
            Err(_) => match prop.extract::<usize>() {
                Ok(i) => Ok(Prop::Seq(i)),
                Err(e) => Err(PyErr::new::<PyValueError, _>(e.to_string())),
            },
        }?))
    }
}

#[derive(Debug)]
pub struct PyObjId(am::ObjId);

impl<'a, 'py> FromPyObject<'a, 'py> for PyObjId {
    type Error = PyErr;

    fn extract(prop: Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        let bytes = prop.extract::<&[u8]>()?;
        let obj_id =
            am::ObjId::try_from(bytes).map_err(|e| PyErr::new::<PyValueError, _>(e.to_string()))?;
        Ok(PyObjId(obj_id))
    }
}

impl<'py> IntoPyObject<'py> for PyObjId {
    type Target = PyBytes;
    type Error = PyErr;
    type Output = Bound<'py, Self::Target>;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let bytes: &[u8] = &self.0.to_bytes();
        Ok(PyBytes::new(py, bytes))
    }
}

#[derive(Debug, Clone)]
pub struct PyChangeHash(am::ChangeHash);

impl<'a, 'py> FromPyObject<'a, 'py> for PyChangeHash {
    type Error = PyErr;

    fn extract(v: Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        let bytes = v.extract::<&[u8]>()?;
        Ok(PyChangeHash(am::ChangeHash::try_from(bytes).map_err(
            |e| PyErr::new::<PyValueError, _>(e.to_string()),
        )?))
    }
}

impl<'py> IntoPyObject<'py> for PyChangeHash {
    type Target = PyBytes;
    type Error = PyErr;
    type Output = Bound<'py, Self::Target>;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        Ok(PyBytes::new(py, self.0.as_ref()))
    }
}

#[derive(Debug, PartialEq, Eq)]
#[pyclass(name = "ObjType", eq, eq_int)]
pub enum PyObjType {
    Map,
    List,
    Text,
}

impl PyObjType {
    fn from_objtype(objtype: ObjType) -> PyObjType {
        match objtype {
            ObjType::Map => PyObjType::Map,
            ObjType::Table => PyObjType::Map,
            ObjType::List => PyObjType::List,
            ObjType::Text => PyObjType::Text,
        }
    }
}

impl Into<ObjType> for &PyObjType {
    fn into(self) -> ObjType {
        match self {
            PyObjType::Map => ObjType::Map,
            PyObjType::List => ObjType::List,
            PyObjType::Text => ObjType::Text,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[pyclass(name = "ScalarType", eq, eq_int)]
pub enum PyScalarType {
    Bytes,
    Str,
    Int,
    Uint,
    F64,
    Counter,
    Timestamp,
    Boolean,
    Unknown,
    Null,
}

#[derive(Debug, Clone)]
pub struct PyScalarValue(am::ScalarValue);
impl<'py> IntoPyObject<'py> for PyScalarValue {
    type Target = PyTuple;
    type Error = PyErr;
    type Output = Bound<'py, Self::Target>;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        match self.0 {
            ScalarValue::Bytes(v) => (PyScalarType::Bytes, v.into_pyobject(py)?),
            ScalarValue::Str(v) => (PyScalarType::Str, v.into_pyobject(py)?.into_any()),
            ScalarValue::Int(v) => (PyScalarType::Int, v.into_pyobject(py)?.into_any()),
            ScalarValue::Uint(v) => (PyScalarType::Uint, v.into_pyobject(py)?.into_any()),
            ScalarValue::F64(v) => (PyScalarType::F64, v.into_pyobject(py)?.into_any()),
            ScalarValue::Counter(_v) => todo!(),
            ScalarValue::Timestamp(v) => (
                PyScalarType::Timestamp,
                PyDateTime::from_timestamp(py, (v as f64) / 1000.0, None)
                    .unwrap()
                    .into_pyobject(py)?
                    .into_any(),
            ),
            ScalarValue::Boolean(v) => (
                PyScalarType::Boolean,
                PyBool::new(py, v).to_owned().into_any(),
            ),
            ScalarValue::Unknown {
                type_code: _,
                bytes: _,
            } => todo!(),
            ScalarValue::Null => (PyScalarType::Null, py.None().bind(py).to_owned().into_any()),
        }
        .into_pyobject(py)
    }
}

impl<'a, 'py> FromPyObject<'a, 'py> for PyScalarValue {
    type Error = PyErr;

    fn extract(v: Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        let (scalar_type, value) = v.extract::<(PyScalarType, Bound<'py, PyAny>)>()?;
        import_scalar(&value, &scalar_type).map(|v| PyScalarValue(v))
    }
}

#[derive(Debug)]
pub struct PyValue<'a>(am::Value<'a>);

impl<'a, 'py> IntoPyObject<'py> for PyValue<'a> {
    type Target = PyAny;
    type Error = PyErr;
    type Output = Bound<'py, Self::Target>;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        match self.0 {
            am::Value::Object(objtype) => Ok(PyObjType::from_objtype(objtype)
                .into_pyobject(py)?
                .into_any()),
            am::Value::Scalar(s) => Ok(PyScalarValue(s.as_ref().clone())
                .into_pyobject(py)?
                .into_any()),
        }
    }
}

#[pyclass(name = "Mark", get_all, set_all)]
#[derive(Debug)]
struct PyMark {
    start: usize,
    end: usize,
    name: String,
    value: PyScalarValue,
}

#[pymethods]
impl PyMark {
    fn __repr__(&self) -> String {
        format!("{:?}", self)
    }
}

#[pyclass(name = "ExpandMark")]
enum PyExpandMark {
    Before,
    After,
    Both,
    // "None" is a reserved word in Python.
    Neither,
}

impl Into<ExpandMark> for &PyExpandMark {
    fn into(self) -> ExpandMark {
        match self {
            PyExpandMark::Before => ExpandMark::Before,
            PyExpandMark::After => ExpandMark::After,
            PyExpandMark::Both => ExpandMark::Both,
            PyExpandMark::Neither => ExpandMark::None,
        }
    }
}

#[pyclass(name = "Change")]
#[derive(Debug)]
struct PyChange(am::Change);

#[pymethods]
impl PyChange {
    fn __repr__(&self) -> String {
        format!("{:?}", self.0)
    }

    #[getter]
    fn actor_id(&self) -> &[u8] {
        self.0.actor_id().to_bytes()
    }

    #[getter]
    fn other_actor_ids(&self) -> Vec<&[u8]> {
        self.0
            .other_actor_ids()
            .iter()
            .map(|id| id.to_bytes())
            .collect()
    }

    fn __len__(&self) -> usize {
        self.0.len()
    }

    #[getter]
    fn max_op(&self) -> u64 {
        self.0.max_op()
    }

    #[getter]
    fn start_op(&self) -> u64 {
        self.0.start_op().into()
    }

    #[getter]
    fn message(&self) -> Option<String> {
        self.0.message().cloned()
    }

    #[getter]
    fn deps(&self) -> Vec<PyChangeHash> {
        self.0.deps().iter().map(|h| PyChangeHash(*h)).collect()
    }

    #[getter]
    fn hash(&self) -> PyChangeHash {
        PyChangeHash(self.0.hash())
    }

    #[getter]
    fn seq(&self) -> u64 {
        self.0.seq()
    }

    #[getter]
    fn timestamp<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDateTime>> {
        PyDateTime::from_timestamp(py, (self.0.timestamp() as f64) / 1000.0, None)
    }

    #[getter]
    fn bytes<'py>(&mut self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, self.0.bytes().as_ref())
    }

    #[getter]
    fn raw_bytes(&self) -> &[u8] {
        self.0.raw_bytes()
    }

    #[getter]
    fn extra_bytes(&self) -> &[u8] {
        self.0.extra_bytes()
    }
}

#[pyclass(name = "Patch")]
#[derive(Debug, Clone)]
struct PyPatch(am::Patch);

#[pymethods]
impl PyPatch {
    fn __repr__(&self) -> String {
        format!("{:?}", self.0)
    }
}
