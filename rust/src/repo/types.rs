//! Core identity types for the repository
//!
//! This module contains wrappers for fundamental identity types from samod-core,
//! including peer IDs, storage IDs, connection IDs, document IDs, etc.

use pyo3::prelude::*;

/// Wrapper for samod_core::PeerId
///
/// Represents a unique identifier for a peer in the network. Each running instance
/// of a repository gets a unique peer ID.
#[pyclass(name = "PeerId")]
#[derive(Clone)]
pub struct PyPeerId(pub(crate) samod_core::PeerId);

#[pymethods]
impl PyPeerId {
    /// Create a new random PeerId
    #[staticmethod]
    fn random() -> Self {
        let mut rng = rand::rng();
        PyPeerId(samod_core::PeerId::new_with_rng(&mut rng))
    }

    /// Create a PeerId from a string
    #[staticmethod]
    fn from_string(s: String) -> Self {
        PyPeerId(samod_core::PeerId::from_string(s))
    }

    /// Get the string representation of this PeerId
    fn to_string(&self) -> String {
        self.0.as_str().to_string()
    }

    /// String representation for debugging
    fn __repr__(&self) -> String {
        format!("PeerId(\"{}\")", self.0)
    }

    /// String representation
    fn __str__(&self) -> String {
        self.0.to_string()
    }

    /// Equality comparison
    fn __eq__(&self, other: &Self) -> bool {
        self.0 == other.0
    }

    /// Hash support
    fn __hash__(&self) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut hasher = DefaultHasher::new();
        self.0.hash(&mut hasher);
        hasher.finish()
    }
}

// Note: FromPyObject and IntoPyObject are automatically implemented by the pyclass macro

/// Wrapper for samod_core::StorageId
///
/// Represents a storage identifier, typically a UUID string. Each storage backend
/// has a unique storage ID.
#[pyclass(name = "StorageId")]
#[derive(Clone)]
pub struct PyStorageId(pub(crate) samod_core::StorageId);

#[pymethods]
impl PyStorageId {
    /// Create a new random StorageId (UUID)
    #[staticmethod]
    fn random() -> Self {
        let mut rng = rand::rng();
        PyStorageId(samod_core::StorageId::new(&mut rng))
    }

    /// Create a StorageId from a string
    #[staticmethod]
    fn from_string(s: String) -> Self {
        PyStorageId(samod_core::StorageId::from(s))
    }

    /// Get the string representation of this StorageId
    fn to_string(&self) -> String {
        self.0.to_string()
    }

    /// String representation for debugging
    fn __repr__(&self) -> String {
        format!("StorageId(\"{}\")", self.0)
    }

    /// String representation
    fn __str__(&self) -> String {
        self.0.to_string()
    }

    /// Equality comparison
    fn __eq__(&self, other: &Self) -> bool {
        self.0 == other.0
    }

    /// Hash support
    fn __hash__(&self) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut hasher = DefaultHasher::new();
        self.0.hash(&mut hasher);
        hasher.finish()
    }
}

/// Wrapper for samod_core::ConnectionId
///
/// Represents a unique identifier for a network connection. Connection IDs are
/// automatically generated when connections are created.
#[pyclass(name = "ConnectionId")]
#[derive(Clone, Copy)]
pub struct PyConnectionId(pub(crate) samod_core::ConnectionId);

#[pymethods]
impl PyConnectionId {
    /// Create a ConnectionId from a u32
    #[staticmethod]
    fn from_u32(id: u32) -> Self {
        PyConnectionId(samod_core::ConnectionId::from(id))
    }

    /// Get the u32 representation of this ConnectionId
    fn to_u32(&self) -> u32 {
        self.0.into()
    }

    /// String representation for debugging
    fn __repr__(&self) -> String {
        format!("ConnectionId({})", u32::from(self.0))
    }

    /// String representation
    fn __str__(&self) -> String {
        format!("{}", u32::from(self.0))
    }

    /// Equality comparison
    fn __eq__(&self, other: &Self) -> bool {
        self.0 == other.0
    }

    /// Hash support
    fn __hash__(&self) -> u64 {
        u32::from(self.0) as u64
    }
}

/// Wrapper for samod_core::DocumentActorId
///
/// Represents a unique identifier for a document actor. Each document in the repo
/// gets its own actor with a unique ID.
#[pyclass(name = "DocumentActorId")]
#[derive(Clone, Copy)]
pub struct PyDocumentActorId(pub(crate) samod_core::DocumentActorId);

#[pymethods]
impl PyDocumentActorId {
    /// Create a new DocumentActorId
    #[staticmethod]
    fn new() -> Self {
        PyDocumentActorId(samod_core::DocumentActorId::new())
    }

    /// Create a DocumentActorId from a u32
    #[staticmethod]
    fn from_u32(id: u32) -> Self {
        PyDocumentActorId(samod_core::DocumentActorId::from(id))
    }

    /// Get the u32 representation of this DocumentActorId
    fn to_u32(&self) -> u32 {
        self.0.into()
    }

    /// String representation for debugging
    fn __repr__(&self) -> String {
        format!("DocumentActorId({})", self.0)
    }

    /// String representation
    fn __str__(&self) -> String {
        self.0.to_string()
    }

    /// Equality comparison
    fn __eq__(&self, other: &Self) -> bool {
        self.0 == other.0
    }

    /// Hash support
    fn __hash__(&self) -> u64 {
        u32::from(self.0) as u64
    }
}

/// Wrapper for samod_core::DocumentId
#[pyclass(name = "DocumentId")]
#[derive(Clone)]
pub struct PyDocumentId(pub(crate) samod_core::DocumentId);

#[pymethods]
impl PyDocumentId {
    /// Create a DocumentId from a string
    #[staticmethod]
    fn from_string(s: String) -> PyResult<Self> {
        use std::str::FromStr;
        samod_core::DocumentId::from_str(&s)
            .map(PyDocumentId)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))
    }

    /// Create a DocumentId from bytes
    #[staticmethod]
    fn from_bytes(bytes: Vec<u8>) -> PyResult<Self> {
        samod_core::DocumentId::try_from(bytes)
            .map(PyDocumentId)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))
    }

    /// Get the string representation of this DocumentId
    fn to_string(&self) -> String {
        self.0.to_string()
    }

    /// Get the bytes representation of this DocumentId
    fn to_bytes<'py>(&self, py: Python<'py>) -> Bound<'py, pyo3::types::PyBytes> {
        pyo3::types::PyBytes::new(py, self.0.as_bytes())
    }

    /// String representation for debugging
    fn __repr__(&self) -> String {
        format!("DocumentId(\"{}\")", self.0)
    }

    /// String representation
    fn __str__(&self) -> String {
        self.0.to_string()
    }

    /// Equality comparison
    fn __eq__(&self, other: &Self) -> bool {
        self.0 == other.0
    }

    /// Hash support
    fn __hash__(&self) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut hasher = DefaultHasher::new();
        self.0.hash(&mut hasher);
        hasher.finish()
    }
}

/// Wrapper for samod_core::AutomergeUrl
///
/// Represents a URL to an Automerge document or a path within a document.
/// Format: "automerge:{document_id}[/path/to/property]"
#[pyclass(name = "AutomergeUrl")]
#[derive(Clone)]
pub struct PyAutomergeUrl(pub(crate) samod_core::AutomergeUrl);

#[pymethods]
impl PyAutomergeUrl {
    /// Create an AutomergeUrl from a string
    #[staticmethod]
    fn from_str(s: String) -> PyResult<Self> {
        use std::str::FromStr;
        samod_core::AutomergeUrl::from_str(&s)
            .map(PyAutomergeUrl)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))
    }

    /// Create an AutomergeUrl from a DocumentId
    #[staticmethod]
    fn from_document_id(document_id: &PyDocumentId) -> PyResult<Self> {
        use std::str::FromStr;
        let url_str = format!("automerge:{}", document_id.0);
        samod_core::AutomergeUrl::from_str(&url_str)
            .map(PyAutomergeUrl)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))
    }

    /// Get the document ID from this AutomergeUrl
    fn document_id(&self) -> PyResult<PyDocumentId> {
        use std::str::FromStr;
        // Extract the document ID by parsing the URL string
        // Format: "automerge:<document_id>[/path...]"
        let url_str = self.0.to_string();
        let doc_id_str = url_str
            .strip_prefix("automerge:")
            .ok_or_else(|| {
                PyErr::new::<pyo3::exceptions::PyValueError, _>("Invalid automerge URL")
            })?
            .split('/')
            .next()
            .ok_or_else(|| {
                PyErr::new::<pyo3::exceptions::PyValueError, _>("No document ID in URL")
            })?;

        samod_core::DocumentId::from_str(doc_id_str)
            .map(PyDocumentId)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))
    }

    /// Get the string representation of this AutomergeUrl
    fn to_str(&self) -> String {
        self.0.to_string()
    }

    /// String representation for debugging
    fn __repr__(&self) -> String {
        format!("AutomergeUrl(\"{}\")", self.0)
    }

    /// String representation
    fn __str__(&self) -> String {
        self.0.to_string()
    }

    /// Equality comparison
    fn __eq__(&self, other: &Self) -> bool {
        self.0.to_string() == other.0.to_string()
    }

    /// Hash support
    fn __hash__(&self) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut hasher = DefaultHasher::new();
        self.0.to_string().hash(&mut hasher);
        hasher.finish()
    }
}
