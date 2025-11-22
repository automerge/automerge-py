//! Connection info types
//!
//! This module contains types for representing connection information
//! such as connection state, peer document synchronization state, etc.

use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::collections::HashMap;

use super::types::{PyConnectionId, PyPeerId, PyDocumentId};

// ===== Connection State =====

/// Base class for connection state
#[pyclass(name = "ConnectionState", subclass)]
#[derive(Clone)]
pub struct PyConnectionState;

#[pymethods]
impl PyConnectionState {
    fn __repr__(&self) -> String {
        "ConnectionState".to_string()
    }
}

/// Connection state: Handshaking - still exchanging peer IDs
#[pyclass(name = "ConnectionStateHandshaking", extends = PyConnectionState)]
#[derive(Clone)]
pub struct PyConnectionStateHandshaking;

#[pymethods]
impl PyConnectionStateHandshaking {
    #[new]
    fn new() -> (Self, PyConnectionState) {
        (PyConnectionStateHandshaking, PyConnectionState)
    }

    fn __repr__(&self) -> String {
        "ConnectionStateHandshaking()".to_string()
    }
}

/// Connection state: Connected - connected with a peer and synchronizing documents
#[pyclass(name = "ConnectionStateConnected", extends = PyConnectionState)]
#[derive(Clone)]
pub struct PyConnectionStateConnected {
    /// The peer ID of the connected peer
    #[pyo3(get)]
    pub their_peer_id: PyPeerId,
}

#[pymethods]
impl PyConnectionStateConnected {
    #[new]
    fn new(their_peer_id: PyPeerId) -> (Self, PyConnectionState) {
        (PyConnectionStateConnected { their_peer_id }, PyConnectionState)
    }

    fn __repr__(&self) -> String {
        format!("ConnectionStateConnected(their_peer_id={})", self.their_peer_id.0)
    }
}

/// Helper enum for internal use
pub(crate) enum ConnectionStateVariant {
    Handshaking(Py<PyConnectionStateHandshaking>),
    Connected(Py<PyConnectionStateConnected>),
}

impl Clone for ConnectionStateVariant {
    fn clone(&self) -> Self {
        Python::with_gil(|py| match self {
            ConnectionStateVariant::Handshaking(h) => {
                ConnectionStateVariant::Handshaking(h.clone_ref(py))
            }
            ConnectionStateVariant::Connected(c) => {
                ConnectionStateVariant::Connected(c.clone_ref(py))
            }
        })
    }
}

impl ConnectionStateVariant {
    pub fn into_py(self, _py: Python<'_>) -> PyObject {
        match self {
            ConnectionStateVariant::Handshaking(h) => h.into(),
            ConnectionStateVariant::Connected(c) => c.into(),
        }
    }
}

impl ConnectionStateVariant {
    pub fn from_rust(py: Python<'_>, state: &samod_core::network::ConnectionState) -> Self {
        match state {
            samod_core::network::ConnectionState::Handshaking => {
                let handshaking = Bound::new(py, PyConnectionStateHandshaking::new())
                    .unwrap()
                    .unbind();
                ConnectionStateVariant::Handshaking(handshaking)
            }
            samod_core::network::ConnectionState::Connected { their_peer_id } => {
                let peer_id = PyPeerId(their_peer_id.clone());
                let connected = Bound::new(py, PyConnectionStateConnected::new(peer_id))
                    .unwrap()
                    .unbind();
                ConnectionStateVariant::Connected(connected)
            }
        }
    }
}

// ===== Peer Document State =====

/// Synchronization state for one (peer, document) pair
#[pyclass(name = "PeerDocState")]
#[derive(Clone)]
pub struct PyPeerDocState {
    /// When we last received a message from this peer for this document
    #[pyo3(get)]
    pub last_received: Option<f64>,

    /// When we last sent a message to this peer for this document
    #[pyo3(get)]
    pub last_sent: Option<f64>,

    /// The heads of the document when we last sent a message (as strings)
    #[pyo3(get)]
    pub last_sent_heads: Option<Vec<crate::PyChangeHash>>,

    /// The last heads of the document that the peer said they had (as strings)
    #[pyo3(get)]
    pub last_acked_heads: Option<Vec<crate::PyChangeHash>>,
}

#[pymethods]
impl PyPeerDocState {
    fn __repr__(&self) -> String {
        format!(
            "PeerDocState(last_received={:?}, last_sent={:?}, sent_heads={}, acked_heads={})",
            self.last_received,
            self.last_sent,
            self.last_sent_heads.as_ref().map_or(0, |v| v.len()),
            self.last_acked_heads.as_ref().map_or(0, |v| v.len())
        )
    }
}

impl From<&samod_core::network::PeerDocState> for PyPeerDocState {
    fn from(state: &samod_core::network::PeerDocState) -> Self {
        PyPeerDocState {
            last_received: state.last_received.map(|ts| ts.as_millis() as f64 / 1000.0),
            last_sent: state.last_sent.map(|ts| ts.as_millis() as f64 / 1000.0),
            last_sent_heads: state.last_sent_heads.as_ref().map(|heads| {
                heads.iter().map(|h| crate::PyChangeHash(*h)).collect()
            }),
            last_acked_heads: state.last_acked_heads.as_ref().map(|heads| {
                heads.iter().map(|h| crate::PyChangeHash(*h)).collect()
            }),
        }
    }
}

// ===== Connection Info =====

/// Information about a live connection
#[pyclass(name = "ConnectionInfo")]
#[derive(Clone)]
pub struct PyConnectionInfo {
    /// Connection ID
    #[pyo3(get)]
    pub id: PyConnectionId,

    /// When we last received any message from this connection
    #[pyo3(get)]
    pub last_received: Option<f64>,

    /// When we last sent any message to this connection
    #[pyo3(get)]
    pub last_sent: Option<f64>,

    /// Connection state (handshaking or connected)
    state: ConnectionStateVariant,

    /// Synchronization state for each document (internal storage)
    docs: HashMap<samod_core::DocumentId, PyPeerDocState>,
}

#[pymethods]
impl PyConnectionInfo {
    /// Get the connection state
    #[getter]
    fn state(&self, py: Python<'_>) -> PyObject {
        self.state.clone().into_py(py)
    }

    /// Get the synchronization state for all documents
    ///
    /// Returns a dict mapping DocumentId to PeerDocState
    fn docs<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let dict = PyDict::new(py);
        for (doc_id, state) in &self.docs {
            dict.set_item(PyDocumentId(doc_id.clone()), state.clone())?;
        }
        Ok(dict)
    }

    /// Get the synchronization state for a specific document
    fn doc_state(&self, document_id: PyDocumentId) -> Option<PyPeerDocState> {
        self.docs.get(&document_id.0).cloned()
    }

    fn __repr__(&self) -> String {
        let state_str = match &self.state {
            ConnectionStateVariant::Handshaking(_) => "Handshaking",
            ConnectionStateVariant::Connected(_) => "Connected",
        };
        format!(
            "ConnectionInfo(id={}, state={}, docs={})",
            u32::from(self.id.0),
            state_str,
            self.docs.len()
        )
    }
}

impl PyConnectionInfo {
    pub(crate) fn from_rust(py: Python<'_>, info: &samod_core::network::ConnectionInfo) -> Self {
        PyConnectionInfo {
            id: PyConnectionId(info.id),
            last_received: info.last_received.map(|ts| ts.as_millis() as f64 / 1000.0),
            last_sent: info.last_sent.map(|ts| ts.as_millis() as f64 / 1000.0),
            state: ConnectionStateVariant::from_rust(py, &info.state),
            docs: info.docs.iter()
                .map(|(doc_id, state)| (doc_id.clone(), PyPeerDocState::from(state)))
                .collect(),
        }
    }
}
