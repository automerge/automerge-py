//! Hub event types
//!
//! This module contains types for events sent to the Hub,
//! including PeerInfo, ConnDirection, and HubEvent.

use pyo3::prelude::*;

use super::types::{PyPeerId, PyConnectionId, PyDocumentActorId, PyDocumentId};
use super::document::{PyDocToHubMsg};
use super::commands::PyDispatchedCommand;
use super::io::PyIoResult;

/// Wrapper for samod_core::network::PeerInfo
///
/// Information about a connected peer after successful handshake.
#[pyclass(name = "PeerInfo")]
#[derive(Clone)]
pub struct PyPeerInfo {
    pub(crate) inner: samod_core::network::PeerInfo,
}

#[pymethods]
impl PyPeerInfo {
    /// Get the peer ID
    #[getter]
    fn peer_id(&self) -> PyPeerId {
        PyPeerId(self.inner.peer_id.clone())
    }

    /// Get the protocol version
    #[getter]
    fn protocol_version(&self) -> String {
        self.inner.protocol_version.clone()
    }

    fn __repr__(&self) -> String {
        format!("PeerInfo(peer_id={}, protocol_version={})",
                self.inner.peer_id.as_str(), self.inner.protocol_version)
    }
}

impl From<samod_core::network::PeerInfo> for PyPeerInfo {
    fn from(info: samod_core::network::PeerInfo) -> Self {
        PyPeerInfo { inner: info }
    }
}

/// Wrapper for samod_core::network::ConnDirection
///
/// Indicates whether a connection is outgoing or incoming.
#[pyclass(name = "ConnDirection")]
#[derive(Clone, Copy)]
pub enum PyConnDirection {
    /// Connection initiated by this peer
    Outgoing,
    /// Connection accepted from a remote peer
    Incoming,
}

impl From<PyConnDirection> for samod_core::network::ConnDirection {
    fn from(dir: PyConnDirection) -> Self {
        match dir {
            PyConnDirection::Outgoing => samod_core::network::ConnDirection::Outgoing,
            PyConnDirection::Incoming => samod_core::network::ConnDirection::Incoming,
        }
    }
}

/// Wrapper for samod_core::actors::hub::HubEvent
///
/// Represents an event that can be sent to the Hub for processing.
#[pyclass(name = "HubEvent")]
#[derive(Clone)]
pub struct PyHubEvent {
    pub(crate) inner: samod_core::actors::hub::HubEvent,
}

#[pymethods]
impl PyHubEvent {
    /// Create an event indicating that an IO task has completed
    ///
    /// Args:
    ///     io_result: The result of the IO operation
    #[staticmethod]
    fn io_complete(io_result: PyIoResult) -> Self {
        PyHubEvent {
            inner: samod_core::actors::hub::HubEvent::io_complete(io_result.to_core()),
        }
    }

    /// Create a tick event for periodic processing
    #[staticmethod]
    fn tick() -> Self {
        PyHubEvent {
            inner: samod_core::actors::hub::HubEvent::tick(),
        }
    }

    /// Create an event indicating that a connection was lost externally
    ///
    /// Args:
    ///     connection_id: The ID of the connection that was lost
    #[staticmethod]
    fn connection_lost(connection_id: PyConnectionId) -> Self {
        PyHubEvent {
            inner: samod_core::actors::hub::HubEvent::connection_lost(connection_id.0),
        }
    }

    /// Create an event to stop the Hub
    #[staticmethod]
    fn stop() -> Self {
        PyHubEvent {
            inner: samod_core::actors::hub::HubEvent::stop(),
        }
    }

    /// Create an event indicating that a message was received from a document actor
    ///
    /// Args:
    ///     actor_id: The ID of the actor that sent the message
    ///     message: The message from the actor
    #[staticmethod]
    fn actor_message(actor_id: PyDocumentActorId, message: PyDocToHubMsg) -> Self {
        PyHubEvent {
            inner: samod_core::actors::hub::HubEvent::actor_message(actor_id.0, message.inner),
        }
    }

    /// Create a command to receive a message on a connection
    ///
    /// Args:
    ///     connection_id: The ID of the connection
    ///     msg: The message bytes
    ///
    /// Returns:
    ///     DispatchedCommand with command_id and event
    #[staticmethod]
    fn receive(connection_id: PyConnectionId, msg: Vec<u8>) -> PyDispatchedCommand {
        let dispatched = samod_core::actors::hub::HubEvent::receive(connection_id.0, msg);
        PyDispatchedCommand::new(dispatched.command_id, dispatched.event)
    }

    /// Create a command to create a new connection
    ///
    /// Args:
    ///     direction: Whether this is an outgoing or incoming connection
    ///
    /// Returns:
    ///     DispatchedCommand with command_id and event
    #[staticmethod]
    fn create_connection(direction: PyConnDirection) -> PyDispatchedCommand {
        let dispatched = samod_core::actors::hub::HubEvent::create_connection(direction.into());
        PyDispatchedCommand::new(dispatched.command_id, dispatched.event)
    }

    /// Create a command to create a new document
    ///
    /// Returns:
    ///     DispatchedCommand with command_id and event
    #[staticmethod]
    fn create_document() -> PyDispatchedCommand {
        let doc = automerge::Automerge::new();
        let dispatched = samod_core::actors::hub::HubEvent::create_document(doc);
        PyDispatchedCommand::new(dispatched.command_id, dispatched.event)
    }

    /// Create a command to find an existing document
    ///
    /// Args:
    ///     document_id: The ID of the document to find
    ///
    /// Returns:
    ///     DispatchedCommand with command_id and event
    #[staticmethod]
    fn find_document(document_id: PyDocumentId) -> PyDispatchedCommand {
        let dispatched = samod_core::actors::hub::HubEvent::find_document(document_id.0);
        PyDispatchedCommand::new(dispatched.command_id, dispatched.event)
    }

    /// Create a command indicating that a document actor is ready
    ///
    /// Args:
    ///     document_id: The ID of the document
    ///
    /// Returns:
    ///     DispatchedCommand with command_id and event
    #[staticmethod]
    fn actor_ready(document_id: PyDocumentId) -> PyDispatchedCommand {
        let dispatched = samod_core::actors::hub::HubEvent::actor_ready(document_id.0);
        PyDispatchedCommand::new(dispatched.command_id, dispatched.event)
    }

    fn __repr__(&self) -> String {
        format!("HubEvent(...)")
    }
}

impl PyHubEvent {
    /// Convert to Rust HubEvent
    pub(crate) fn to_rust_event(&self) -> samod_core::actors::hub::HubEvent {
        self.inner.clone()
    }
}
