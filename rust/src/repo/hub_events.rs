//! Hub event types
//!
//! This module contains types for events sent to the Hub,
//! including PeerInfo and HubEvent.

use pyo3::prelude::*;

use super::commands::PyDispatchedCommand;
use super::document::PyDocToHubMsg;
use super::io::PyIoResult;
use super::types::{PyConnectionId, PyDocumentActorId, PyDocumentId, PyPeerId};

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
        format!(
            "PeerInfo(peer_id={}, protocol_version={})",
            self.inner.peer_id.as_str(),
            self.inner.protocol_version
        )
    }
}

impl From<samod_core::network::PeerInfo> for PyPeerInfo {
    fn from(info: samod_core::network::PeerInfo) -> Self {
        PyPeerInfo { inner: info }
    }
}

/// Wrapper for samod_core::actors::hub::HubEvent
///
/// Represents an event that can be sent to the Hub for processing.
#[derive(Clone)]
pub(crate) enum HubEventKind {
    Core(samod_core::actors::hub::HubEvent),
    FindDocument {
        command_id: samod_core::CommandId,
        document_id: samod_core::DocumentId,
        event: samod_core::actors::hub::HubEvent,
    },
}

impl From<samod_core::actors::hub::HubEvent> for HubEventKind {
    fn from(event: samod_core::actors::hub::HubEvent) -> Self {
        Self::Core(event)
    }
}

#[pyclass(name = "HubEvent")]
#[derive(Clone)]
pub struct PyHubEvent {
    pub(crate) inner: HubEventKind,
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
            inner: samod_core::actors::hub::HubEvent::io_complete(io_result.to_core()).into(),
        }
    }

    /// Create a tick event for periodic processing
    #[staticmethod]
    fn tick() -> Self {
        PyHubEvent {
            inner: samod_core::actors::hub::HubEvent::tick().into(),
        }
    }

    /// Create an event indicating that a connection was lost externally
    ///
    /// Args:
    ///     connection_id: The ID of the connection that was lost
    #[staticmethod]
    fn connection_lost(connection_id: PyConnectionId) -> Self {
        PyHubEvent {
            inner: samod_core::actors::hub::HubEvent::connection_lost(connection_id.0).into(),
        }
    }

    /// Create an event to stop the Hub
    #[staticmethod]
    fn stop() -> Self {
        PyHubEvent {
            inner: samod_core::actors::hub::HubEvent::stop().into(),
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
            inner: samod_core::actors::hub::HubEvent::actor_message(actor_id.0, message.inner)
                .into(),
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

    /// Register a persistent outgoing connector. Durations are in seconds.
    #[staticmethod]
    #[pyo3(signature = (url, initial_delay=0.1, max_delay=30.0, max_retries=None))]
    fn add_dialer(
        url: String,
        initial_delay: f64,
        max_delay: f64,
        max_retries: Option<u32>,
    ) -> PyResult<PyDispatchedCommand> {
        use pyo3::exceptions::PyValueError;
        use samod_core::network::{BackoffConfig, DialerConfig};
        use std::time::Duration;

        let initial_delay = Duration::try_from_secs_f64(initial_delay)
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        let max_delay = Duration::try_from_secs_f64(max_delay)
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        if max_delay < initial_delay {
            return Err(PyValueError::new_err("max_delay must be >= initial_delay"));
        }
        let config = DialerConfig {
            url: url
                .parse()
                .map_err(|e| PyValueError::new_err(format!("{e}")))?,
            backoff: BackoffConfig {
                initial_delay,
                max_delay,
                max_retries,
            },
        };
        let command = samod_core::actors::hub::HubEvent::add_dialer(config);
        Ok(PyDispatchedCommand::new(command.command_id, command.event))
    }

    #[staticmethod]
    fn add_listener(url: String) -> PyResult<PyDispatchedCommand> {
        let config = samod_core::network::ListenerConfig {
            url: url
                .parse()
                .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("{e}")))?,
        };
        let command = samod_core::actors::hub::HubEvent::add_listener(config);
        Ok(PyDispatchedCommand::new(command.command_id, command.event))
    }

    #[staticmethod]
    #[pyo3(signature = (dialer_id, expected_peer_id=None))]
    fn create_dialer_connection(
        dialer_id: u32,
        expected_peer_id: Option<PyPeerId>,
    ) -> PyDispatchedCommand {
        let command = samod_core::actors::hub::HubEvent::create_dialer_connection(
            dialer_id.into(),
            expected_peer_id.map(|id| id.0),
        );
        PyDispatchedCommand::new(command.command_id, command.event)
    }

    #[staticmethod]
    #[pyo3(signature = (listener_id, expected_peer_id=None))]
    fn create_listener_connection(
        listener_id: u32,
        expected_peer_id: Option<PyPeerId>,
    ) -> PyDispatchedCommand {
        let command = samod_core::actors::hub::HubEvent::create_listener_connection(
            listener_id.into(),
            expected_peer_id.map(|id| id.0),
        );
        PyDispatchedCommand::new(command.command_id, command.event)
    }

    #[staticmethod]
    fn dial_failed(dialer_id: u32, error: String, permanent: bool) -> Self {
        Self {
            inner: samod_core::actors::hub::HubEvent::dial_failed(
                dialer_id.into(),
                error,
                permanent,
            )
            .into(),
        }
    }

    #[staticmethod]
    fn remove_dialer(dialer_id: u32) -> Self {
        Self {
            inner: samod_core::actors::hub::HubEvent::remove_dialer(dialer_id.into()).into(),
        }
    }

    #[staticmethod]
    fn remove_listener(listener_id: u32) -> Self {
        Self {
            inner: samod_core::actors::hub::HubEvent::remove_listener(listener_id.into()).into(),
        }
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
        PyDispatchedCommand::find_document(document_id.0)
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
