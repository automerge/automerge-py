use pyo3::prelude::*;

use super::hub_events::PyHubEvent;
use super::types::{PyConnectionId, PyDocumentActorId, PyDocumentId};

/// Wrapper for samod_core::CommandId
///
/// Represents a unique identifier for a command dispatched to the hub.
#[pyclass(name = "CommandId")]
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct PyCommandId(pub(crate) samod_core::CommandId);

#[pymethods]
impl PyCommandId {
    /// Create a CommandId from a u32
    #[staticmethod]
    fn from_u32(id: u32) -> Self {
        PyCommandId(samod_core::CommandId::from(id))
    }

    /// Get the u32 representation of this CommandId
    fn to_u32(&self) -> u32 {
        self.0.into()
    }

    /// String representation for debugging
    fn __repr__(&self) -> String {
        format!("CommandId({})", self.0)
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

/// Wrapper for samod_core::actors::hub::DispatchedCommand
///
/// A command that has been prepared for execution with a unique identifier.
#[pyclass(name = "DispatchedCommand")]
pub struct PyDispatchedCommand {
    command_id: samod_core::actors::hub::CommandId,
    event: samod_core::actors::hub::HubEvent,
}

#[pymethods]
impl PyDispatchedCommand {
    /// Get the command ID for tracking completion
    #[getter]
    fn command_id(&self) -> PyCommandId {
        PyCommandId(self.command_id)
    }

    /// Get the event to be processed
    #[getter]
    fn event(&self) -> PyHubEvent {
        PyHubEvent {
            inner: self.event.clone(),
        }
    }

    fn __repr__(&self) -> String {
        format!("DispatchedCommand(command_id={})", self.command_id)
    }
}

impl PyDispatchedCommand {
    pub(crate) fn new(
        command_id: samod_core::actors::hub::CommandId,
        event: samod_core::actors::hub::HubEvent,
    ) -> Self {
        PyDispatchedCommand { command_id, event }
    }
}

// ===== Command Result =====

/// CreateConnection command result - new connection was created
#[pyclass(name = "CommandResultCreateConnection")]
#[derive(Clone)]
pub struct PyCommandResultCreateConnection {
    #[pyo3(get)]
    connection_id: PyConnectionId,
}

#[pymethods]
impl PyCommandResultCreateConnection {
    #[new]
    fn new(connection_id: PyConnectionId) -> Self {
        PyCommandResultCreateConnection { connection_id }
    }

    fn __repr__(&self) -> String {
        format!(
            "CommandResultCreateConnection(connection_id={:?})",
            self.connection_id.0
        )
    }
}

/// DisconnectConnection command result - connection was disconnected
#[pyclass(name = "CommandResultDisconnectConnection")]
#[derive(Clone)]
pub struct PyCommandResultDisconnectConnection;

#[pymethods]
impl PyCommandResultDisconnectConnection {
    #[new]
    fn new() -> Self {
        PyCommandResultDisconnectConnection
    }

    fn __repr__(&self) -> String {
        "CommandResultDisconnectConnection".to_string()
    }
}

/// Receive command result - message was received on a connection
#[pyclass(name = "CommandResultReceive")]
#[derive(Clone)]
pub struct PyCommandResultReceive {
    #[pyo3(get)]
    connection_id: PyConnectionId,
    #[pyo3(get)]
    error: Option<String>,
}

#[pymethods]
impl PyCommandResultReceive {
    #[new]
    fn new(connection_id: PyConnectionId, error: Option<String>) -> Self {
        PyCommandResultReceive {
            connection_id,
            error,
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "CommandResultReceive(connection_id={:?}, error={:?})",
            self.connection_id.0, self.error
        )
    }
}

/// ActorReady command result - actor is ready to process messages
#[pyclass(name = "CommandResultActorReady")]
#[derive(Clone)]
pub struct PyCommandResultActorReady;

#[pymethods]
impl PyCommandResultActorReady {
    #[new]
    fn new() -> Self {
        PyCommandResultActorReady
    }

    fn __repr__(&self) -> String {
        "CommandResultActorReady".to_string()
    }
}

/// CreateDocument command result - new document was created
#[pyclass(name = "CommandResultCreateDocument")]
#[derive(Clone)]
pub struct PyCommandResultCreateDocument {
    #[pyo3(get)]
    actor_id: PyDocumentActorId,
    #[pyo3(get)]
    document_id: PyDocumentId,
}

#[pymethods]
impl PyCommandResultCreateDocument {
    #[new]
    fn new(actor_id: PyDocumentActorId, document_id: PyDocumentId) -> Self {
        PyCommandResultCreateDocument {
            actor_id,
            document_id,
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "CommandResultCreateDocument(actor_id={}, document_id={})",
            self.actor_id.0, self.document_id.0
        )
    }
}

/// FindDocument command result - document was found (or not)
#[pyclass(name = "CommandResultFindDocument")]
#[derive(Clone)]
pub struct PyCommandResultFindDocument {
    #[pyo3(get)]
    actor_id: PyDocumentActorId,
    #[pyo3(get)]
    found: bool,
}

#[pymethods]
impl PyCommandResultFindDocument {
    #[new]
    fn new(actor_id: PyDocumentActorId, found: bool) -> Self {
        PyCommandResultFindDocument { actor_id, found }
    }

    fn __repr__(&self) -> String {
        format!(
            "CommandResultFindDocument(actor_id={}, found={})",
            self.actor_id.0, self.found
        )
    }
}

// Helper function to convert Rust CommandResult to appropriate Python subclass
pub(crate) fn command_result_to_py(
    py: Python<'_>,
    result: &samod_core::actors::hub::CommandResult,
) -> PyResult<PyObject> {
    match result {
        samod_core::actors::hub::CommandResult::CreateConnection { connection_id } => Py::new(
            py,
            PyCommandResultCreateConnection {
                connection_id: PyConnectionId(*connection_id),
            },
        )
        .map(|obj| obj.into()),
        samod_core::actors::hub::CommandResult::DisconnectConnection => {
            Py::new(py, PyCommandResultDisconnectConnection).map(|obj| obj.into())
        }
        samod_core::actors::hub::CommandResult::Receive {
            connection_id,
            error,
        } => Py::new(
            py,
            PyCommandResultReceive {
                connection_id: PyConnectionId(*connection_id),
                error: error.clone(),
            },
        )
        .map(|obj| obj.into()),
        samod_core::actors::hub::CommandResult::ActorReady => {
            Py::new(py, PyCommandResultActorReady).map(|obj| obj.into())
        }
        samod_core::actors::hub::CommandResult::CreateDocument {
            actor_id,
            document_id,
        } => Py::new(
            py,
            PyCommandResultCreateDocument {
                actor_id: PyDocumentActorId(*actor_id),
                document_id: PyDocumentId(document_id.clone()),
            },
        )
        .map(|obj| obj.into()),
        samod_core::actors::hub::CommandResult::FindDocument { actor_id, found } => Py::new(
            py,
            PyCommandResultFindDocument {
                actor_id: PyDocumentActorId(*actor_id),
                found: *found,
            },
        )
        .map(|obj| obj.into()),
    }
}
