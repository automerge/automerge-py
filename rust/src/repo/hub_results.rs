//! Hub result types
//!
//! This module contains types for results returned from Hub event processing,
//! including ConnectionEvent variants and HubResults.

use pyo3::prelude::*;

use super::commands::{command_result_to_py, PyCommandId};
use super::document::{PyHubToDocMsg, PySpawnArgs};
use super::hub_events::PyPeerInfo;
use super::io::PyIoTask;
use super::types::{PyConnectionId, PyDocumentActorId};

// ===== Connection Event =====

/// HandshakeCompleted event - connection handshake successfully completed
#[pyclass(name = "ConnectionEventHandshakeCompleted")]
#[derive(Clone)]
pub struct PyConnectionEventHandshakeCompleted {
    #[pyo3(get)]
    connection_id: PyConnectionId,
    #[pyo3(get)]
    peer_info: PyPeerInfo,
}

#[pymethods]
impl PyConnectionEventHandshakeCompleted {
    #[new]
    fn new(connection_id: PyConnectionId, peer_info: PyPeerInfo) -> Self {
        PyConnectionEventHandshakeCompleted {
            connection_id,
            peer_info,
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "ConnectionEventHandshakeCompleted(connection_id={:?}, peer_id={})",
            self.connection_id.0,
            self.peer_info.inner.peer_id.as_str()
        )
    }
}

/// ConnectionFailed event - connection attempt failed
#[pyclass(name = "ConnectionEventConnectionFailed")]
#[derive(Clone)]
pub struct PyConnectionEventConnectionFailed {
    #[pyo3(get)]
    connection_id: PyConnectionId,
    #[pyo3(get)]
    error: String,
}

#[pymethods]
impl PyConnectionEventConnectionFailed {
    #[new]
    fn new(connection_id: PyConnectionId, error: String) -> Self {
        PyConnectionEventConnectionFailed {
            connection_id,
            error,
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "ConnectionEventConnectionFailed(connection_id={:?}, error={})",
            self.connection_id.0, self.error
        )
    }
}

/// StateChanged event - connection state has changed
#[pyclass(name = "ConnectionEventStateChanged")]
#[derive(Clone)]
pub struct PyConnectionEventStateChanged {
    #[pyo3(get)]
    connection_id: PyConnectionId,
    #[pyo3(get)]
    new_state: String,
}

#[pymethods]
impl PyConnectionEventStateChanged {
    #[new]
    fn new(connection_id: PyConnectionId, new_state: String) -> Self {
        PyConnectionEventStateChanged {
            connection_id,
            new_state,
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "ConnectionEventStateChanged(connection_id={:?}, new_state={})",
            self.connection_id.0, self.new_state
        )
    }
}

// Helper function to convert Rust ConnectionEvent to appropriate Python subclass
pub(crate) fn connection_event_to_py(
    py: Python<'_>,
    event: &samod_core::network::ConnectionEvent,
) -> PyResult<PyObject> {
    match event {
        samod_core::network::ConnectionEvent::HandshakeCompleted {
            connection_id,
            peer_info,
        } => Py::new(
            py,
            PyConnectionEventHandshakeCompleted {
                connection_id: PyConnectionId(*connection_id),
                peer_info: PyPeerInfo {
                    inner: peer_info.clone(),
                },
            },
        )
        .map(|obj| obj.into()),
        samod_core::network::ConnectionEvent::ConnectionFailed {
            connection_id,
            error,
        } => Py::new(
            py,
            PyConnectionEventConnectionFailed {
                connection_id: PyConnectionId(*connection_id),
                error: error.clone(),
            },
        )
        .map(|obj| obj.into()),
        samod_core::network::ConnectionEvent::StateChanged {
            connection_id,
            new_state,
        } => {
            Py::new(
                py,
                PyConnectionEventStateChanged {
                    connection_id: PyConnectionId(*connection_id),
                    new_state: format!("{:?}", new_state), // Convert ConnectionInfo to string
                },
            )
            .map(|obj| obj.into())
        }
    }
}

// ===== Hub Results =====

/// Wrapper for samod_core::actors::hub::HubResults
///
/// Contains the results of processing a Hub event, including new IO tasks
/// and completed commands.
#[pyclass(name = "HubResults")]
#[derive(Clone)]
pub struct PyHubResults {
    pub(crate) inner: samod_core::actors::hub::HubResults,
}

#[pymethods]
impl PyHubResults {
    /// Get new IO tasks that need to be executed
    #[getter]
    fn new_tasks<'py>(&self, py: Python<'py>) -> PyResult<Vec<Py<PyIoTask>>> {
        self.inner
            .new_tasks
            .iter()
            .map(|task| {
                let py_task = PyIoTask::from(task.clone());
                Py::new(py, py_task)
            })
            .collect()
    }

    /// Get completed commands
    ///
    /// Returns a dict mapping CommandId to CommandResult.
    #[getter]
    fn completed_commands<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, pyo3::types::PyDict>> {
        let dict = pyo3::types::PyDict::new(py);
        for (cmd_id, result) in &self.inner.completed_commands {
            dict.set_item(PyCommandId(*cmd_id), command_result_to_py(py, result)?)?;
        }
        Ok(dict)
    }

    /// Get requests to spawn new document actors
    #[getter]
    fn spawn_actors(&self) -> Vec<PySpawnArgs> {
        self.inner
            .spawn_actors
            .iter()
            .map(|args| PySpawnArgs::from(args.clone()))
            .collect()
    }

    /// Get messages to send to document actors
    ///
    /// Returns a list of (DocumentActorId, HubToDocMsg) tuples.
    #[getter]
    fn actor_messages(&self) -> Vec<(PyDocumentActorId, PyHubToDocMsg)> {
        self.inner
            .actor_messages
            .iter()
            .map(|(id, msg)| (PyDocumentActorId(*id), PyHubToDocMsg::from(msg.clone())))
            .collect()
    }

    /// Get connection events emitted during processing
    #[getter]
    fn connection_events<'py>(&self, py: Python<'py>) -> PyResult<Vec<PyObject>> {
        self.inner
            .connection_events
            .iter()
            .map(|event| connection_event_to_py(py, event))
            .collect()
    }

    /// Check if the hub is stopped
    #[getter]
    fn stopped(&self) -> bool {
        self.inner.stopped
    }

    fn __repr__(&self) -> String {
        format!("HubResults(stopped={})", self.inner.stopped)
    }
}

impl PyHubResults {
    pub(crate) fn from_rust(
        _py: Python<'_>,
        results: samod_core::actors::hub::HubResults,
    ) -> PyResult<Self> {
        Ok(PyHubResults { inner: results })
    }
}
