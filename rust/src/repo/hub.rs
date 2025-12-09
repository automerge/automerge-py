//! Hub actor
//!
//! This module contains the Hub type, which is the central coordinator
//! for the repository.

use pyo3::prelude::*;

use super::types::{PyPeerId, PyStorageId};
use super::hub_events::PyHubEvent;
use super::hub_results::PyHubResults;

/// Wrapper for samod_core::actors::hub::Hub
///
/// The Hub is the central coordinator for the repository. It manages
/// document actors, connections, and storage operations.
#[pyclass(name = "Hub")]
pub struct PyHub {
    pub(crate) inner: std::sync::Arc<std::sync::Mutex<samod_core::actors::hub::Hub>>,
}

#[pymethods]
impl PyHub {
    /// Get the storage ID for this Hub
    ///
    /// The storage ID identifies the storage layer this peer is connected to.
    fn storage_id(&self) -> PyStorageId {
        let guard = self.inner.lock().unwrap();
        PyStorageId(guard.storage_id())
    }

    /// Get the peer ID for this Hub
    ///
    /// The peer ID is a unique identifier for this specific peer instance.
    fn peer_id(&self) -> PyPeerId {
        let guard = self.inner.lock().unwrap();
        PyPeerId(guard.peer_id())
    }

    /// Check if this Hub has been stopped
    fn is_stopped(&self) -> bool {
        let guard = self.inner.lock().unwrap();
        guard.is_stopped()
    }

    /// Get information about all active connections
    ///
    /// Returns a list of ConnectionInfo objects describing each active connection,
    /// including connection state, peer information, and per-document sync state.
    fn connections(&self, py: Python<'_>) -> Vec<super::connection::PyConnectionInfo> {
        let guard = self.inner.lock().unwrap();
        guard.connections()
            .iter()
            .map(|info| super::connection::PyConnectionInfo::from_rust(py, info))
            .collect()
    }

    /// Handle an event and return the results
    ///
    /// This is the main interface for interacting with the Hub. Events can be
    /// commands to execute, IO completion notifications, or network messages.
    ///
    /// Args:
    ///     now: Current Unix timestamp in seconds (float)
    ///     event: The event to process
    ///
    /// Returns:
    ///     HubResults containing new tasks and completed commands
    fn handle_event<'py>(&self, py: Python<'py>, now: f64, event: &PyHubEvent) -> PyResult<PyHubResults> {
        let mut guard = self.inner.lock().unwrap();
        let mut rng = rand::rng();
        let timestamp = samod_core::UnixTimestamp::from_millis((now * 1000.0) as u128);

        // Convert PyHubEvent to samod_core::HubEvent
        let rust_event = event.to_rust_event();

        let results = guard.handle_event(&mut rng, timestamp, rust_event);

        PyHubResults::from_rust(py, results)
    }

    fn __repr__(&self) -> String {
        let guard = self.inner.lock().unwrap();
        let peer_id = guard.peer_id();
        let stopped = guard.is_stopped();
        format!("Hub(peer_id={}, stopped={})", peer_id.as_str(), stopped)
    }
}
