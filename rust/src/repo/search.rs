//! Direct, immutable snapshots of samod's document-search state.

use pyo3::prelude::*;
use pyo3::types::PyDict;

use super::types::PyConnectionId;

#[pyclass(name = "DocSearchPhase", eq, eq_int)]
#[derive(Clone, Copy, PartialEq)]
pub enum PyDocSearchPhase {
    Loading,
    Searching,
    Ready,
}

#[pyclass(name = "PeerRequestState", eq, eq_int)]
#[derive(Clone, Copy, PartialEq)]
pub enum PyPeerRequestState {
    Requested,
    Unavailable,
    Syncing,
    Available,
}

#[pyclass(name = "DocSearch", frozen)]
#[derive(Clone)]
pub struct PyDocSearch(pub(crate) samod_core::DocSearch);

#[pymethods]
impl PyDocSearch {
    #[getter]
    fn phase(&self) -> PyDocSearchPhase {
        match self.0.phase() {
            samod_core::DocSearchPhase::Loading => PyDocSearchPhase::Loading,
            samod_core::DocSearchPhase::Searching(_) => PyDocSearchPhase::Searching,
            samod_core::DocSearchPhase::Ready => PyDocSearchPhase::Ready,
        }
    }

    /// Per-connection request states; empty outside the Searching phase.
    #[getter]
    fn peers<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let dict = PyDict::new(py);
        if let samod_core::DocSearchPhase::Searching(peers) = self.0.phase() {
            for (connection_id, state) in peers {
                let state = match state {
                    samod_core::PeerRequestState::Requested => PyPeerRequestState::Requested,
                    samod_core::PeerRequestState::Unavailable => PyPeerRequestState::Unavailable,
                    samod_core::PeerRequestState::Syncing => PyPeerRequestState::Syncing,
                    samod_core::PeerRequestState::Available => PyPeerRequestState::Available,
                };
                dict.set_item(PyConnectionId(*connection_id), state)?;
            }
        }
        Ok(dict)
    }

    #[getter]
    fn pending_connections(&self) -> Vec<String> {
        self.0
            .pending_connections()
            .iter()
            .map(ToString::to_string)
            .collect()
    }

    fn is_currently_unavailable(&self) -> bool {
        self.0.is_currently_unavailable()
    }

    fn __eq__(&self, other: &Self) -> bool {
        self.0 == other.0
    }

    fn __repr__(&self) -> String {
        format!("DocSearch({:?})", self.0)
    }
}
