//! Sharing decisions, enforced by samod-core before document messages are processed.

use pyo3::prelude::*;

#[pyclass(name = "ShareAccess", eq, eq_int)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PyShareAccess {
    // `None` is a Python keyword.
    None_,
    ReadOnly,
    ReadWrite,
}

#[pyclass(name = "ShareConfig", frozen, eq)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct PyShareConfig(pub(crate) samod_core::ShareConfig);

#[pymethods]
impl PyShareConfig {
    #[new]
    #[pyo3(signature = (access=PyShareAccess::ReadWrite, announce=true))]
    fn new(access: PyShareAccess, announce: bool) -> Self {
        Self(samod_core::ShareConfig {
            access: match access {
                PyShareAccess::None_ => samod_core::ShareAccess::None,
                PyShareAccess::ReadOnly => samod_core::ShareAccess::ReadOnly,
                PyShareAccess::ReadWrite => samod_core::ShareAccess::ReadWrite,
            },
            announce,
        })
    }

    #[staticmethod]
    fn none() -> Self {
        Self(samod_core::ShareConfig::none())
    }

    #[staticmethod]
    fn read_only() -> Self {
        Self(samod_core::ShareConfig::read_only())
    }

    #[staticmethod]
    fn read_write() -> Self {
        Self(samod_core::ShareConfig::read_write())
    }

    fn with_announce(&self, announce: bool) -> Self {
        Self(self.0.with_announce(announce))
    }

    #[getter]
    fn access(&self) -> PyShareAccess {
        match self.0.access {
            samod_core::ShareAccess::None => PyShareAccess::None_,
            samod_core::ShareAccess::ReadOnly => PyShareAccess::ReadOnly,
            samod_core::ShareAccess::ReadWrite => PyShareAccess::ReadWrite,
        }
    }

    #[getter]
    fn announce(&self) -> bool {
        self.0.announce
    }

    fn __repr__(&self) -> String {
        format!(
            "ShareConfig(access={:?}, announce={})",
            self.access(),
            self.0.announce
        )
    }
}

#[pyclass(name = "CheckSharePolicyAction")]
#[derive(Clone)]
pub struct PyCheckSharePolicyAction {
    #[pyo3(get)]
    pub peer_id: super::types::PyPeerId,
}

#[pymethods]
impl PyCheckSharePolicyAction {
    #[new]
    fn new(peer_id: super::types::PyPeerId) -> Self {
        Self { peer_id }
    }
}

#[pyclass(name = "DocumentIoResultCheckSharePolicy")]
#[derive(Clone)]
pub struct PyDocumentIoResultCheckSharePolicy {
    #[pyo3(get)]
    pub config: PyShareConfig,
}

#[pymethods]
impl PyDocumentIoResultCheckSharePolicy {
    #[new]
    fn new(config: PyShareConfig) -> Self {
        Self { config }
    }
}

#[pyclass(name = "CheckSharePolicyResultPayload")]
#[derive(Clone)]
pub struct PyCheckSharePolicyResultPayload {
    #[pyo3(get)]
    pub config: PyShareConfig,
}

#[pymethods]
impl PyCheckSharePolicyResultPayload {
    #[new]
    fn new(config: PyShareConfig) -> Self {
        Self { config }
    }
}
