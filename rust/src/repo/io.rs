//! IO infrastructure for sans-IO pattern
//!
//! This module contains types for the sans-IO task/result pattern used throughout
//! the repo implementation, including IoTaskId, IoTask, IoResult, action types,
//! payload types, and IO result variant types.

use pyo3::prelude::*;
use pyo3::types::PyBytes;
use samod_core::actors::document::io::{DocumentIoResult, DocumentIoTask};
use samod_core::actors::hub::io::{HubIoAction, HubIoResult};

use super::storage::{
    PyStorageResult, PyStorageTaskDelete, PyStorageTaskLoad, PyStorageTaskLoadRange,
    PyStorageTaskPut,
};
use super::types::{PyConnectionId, PyPeerId};

/// Wrapper for samod_core::io::IoTaskId
///
/// Represents a unique identifier for an IO task.
#[pyclass(name = "IoTaskId")]
#[derive(Clone, Copy)]
pub struct PyIoTaskId(pub(crate) samod_core::io::IoTaskId);

#[pymethods]
impl PyIoTaskId {
    /// Create an IoTaskId from a u32
    #[staticmethod]
    fn from_u32(id: u32) -> Self {
        PyIoTaskId(samod_core::io::IoTaskId::from(id))
    }

    /// Get the u32 representation of this IoTaskId
    fn to_u32(&self) -> u32 {
        self.0.into()
    }

    /// String representation for debugging
    fn __repr__(&self) -> String {
        format!("IoTaskId({})", u32::from(self.0))
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

// ===== Action Classes =====

/// Action for storage operations
///
/// This is a wrapper that holds one of the StorageTask subclasses.
/// Use isinstance() to determine which task type it contains.
#[pyclass(name = "StorageTaskAction")]
pub struct PyStorageTaskAction {
    #[pyo3(get)]
    pub task: PyObject, // One of: StorageTaskLoad, StorageTaskLoadRange, StorageTaskPut, StorageTaskDelete
}

impl Clone for PyStorageTaskAction {
    fn clone(&self) -> Self {
        Python::with_gil(|py| PyStorageTaskAction {
            task: self.task.clone_ref(py),
        })
    }
}

#[pymethods]
impl PyStorageTaskAction {
    fn __repr__(&self) -> String {
        Python::with_gil(|py| {
            let task_repr = self.task.bind(py).repr().unwrap().to_string();
            format!("StorageTaskAction({})", task_repr)
        })
    }
}

impl PyStorageTaskAction {
    pub(crate) fn to_rust(&self) -> samod_core::io::StorageTask {
        Python::with_gil(|py| {
            let task = self.task.bind(py);

            if let Ok(load) = task.extract::<PyStorageTaskLoad>() {
                load.to_rust()
            } else if let Ok(load_range) = task.extract::<PyStorageTaskLoadRange>() {
                load_range.to_rust()
            } else if let Ok(put) = task.extract::<PyStorageTaskPut>() {
                put.to_rust()
            } else if let Ok(delete) = task.extract::<PyStorageTaskDelete>() {
                delete.to_rust()
            } else {
                panic!("Invalid StorageTask variant")
            }
        })
    }
}

/// Action for sending messages over a connection
#[pyclass(name = "SendAction")]
#[derive(Clone)]
pub struct PySendAction {
    #[pyo3(get)]
    pub connection_id: PyConnectionId,
    message_bytes: Vec<u8>,
}

#[pymethods]
impl PySendAction {
    #[new]
    fn new(connection_id: PyConnectionId, message: Vec<u8>) -> Self {
        PySendAction {
            connection_id,
            message_bytes: message,
        }
    }

    #[getter]
    fn message<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.message_bytes)
    }

    fn __repr__(&self) -> String {
        format!(
            "SendAction(connection_id={}, message=<{} bytes>)",
            u32::from(self.connection_id.0),
            self.message_bytes.len()
        )
    }
}

/// Action for disconnecting a connection
#[pyclass(name = "DisconnectAction")]
#[derive(Clone)]
pub struct PyDisconnectAction {
    #[pyo3(get)]
    pub connection_id: PyConnectionId,
}

#[pymethods]
impl PyDisconnectAction {
    #[new]
    fn new(connection_id: PyConnectionId) -> Self {
        PyDisconnectAction { connection_id }
    }

    fn __repr__(&self) -> String {
        format!(
            "DisconnectAction(connection_id={})",
            u32::from(self.connection_id.0)
        )
    }
}

/// Action for checking document announce policy
#[pyclass(name = "CheckAnnouncePolicyAction")]
#[derive(Clone)]
pub struct PyCheckAnnouncePolicyAction {
    #[pyo3(get)]
    pub peer_id: PyPeerId,
}

#[pymethods]
impl PyCheckAnnouncePolicyAction {
    #[new]
    fn new(peer_id: PyPeerId) -> Self {
        PyCheckAnnouncePolicyAction { peer_id }
    }

    fn __repr__(&self) -> String {
        format!("CheckAnnouncePolicyAction(peer_id={})", self.peer_id.0)
    }
}

// ===== Hub IO Result =====

/// Send result for Hub IO operations
///
/// This variant has no associated data.
#[pyclass(name = "HubIoResultSend")]
#[derive(Clone)]
pub struct PyHubIoResultSend;

#[pymethods]
impl PyHubIoResultSend {
    #[new]
    fn new() -> Self {
        PyHubIoResultSend
    }

    fn __repr__(&self) -> String {
        "HubIoResultSend()".to_string()
    }
}

impl PyHubIoResultSend {
    /// Convert to the underlying Rust enum variant
    pub(crate) fn to_rust(&self) -> samod_core::actors::hub::io::HubIoResult {
        samod_core::actors::hub::io::HubIoResult::Send
    }
}

/// Disconnect result for Hub IO operations
///
/// This variant has no associated data.
#[pyclass(name = "HubIoResultDisconnect")]
#[derive(Clone)]
pub struct PyHubIoResultDisconnect;

#[pymethods]
impl PyHubIoResultDisconnect {
    #[new]
    fn new() -> Self {
        PyHubIoResultDisconnect
    }

    fn __repr__(&self) -> String {
        "HubIoResultDisconnect()".to_string()
    }
}

impl PyHubIoResultDisconnect {
    /// Convert to the underlying Rust enum variant
    pub(crate) fn to_rust(&self) -> samod_core::actors::hub::io::HubIoResult {
        samod_core::actors::hub::io::HubIoResult::Disconnect
    }
}

// ===== Document IO Result =====

/// Storage result for Document IO operations
#[pyclass(name = "DocumentIoResultStorage")]
#[derive(Clone)]
pub struct PyDocumentIoResultStorage {
    #[pyo3(get)]
    storage_result: PyStorageResult,
}

#[pymethods]
impl PyDocumentIoResultStorage {
    #[new]
    fn new(storage_result: &PyStorageResult) -> Self {
        PyDocumentIoResultStorage {
            storage_result: storage_result.clone(),
        }
    }

    fn __repr__(&self) -> String {
        "DocumentIoResultStorage(...)".to_string()
    }
}

impl PyDocumentIoResultStorage {
    /// Convert to the underlying Rust enum variant
    pub(crate) fn to_rust(&self) -> samod_core::actors::document::io::DocumentIoResult {
        samod_core::actors::document::io::DocumentIoResult::Storage(self.storage_result.0.clone())
    }
}

/// CheckAnnouncePolicy result for Document IO operations
#[pyclass(name = "DocumentIoResultCheckAnnouncePolicy")]
#[derive(Clone)]
pub struct PyDocumentIoResultCheckAnnouncePolicy {
    #[pyo3(get)]
    should_announce: bool,
}

#[pymethods]
impl PyDocumentIoResultCheckAnnouncePolicy {
    #[new]
    fn new(should_announce: bool) -> Self {
        PyDocumentIoResultCheckAnnouncePolicy { should_announce }
    }

    fn __repr__(&self) -> String {
        format!(
            "DocumentIoResultCheckAnnouncePolicy({})",
            self.should_announce
        )
    }
}

impl PyDocumentIoResultCheckAnnouncePolicy {
    /// Convert to the underlying Rust enum variant
    pub(crate) fn to_rust(&self) -> samod_core::actors::document::io::DocumentIoResult {
        samod_core::actors::document::io::DocumentIoResult::CheckAnnouncePolicy(
            self.should_announce,
        )
    }
}

// ===== IoTask =====

/// Wrapper for samod_core::io::IoTask<Action>
///
/// Represents an IO task with an ID and an action. The action can be one of:
/// - StorageTaskAction
/// - SendAction
/// - DisconnectAction
/// - CheckAnnouncePolicyAction
#[pyclass(name = "IoTask")]
pub struct PyIoTask {
    #[pyo3(get)]
    task_id: PyIoTaskId,
    #[pyo3(get)]
    action: PyObject,
}

#[pymethods]
impl PyIoTask {
    fn __repr__(&self) -> String {
        Python::with_gil(|py| {
            let action_repr = self.action.bind(py).repr().unwrap().to_string();
            format!(
                "IoTask(task_id={}, action={})",
                u32::from(self.task_id.0),
                action_repr
            )
        })
    }
}

// Internal conversion from Rust IoTask<StorageTask> to Python
impl From<samod_core::io::IoTask<samod_core::io::StorageTask>> for PyIoTask {
    fn from(task: samod_core::io::IoTask<samod_core::io::StorageTask>) -> Self {
        Python::with_gil(|py| {
            // Convert the Rust StorageTask enum to the appropriate Python subclass
            let task_obj: PyObject = match &task.action {
                samod_core::io::StorageTask::Load { key } => Py::new(
                    py,
                    PyStorageTaskLoad {
                        key: super::storage::PyStorageKey(key.clone()),
                    },
                )
                .unwrap()
                .into(),
                samod_core::io::StorageTask::LoadRange { prefix } => Py::new(
                    py,
                    PyStorageTaskLoadRange {
                        prefix: super::storage::PyStorageKey(prefix.clone()),
                    },
                )
                .unwrap()
                .into(),
                samod_core::io::StorageTask::Put { key, value } => Py::new(
                    py,
                    PyStorageTaskPut {
                        key: super::storage::PyStorageKey(key.clone()),
                        value: value.clone(),
                    },
                )
                .unwrap()
                .into(),
                samod_core::io::StorageTask::Delete { key } => Py::new(
                    py,
                    PyStorageTaskDelete {
                        key: super::storage::PyStorageKey(key.clone()),
                    },
                )
                .unwrap()
                .into(),
            };

            let action = PyStorageTaskAction { task: task_obj };
            PyIoTask {
                task_id: PyIoTaskId(task.task_id),
                action: Py::new(py, action).unwrap().into(),
            }
        })
    }
}

// Internal conversion from Rust IoTask<HubIoAction> to Python
impl From<samod_core::io::IoTask<HubIoAction>> for PyIoTask {
    fn from(task: samod_core::io::IoTask<HubIoAction>) -> Self {
        Python::with_gil(|py| {
            let action: PyObject = match task.action {
                HubIoAction::Send { connection_id, msg } => Py::new(
                    py,
                    PySendAction {
                        connection_id: PyConnectionId(connection_id),
                        message_bytes: msg,
                    },
                )
                .unwrap()
                .into(),
                HubIoAction::Disconnect { connection_id } => Py::new(
                    py,
                    PyDisconnectAction {
                        connection_id: PyConnectionId(connection_id),
                    },
                )
                .unwrap()
                .into(),
            };
            PyIoTask {
                task_id: PyIoTaskId(task.task_id),
                action,
            }
        })
    }
}

// Internal conversion from Rust IoTask<DocumentIoTask> to Python
impl From<samod_core::io::IoTask<DocumentIoTask>> for PyIoTask {
    fn from(task: samod_core::io::IoTask<DocumentIoTask>) -> Self {
        Python::with_gil(|py| {
            let action: PyObject = match task.action {
                DocumentIoTask::Storage(storage_task) => {
                    // Convert the Rust StorageTask enum to the appropriate Python subclass
                    let task_obj: PyObject = match &storage_task {
                        samod_core::io::StorageTask::Load { key } => Py::new(
                            py,
                            PyStorageTaskLoad {
                                key: super::storage::PyStorageKey(key.clone()),
                            },
                        )
                        .unwrap()
                        .into(),
                        samod_core::io::StorageTask::LoadRange { prefix } => Py::new(
                            py,
                            PyStorageTaskLoadRange {
                                prefix: super::storage::PyStorageKey(prefix.clone()),
                            },
                        )
                        .unwrap()
                        .into(),
                        samod_core::io::StorageTask::Put { key, value } => Py::new(
                            py,
                            PyStorageTaskPut {
                                key: super::storage::PyStorageKey(key.clone()),
                                value: value.clone(),
                            },
                        )
                        .unwrap()
                        .into(),
                        samod_core::io::StorageTask::Delete { key } => Py::new(
                            py,
                            PyStorageTaskDelete {
                                key: super::storage::PyStorageKey(key.clone()),
                            },
                        )
                        .unwrap()
                        .into(),
                    };

                    Py::new(py, PyStorageTaskAction { task: task_obj })
                        .unwrap()
                        .into()
                }
                DocumentIoTask::CheckAnnouncePolicy { peer_id } => Py::new(
                    py,
                    PyCheckAnnouncePolicyAction {
                        peer_id: PyPeerId(peer_id),
                    },
                )
                .unwrap()
                .into(),
            };
            PyIoTask {
                task_id: PyIoTaskId(task.task_id),
                action,
            }
        })
    }
}

// Helper to convert a reference to DocumentIoTask to PyIoTask
pub(crate) fn document_io_task_ref_to_py(
    py: Python<'_>,
    task_id: samod_core::io::IoTaskId,
    task: &DocumentIoTask,
) -> PyIoTask {
    let action: PyObject = match task {
        DocumentIoTask::Storage(storage_task) => {
            let task_obj: PyObject = match storage_task {
                samod_core::io::StorageTask::Load { key } => Py::new(
                    py,
                    PyStorageTaskLoad {
                        key: super::storage::PyStorageKey(key.clone()),
                    },
                )
                .unwrap()
                .into(),
                samod_core::io::StorageTask::LoadRange { prefix } => Py::new(
                    py,
                    PyStorageTaskLoadRange {
                        prefix: super::storage::PyStorageKey(prefix.clone()),
                    },
                )
                .unwrap()
                .into(),
                samod_core::io::StorageTask::Put { key, value } => Py::new(
                    py,
                    PyStorageTaskPut {
                        key: super::storage::PyStorageKey(key.clone()),
                        value: value.clone(),
                    },
                )
                .unwrap()
                .into(),
                samod_core::io::StorageTask::Delete { key } => Py::new(
                    py,
                    PyStorageTaskDelete {
                        key: super::storage::PyStorageKey(key.clone()),
                    },
                )
                .unwrap()
                .into(),
            };
            Py::new(py, PyStorageTaskAction { task: task_obj })
                .unwrap()
                .into()
        }
        DocumentIoTask::CheckAnnouncePolicy { peer_id } => Py::new(
            py,
            PyCheckAnnouncePolicyAction {
                peer_id: PyPeerId(peer_id.clone()),
            },
        )
        .unwrap()
        .into(),
    };
    PyIoTask {
        task_id: PyIoTaskId(task_id),
        action,
    }
}

// ===== Payload Classes =====

/// Payload for storage operation results
#[pyclass(name = "StorageResultPayload")]
pub struct PyStorageResultPayload {
    pub(crate) result: std::sync::Mutex<Option<samod_core::io::StorageResult>>,
}

#[pymethods]
impl PyStorageResultPayload {
    fn __repr__(&self) -> String {
        "StorageResultPayload(<StorageResult>)".to_string()
    }
}

impl PyStorageResultPayload {
    /// Take the storage result (consumes it, can only be called once)
    pub(crate) fn take_result(&self) -> PyResult<PyStorageResult> {
        let mut guard = self.result.lock().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Lock error: {}", e))
        })?;
        if let Some(result) = guard.take() {
            Ok(PyStorageResult(result))
        } else {
            Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "StorageResultPayload has already been consumed",
            ))
        }
    }
}

/// Payload for check announce policy results
#[pyclass(name = "CheckAnnouncePolicyResultPayload")]
#[derive(Clone)]
pub struct PyCheckAnnouncePolicyResultPayload {
    #[pyo3(get)]
    pub should_announce: bool,
}

#[pymethods]
impl PyCheckAnnouncePolicyResultPayload {
    #[new]
    fn new(should_announce: bool) -> Self {
        PyCheckAnnouncePolicyResultPayload { should_announce }
    }

    fn __repr__(&self) -> String {
        format!(
            "CheckAnnouncePolicyResultPayload(should_announce={})",
            self.should_announce
        )
    }
}

/// Payload for send operation results (success indicator)
#[pyclass(name = "SendResultPayload")]
#[derive(Clone)]
pub struct PySendResultPayload;

#[pymethods]
impl PySendResultPayload {
    #[new]
    fn new() -> Self {
        PySendResultPayload
    }

    fn __repr__(&self) -> String {
        "SendResultPayload()".to_string()
    }
}

/// Payload for disconnect operation results (success indicator)
#[pyclass(name = "DisconnectResultPayload")]
#[derive(Clone)]
pub struct PyDisconnectResultPayload;

#[pymethods]
impl PyDisconnectResultPayload {
    #[new]
    fn new() -> Self {
        PyDisconnectResultPayload
    }

    fn __repr__(&self) -> String {
        "DisconnectResultPayload()".to_string()
    }
}

// ===== IoResult =====

/// Wrapper for samod_core::io::IoResult<Payload>
///
/// Represents the result of an IO task with an ID and a payload. The payload can be one of:
/// - StorageResultPayload
/// - CheckAnnouncePolicyResultPayload
/// - SendResultPayload
/// - DisconnectResultPayload
#[pyclass(name = "IoResult")]
pub struct PyIoResult {
    #[pyo3(get)]
    pub(crate) task_id: PyIoTaskId,
    #[pyo3(get)]
    pub(crate) payload: PyObject,
}

impl Clone for PyIoResult {
    fn clone(&self) -> Self {
        Python::with_gil(|py| PyIoResult {
            task_id: self.task_id,
            payload: self.payload.clone_ref(py),
        })
    }
}

#[pymethods]
impl PyIoResult {
    /// Create a new IoResult with a task ID and StorageResult payload
    ///
    /// This is used when bridging between Python storage adapters and Rust actors.
    /// After executing a storage task via Python's Storage protocol, call this method
    /// to create an IoResult that can be fed back to the loader/hub/document actors.
    #[staticmethod]
    fn from_storage_result(
        task_id: PyIoTaskId,
        storage_result: &PyStorageResult,
    ) -> PyResult<Self> {
        Python::with_gil(|py| {
            let payload = PyStorageResultPayload {
                result: std::sync::Mutex::new(Some(storage_result.0.clone())),
            };
            Ok(PyIoResult {
                task_id,
                payload: Py::new(py, payload)?.into(),
            })
        })
    }

    /// Create a new IoResult with a task ID and HubIoResult payload
    ///
    /// This is used when providing Hub IO results (Send or Disconnect).
    ///
    /// Args:
    ///     task_id: The IO task ID
    ///     hub_result: The HubIoResultSend or HubIoResultDisconnect
    #[staticmethod]
    fn from_hub_result<'py>(task_id: PyIoTaskId, hub_result: &Bound<'py, PyAny>) -> PyResult<Self> {
        Python::with_gil(|py| {
            let payload: PyObject = if hub_result.is_instance_of::<PyHubIoResultSend>() {
                Py::new(py, PySendResultPayload)?.into()
            } else if hub_result.is_instance_of::<PyHubIoResultDisconnect>() {
                Py::new(py, PyDisconnectResultPayload)?.into()
            } else {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "Expected HubIoResultSend or HubIoResultDisconnect",
                ));
            };
            Ok(PyIoResult { task_id, payload })
        })
    }

    /// Create a new IoResult with a task ID and DocumentIoResult payload
    ///
    /// This is used when providing Document IO results.
    ///
    /// Args:
    ///     task_id: The IO task ID
    ///     doc_result: The DocumentIoResultStorage or DocumentIoResultCheckAnnouncePolicy
    #[staticmethod]
    fn from_document_result<'py>(
        task_id: PyIoTaskId,
        doc_result: &Bound<'py, PyAny>,
    ) -> PyResult<Self> {
        Python::with_gil(|py| {
            let payload: PyObject =
                if let Ok(storage) = doc_result.extract::<PyDocumentIoResultStorage>() {
                    Py::new(
                        py,
                        PyStorageResultPayload {
                            result: std::sync::Mutex::new(Some(storage.storage_result.0.clone())),
                        },
                    )?
                    .into()
                } else if let Ok(announce) =
                    doc_result.extract::<PyDocumentIoResultCheckAnnouncePolicy>()
                {
                    Py::new(
                        py,
                        PyCheckAnnouncePolicyResultPayload {
                            should_announce: announce.should_announce,
                        },
                    )?
                    .into()
                } else {
                    return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "Expected DocumentIoResultStorage or DocumentIoResultCheckAnnouncePolicy",
                    ));
                };
            Ok(PyIoResult { task_id, payload })
        })
    }

    /// Create an IoResult for a successful send operation
    #[staticmethod]
    fn from_send_result(task_id: PyIoTaskId) -> PyResult<Self> {
        Python::with_gil(|py| {
            Ok(PyIoResult {
                task_id,
                payload: Py::new(py, PySendResultPayload)?.into(),
            })
        })
    }

    /// Create an IoResult for a successful disconnect operation
    #[staticmethod]
    fn from_disconnect_result(task_id: PyIoTaskId) -> PyResult<Self> {
        Python::with_gil(|py| {
            Ok(PyIoResult {
                task_id,
                payload: Py::new(py, PyDisconnectResultPayload)?.into(),
            })
        })
    }

    /// Create an IoResult for a check announce policy operation
    #[staticmethod]
    fn from_check_announce_policy_result(
        task_id: PyIoTaskId,
        should_announce: bool,
    ) -> PyResult<Self> {
        Python::with_gil(|py| {
            Ok(PyIoResult {
                task_id,
                payload: Py::new(py, PyCheckAnnouncePolicyResultPayload { should_announce })?
                    .into(),
            })
        })
    }

    fn __repr__(&self) -> String {
        Python::with_gil(|py| {
            let payload_repr = self.payload.bind(py).repr().unwrap().to_string();
            format!(
                "IoResult(task_id={}, payload={})",
                u32::from(self.task_id.0),
                payload_repr
            )
        })
    }
}

// Internal conversion methods

impl PyIoResult {
    /// Convert PyIoResult back to Rust IoResult<DocumentIoResult>
    ///
    /// This is used when feeding IO results back to document actors.
    pub(crate) fn to_document_io_result(
        &self,
    ) -> PyResult<samod_core::io::IoResult<DocumentIoResult>> {
        Python::with_gil(|py| {
            let payload_obj = self.payload.bind(py);

            // Try to extract the payload as different types
            let payload =
                if let Ok(storage_payload) = payload_obj.extract::<Py<PyStorageResultPayload>>() {
                    // Extract the storage result from the payload
                    let storage_result = storage_payload.borrow(py).take_result()?;
                    DocumentIoResult::Storage(storage_result.0)
                } else if let Ok(announce_payload) =
                    payload_obj.extract::<PyCheckAnnouncePolicyResultPayload>()
                {
                    DocumentIoResult::CheckAnnouncePolicy(announce_payload.should_announce)
                } else {
                    return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "Expected StorageResultPayload or CheckAnnouncePolicyResultPayload",
                    ));
                };

            Ok(samod_core::io::IoResult {
                task_id: self.task_id.0,
                payload,
            })
        })
    }

    /// Convert PyIoResult back to Rust IoResult<HubIoResult>
    ///
    /// This is used when feeding IO results back to the Hub.
    pub(crate) fn to_hub_io_result(&self) -> PyResult<samod_core::io::IoResult<HubIoResult>> {
        Python::with_gil(|py| {
            let payload_obj = self.payload.bind(py);

            // Try to extract the payload as different types
            let payload =
                if let Ok(_send_payload) = payload_obj.extract::<Py<PySendResultPayload>>() {
                    HubIoResult::Send
                } else if let Ok(_disconnect_payload) =
                    payload_obj.extract::<Py<PyDisconnectResultPayload>>()
                {
                    HubIoResult::Disconnect
                } else {
                    return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "Expected SendResultPayload or DisconnectResultPayload",
                    ));
                };

            Ok(samod_core::io::IoResult {
                task_id: self.task_id.0,
                payload,
            })
        })
    }

    /// Convert PyIoResult to core IoResult for HubEvent.io_complete()
    ///
    /// This auto-detects the payload type and converts appropriately.
    pub(crate) fn to_core(&self) -> samod_core::io::IoResult<HubIoResult> {
        // Try to convert as Hub IO result first
        self.to_hub_io_result()
            .expect("Failed to convert PyIoResult to core IoResult")
    }
}

// Internal conversion from Rust IoResult<StorageResult> to Python
impl From<samod_core::io::IoResult<samod_core::io::StorageResult>> for PyIoResult {
    fn from(result: samod_core::io::IoResult<samod_core::io::StorageResult>) -> Self {
        Python::with_gil(|py| {
            let payload = PyStorageResultPayload {
                result: std::sync::Mutex::new(Some(result.payload)),
            };
            PyIoResult {
                task_id: PyIoTaskId(result.task_id),
                payload: Py::new(py, payload).unwrap().into(),
            }
        })
    }
}

// Internal conversion from Rust IoResult<HubIoResult> to Python
impl From<samod_core::io::IoResult<HubIoResult>> for PyIoResult {
    fn from(result: samod_core::io::IoResult<HubIoResult>) -> Self {
        Python::with_gil(|py| {
            let payload: PyObject = match result.payload {
                HubIoResult::Send => Py::new(py, PySendResultPayload).unwrap().into(),
                HubIoResult::Disconnect => Py::new(py, PyDisconnectResultPayload).unwrap().into(),
            };
            PyIoResult {
                task_id: PyIoTaskId(result.task_id),
                payload,
            }
        })
    }
}

// Internal conversion from Rust IoResult<DocumentIoResult> to Python
impl From<samod_core::io::IoResult<DocumentIoResult>> for PyIoResult {
    fn from(result: samod_core::io::IoResult<DocumentIoResult>) -> Self {
        Python::with_gil(|py| {
            let payload: PyObject = match result.payload {
                DocumentIoResult::Storage(storage_result) => Py::new(
                    py,
                    PyStorageResultPayload {
                        result: std::sync::Mutex::new(Some(storage_result)),
                    },
                )
                .unwrap()
                .into(),
                DocumentIoResult::CheckAnnouncePolicy(should_announce) => {
                    Py::new(py, PyCheckAnnouncePolicyResultPayload { should_announce })
                        .unwrap()
                        .into()
                }
            };
            PyIoResult {
                task_id: PyIoTaskId(result.task_id),
                payload,
            }
        })
    }
}
