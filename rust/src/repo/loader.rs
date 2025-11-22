//! Loader types for repository initialization
//!
//! This module contains types for the SamodLoader and its state machine,
//! which handles loading repository state from storage.

use pyo3::prelude::*;

use super::types::PyPeerId;
use super::io::{PyIoTask, PyIoResult, PyStorageResultPayload};
use super::hub::PyHub;

/// LoaderState variant indicating IO tasks need to be executed
///
/// This is a consume-once type - the tasks can only be retrieved once.
#[pyclass(name = "LoaderStateNeedIo")]
pub struct PyLoaderStateNeedIo {
    tasks: std::sync::Mutex<Option<Vec<Py<PyIoTask>>>>,
}

#[pymethods]
impl PyLoaderStateNeedIo {
    /// Get the IO tasks (consume-once)
    ///
    /// This method consumes the tasks - it can only be called once.
    /// Subsequent calls will raise an error.
    #[getter]
    fn tasks(&self, py: Python<'_>) -> PyResult<Py<pyo3::types::PyList>> {
        let mut guard = self.tasks.lock().unwrap();
        if let Some(tasks) = guard.take() {
            let py_list = pyo3::types::PyList::empty(py);
            for task in tasks {
                py_list.append(task)?;
            }
            Ok(py_list.into())
        } else {
            Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "tasks have already been consumed"
            ))
        }
    }

    fn __repr__(&self) -> String {
        let guard = self.tasks.lock().unwrap();
        if let Some(tasks) = guard.as_ref() {
            format!("LoaderStateNeedIo(tasks={})", tasks.len())
        } else {
            "LoaderStateNeedIo(<consumed>)".to_string()
        }
    }
}

/// LoaderState variant indicating loading is complete and Hub is ready
///
/// This is a consume-once type - the hub can only be retrieved once.
#[pyclass(name = "LoaderStateLoaded")]
pub struct PyLoaderStateLoaded {
    hub: std::sync::Mutex<Option<Py<PyHub>>>,
}

#[pymethods]
impl PyLoaderStateLoaded {
    /// Get the Hub (consume-once)
    ///
    /// This method consumes the hub - it can only be called once.
    /// Subsequent calls will raise an error.
    #[getter]
    fn hub(&self) -> PyResult<Py<PyHub>> {
        let mut guard = self.hub.lock().unwrap();
        guard.take()
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "hub has already been consumed"
            ))
    }

    fn __repr__(&self) -> String {
        let guard = self.hub.lock().unwrap();
        if guard.is_some() {
            "LoaderStateLoaded(hub=...)".to_string()
        } else {
            "LoaderStateLoaded(<consumed>)".to_string()
        }
    }
}

/// Wrapper for samod_core::SamodLoader
///
/// The loader is responsible for initializing the repository by loading
/// initial state from storage. It uses the sans-IO pattern: it produces
/// IO tasks that must be executed by the caller, and then the results
/// are fed back to progress the loading process.
///
/// Usage:
/// 1. Create a loader with SamodLoader(peer_id)
/// 2. Call step() to get the current state
/// 3. If NeedIo, execute the IO tasks and call provide_io_result() for each
/// 4. Call step() again
/// 5. Repeat until Loaded
#[pyclass(name = "SamodLoader")]
pub struct PySamodLoader {
    inner: std::sync::Arc<std::sync::Mutex<samod_core::SamodLoader>>,
}

#[pymethods]
impl PySamodLoader {
    /// Create a new SamodLoader with the given peer ID
    #[new]
    fn new(peer_id: PyPeerId) -> PyResult<Self> {
        let loader = samod_core::SamodLoader::new(peer_id.0);
        Ok(PySamodLoader {
            inner: std::sync::Arc::new(std::sync::Mutex::new(loader)),
        })
    }

    /// Step the loader state machine forward
    ///
    /// Args:
    ///     now: Current Unix timestamp in seconds (float)
    ///
    /// Returns:
    ///     Either LoaderStateNeedIo (with tasks to execute) or LoaderStateLoaded (with the Hub)
    fn step(&self, now: f64) -> PyResult<PyObject> {
        let mut guard = self.inner.lock().unwrap();
        let mut rng = rand::rng();
        let timestamp = samod_core::UnixTimestamp::from_millis((now * 1000.0) as u128);

        Python::with_gil(|py| {
            match guard.step(&mut rng, timestamp) {
                samod_core::LoaderState::NeedIo(tasks) => {
                    let py_tasks: Vec<Py<PyIoTask>> = tasks.into_iter()
                        .map(|task| {
                            let py_task: PyIoTask = task.into();
                            Py::new(py, py_task).unwrap()
                        })
                        .collect();
                    let state = PyLoaderStateNeedIo {
                        tasks: std::sync::Mutex::new(Some(py_tasks)),
                    };
                    Ok(Py::new(py, state)?.into())
                }
                samod_core::LoaderState::Loaded(hub) => {
                    let py_hub = Py::new(py, PyHub {
                        inner: std::sync::Arc::new(std::sync::Mutex::new(*hub)),
                    })?;
                    let state = PyLoaderStateLoaded {
                        hub: std::sync::Mutex::new(Some(py_hub)),
                    };
                    Ok(Py::new(py, state)?.into())
                }
            }
        })
    }

    /// Provide the result of an IO operation
    ///
    /// Call this method for each IO task that was returned by step().
    ///
    /// Args:
    ///     io_result: The result of executing an IoTask
    fn provide_io_result(&self, io_result: &PyIoResult) -> PyResult<()> {
        let mut guard = self.inner.lock().unwrap();

        // Extract the payload from PyIoResult
        Python::with_gil(|py| {
            let payload = io_result.payload.bind(py);

            // Check if it's a StorageResultPayload
            if let Ok(storage_payload) = payload.downcast::<PyStorageResultPayload>() {
                let storage_result = storage_payload.borrow().take_result()?;
                let rust_result = samod_core::io::IoResult {
                    task_id: io_result.task_id.0,
                    payload: storage_result.0,
                };
                guard.provide_io_result(rust_result);
                Ok(())
            } else {
                Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "Expected StorageResultPayload for loader"
                ))
            }
        })
    }

    fn __repr__(&self) -> String {
        "SamodLoader(...)".to_string()
    }
}
