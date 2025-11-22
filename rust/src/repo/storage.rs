//! Storage types and operations
//!
//! This module contains wrappers for storage-related types including StorageKey,
//! StorageTask, and StorageResult.

use pyo3::prelude::*;
use pyo3::types::PyBytes;
use std::collections::HashMap;

/// Wrapper for samod_core::StorageKey
///
/// Represents a hierarchical key for storage operations. Storage keys are
/// composed of string components that form a path-like structure.
#[pyclass(name = "StorageKey")]
#[derive(Clone)]
pub struct PyStorageKey(pub(crate) samod_core::StorageKey);

#[pymethods]
impl PyStorageKey {
    /// Create a StorageKey from a list of string parts
    #[staticmethod]
    fn from_parts(parts: Vec<String>) -> PyResult<Self> {
        samod_core::StorageKey::from_parts(parts)
            .map(PyStorageKey)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))
    }

    /// Get the parts of this StorageKey as a list of strings
    fn to_parts(&self) -> Vec<String> {
        self.0.clone().into_iter().collect()
    }

    /// String representation (parts joined with "/")
    fn __str__(&self) -> String {
        self.0.to_string()
    }

    /// String representation for debugging
    fn __repr__(&self) -> String {
        format!("StorageKey(\"{}\")", self.0)
    }

    /// Equality comparison
    fn __eq__(&self, other: &Self) -> bool {
        self.0 == other.0
    }

    /// Hash support
    fn __hash__(&self) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut hasher = DefaultHasher::new();
        self.0.hash(&mut hasher);
        hasher.finish()
    }
}

// ===== Storage Task Variants =====

/// Load task - retrieve a single value by key
#[pyclass(name = "StorageTaskLoad")]
#[derive(Clone)]
pub struct PyStorageTaskLoad {
    #[pyo3(get)]
    pub(crate) key: PyStorageKey,
}

#[pymethods]
impl PyStorageTaskLoad {
    #[new]
    fn new(key: PyStorageKey) -> Self {
        PyStorageTaskLoad { key }
    }

    fn __repr__(&self) -> String {
        format!("StorageTaskLoad(key={})", self.key.0)
    }
}

impl PyStorageTaskLoad {
    pub(crate) fn to_rust(&self) -> samod_core::io::StorageTask {
        samod_core::io::StorageTask::Load {
            key: self.key.0.clone(),
        }
    }
}

/// LoadRange task - retrieve all key-value pairs with keys starting with the prefix
#[pyclass(name = "StorageTaskLoadRange")]
#[derive(Clone)]
pub struct PyStorageTaskLoadRange {
    #[pyo3(get)]
    pub(crate) prefix: PyStorageKey,
}

#[pymethods]
impl PyStorageTaskLoadRange {
    #[new]
    fn new(prefix: PyStorageKey) -> Self {
        PyStorageTaskLoadRange { prefix }
    }

    fn __repr__(&self) -> String {
        format!("StorageTaskLoadRange(prefix={})", self.prefix.0)
    }
}

impl PyStorageTaskLoadRange {
    pub(crate) fn to_rust(&self) -> samod_core::io::StorageTask {
        samod_core::io::StorageTask::LoadRange {
            prefix: self.prefix.0.clone(),
        }
    }
}

/// Put task - store a key-value pair
#[pyclass(name = "StorageTaskPut")]
#[derive(Clone)]
pub struct PyStorageTaskPut {
    #[pyo3(get)]
    pub(crate) key: PyStorageKey,
    pub(crate) value: Vec<u8>,
}

#[pymethods]
impl PyStorageTaskPut {
    #[new]
    fn new(key: PyStorageKey, value: Vec<u8>) -> Self {
        PyStorageTaskPut { key, value }
    }

    #[getter]
    fn value<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.value)
    }

    fn __repr__(&self) -> String {
        format!("StorageTaskPut(key={}, value=<{} bytes>)", self.key.0, self.value.len())
    }
}

impl PyStorageTaskPut {
    pub(crate) fn to_rust(&self) -> samod_core::io::StorageTask {
        samod_core::io::StorageTask::Put {
            key: self.key.0.clone(),
            value: self.value.clone(),
        }
    }
}

/// Delete task - remove a key-value pair
#[pyclass(name = "StorageTaskDelete")]
#[derive(Clone)]
pub struct PyStorageTaskDelete {
    #[pyo3(get)]
    pub(crate) key: PyStorageKey,
}

#[pymethods]
impl PyStorageTaskDelete {
    #[new]
    fn new(key: PyStorageKey) -> Self {
        PyStorageTaskDelete { key }
    }

    fn __repr__(&self) -> String {
        format!("StorageTaskDelete(key={})", self.key.0)
    }
}

impl PyStorageTaskDelete {
    pub(crate) fn to_rust(&self) -> samod_core::io::StorageTask {
        samod_core::io::StorageTask::Delete {
            key: self.key.0.clone(),
        }
    }
}

/// Wrapper for samod_core::io::StorageResult
///
/// Represents the result of storage operations.
#[pyclass(name = "StorageResult")]
#[derive(Clone)]
pub struct PyStorageResult(pub(crate) samod_core::io::StorageResult);

#[pymethods]
impl PyStorageResult {
    /// Create a Load result with an optional value
    #[staticmethod]
    fn load(value: Option<Vec<u8>>) -> Self {
        PyStorageResult(samod_core::io::StorageResult::Load { value })
    }

    /// Create a LoadRange result with a list of (key, value) tuples
    #[staticmethod]
    fn load_range(values: Vec<(PyStorageKey, Vec<u8>)>) -> Self {
        let rust_values = values
            .into_iter()
            .map(|(k, v)| (k.0, v))
            .collect::<HashMap<_, _>>();
        PyStorageResult(samod_core::io::StorageResult::LoadRange {
            values: rust_values,
        })
    }

    /// Create a Put result (success)
    #[staticmethod]
    fn put() -> Self {
        PyStorageResult(samod_core::io::StorageResult::Put)
    }

    /// Create a Delete result (success)
    #[staticmethod]
    fn delete() -> Self {
        PyStorageResult(samod_core::io::StorageResult::Delete)
    }

    /// Check if this is a Load result
    fn is_load(&self) -> bool {
        matches!(self.0, samod_core::io::StorageResult::Load { .. })
    }

    /// Check if this is a LoadRange result
    fn is_load_range(&self) -> bool {
        matches!(self.0, samod_core::io::StorageResult::LoadRange { .. })
    }

    /// Check if this is a Put result
    fn is_put(&self) -> bool {
        matches!(self.0, samod_core::io::StorageResult::Put)
    }

    /// Check if this is a Delete result
    fn is_delete(&self) -> bool {
        matches!(self.0, samod_core::io::StorageResult::Delete)
    }

    /// Get the loaded value (only for Load results, returns None for others or if key wasn't found)
    fn load_value<'py>(&self, py: Python<'py>) -> Option<Bound<'py, PyBytes>> {
        match &self.0 {
            samod_core::io::StorageResult::Load { value } => {
                value.as_ref().map(|v| PyBytes::new(py, v))
            }
            _ => None,
        }
    }

    /// Get the loaded key-value pairs (only for LoadRange results, returns None for others)
    fn load_range_values<'py>(&self, py: Python<'py>) -> Option<Bound<'py, pyo3::types::PyDict>> {
        match &self.0 {
            samod_core::io::StorageResult::LoadRange { values } => {
                let dict = pyo3::types::PyDict::new(py);
                for (key, value) in values {
                    let py_key = PyStorageKey(key.clone());
                    let py_value = PyBytes::new(py, value);
                    dict.set_item(py_key, py_value).ok()?;
                }
                Some(dict)
            }
            _ => None,
        }
    }

    /// String representation for debugging
    fn __repr__(&self) -> String {
        match &self.0 {
            samod_core::io::StorageResult::Load { value } => {
                if let Some(v) = value {
                    format!("StorageResult.Load(value=<{} bytes>)", v.len())
                } else {
                    "StorageResult.Load(value=None)".to_string()
                }
            }
            samod_core::io::StorageResult::LoadRange { values } => {
                format!("StorageResult.LoadRange(<{} entries>)", values.len())
            }
            samod_core::io::StorageResult::Put => "StorageResult.Put()".to_string(),
            samod_core::io::StorageResult::Delete => "StorageResult.Delete()".to_string(),
        }
    }
}
