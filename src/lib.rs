use pyo3::prelude::*;
use pyo3::wrap_pyfunction;
use pyo3::types::{PyString, PyLong};
use automerge::{Backend, Frontend};
use maplit::hashmap;


#[pyclass(unsendable)]
struct PyBackend {
    backend: Backend,
}

#[pymethods]
impl PyBackend {
    pub fn get_name(&self) -> PyResult<String> {
        Ok("hello world".to_string())
    }
}

#[pyfunction]
fn new_backend() -> PyResult<PyBackend> {
    Ok(PyBackend{backend: Backend::init()})
}

#[pyclass(unsendable)]
struct PyFrontend {
    frontend: Frontend,
}

#[pymethods]
impl PyFrontend {
    fn value_at_path(&self, path: Vec<&PyAny>) -> PyResult<Option<AutomergeType>> {
        let val = self.frontend.value_at_path(&values_to_path(path)?).map(|v| match v {
            automerge::Value::Map(..) => AutomergeType::Map,
            automerge::Value::Sequence(..) => AutomergeType::List,
            automerge::Value::Text(..) => AutomergeType::Text,
            automerge::Value::Primitive(pval) => {
                AutomergeType::Value(PyAutoScalar(pval))
            }
        });
        Ok(val)
    }

    fn set_at_path(&self, path: Vec<&PyAny>, value: &Py<String>) -> PyResult<()> {
        self.frontend.
    }
}

fn values_to_path(values: Vec<&PyAny>) -> PyResult<automerge::Path> {
    let mut result = automerge::Path::root();
    for elem in values {
        if let Ok(name) = elem.downcast::<PyString>() {
            result = result.key(name.to_string());
        } else if let Ok(index) = elem.downcast::<PyLong>() {
            let index_u32: u32 = index.extract()?;
            result = result.index(index_u32);
        } else {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>("Path should be a list of strings and integers"))
        }
    }
    Ok(result)
}

#[pyfunction]
fn new_frontend() -> PyResult<PyFrontend> {
    Ok(PyFrontend{frontend: Frontend::new()})
}

pub struct PyAutoScalar(automerge::ScalarValue);

pub enum AutomergeType {
    Map,
    List,
    Text,
    Table,
    Value(PyAutoScalar),
}

impl IntoPy<PyObject> for AutomergeType {
    fn into_py(self, py: Python) -> PyObject {
        match self {
            AutomergeType::Map => "map".into_py(py),
            AutomergeType::List => "list".into_py(py),
            AutomergeType::Text => "text".into_py(py),
            AutomergeType::Table => "table".into_py(py),
            AutomergeType::Value(v) => {
                let val = v.into_py(py);
                let result = hashmap!{"value" => val};
                result.into_py(py)
            }
        }
    }
}

impl IntoPy<PyObject> for PyAutoScalar {
    fn into_py(self, py: Python) -> PyObject {
        match self.0 {
            automerge::ScalarValue::Str(s) => s.to_object(py),
            automerge::ScalarValue::Int(i) => i.to_object(py),
            automerge::ScalarValue::Uint(u) => u.to_object(py),
            automerge::ScalarValue::F64(f) => f.to_object(py),
            automerge::ScalarValue::F32(f) => f.to_object(py),
            automerge::ScalarValue::Counter(c) => c.to_object(py),
            automerge::ScalarValue::Timestamp(i) => i.to_object(py),
            automerge::ScalarValue::Cursor(..) => unimplemented!(),
            automerge::ScalarValue::Boolean(b) => b.to_object(py),
            automerge::ScalarValue::Null => Option::<PyObject>::to_object(&None, py),
        }
    }
}


#[pymodule(automerge)]
fn automerge(py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(new_backend, m)?)?;
    m.add_function(wrap_pyfunction!(new_frontend, m)?)?;
    m.add_class::<PyBackend>()?;
    m.add_class::<PyFrontend>()?;
     
    Ok(())
}

