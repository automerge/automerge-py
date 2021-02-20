use automerge::{
    Backend, Frontend, InvalidChangeRequest, LocalChange, MapType, Path, ScalarValue, Value,
};
use maplit::hashmap;
use std::collections::HashMap;

use pyo3::prelude::*;
use pyo3::type_object::PyTypeObject;
use pyo3::types::{
    PyAny, PyBool, PyByteArray, PyBytes, PyDict, PyFloat, PyInt, PyList, PyLong, PyString,
    PyUnicode,
};
use pyo3::wrap_pyfunction;
use serde::{Deserialize, Serialize};

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
    Ok(PyBackend {
        backend: Backend::init(),
    })
}

#[pyclass(unsendable)]
struct PyFrontend {
    frontend: Frontend,
}

#[pymethods]
impl PyFrontend {
    fn value_at_path(&self, path: Vec<&PyAny>) -> PyResult<Option<AutomergeType>> {
        let val = self
            .frontend
            .value_at_path(&values_to_path(path)?)
            .map(|v| match v {
                automerge::Value::Map(..) => AutomergeType::Map,
                automerge::Value::Sequence(..) => AutomergeType::List,
                automerge::Value::Text(..) => AutomergeType::Text,
                automerge::Value::Primitive(pval) => AutomergeType::Value(PyAutoScalar(pval)),
            });
        Ok(val)
    }

    fn set_at_path<'p>(
        &mut self,
        py: Python<'p>,
        path: Vec<&PyAny>,
        value: &'p PyAny,
    ) -> PyResult<&'p PyAny> {
        // Create a "change" action, that sets the value for the given key
        let change = LocalChange::set(values_to_path(path)?, py_to_automerge_val(value));
        // Apply this change
        let change_request = self
            .frontend
            .change::<_, InvalidChangeRequest>(Some("set".into()), |frontend| {
                frontend.add_change(change)?;
                Ok(())
            })
            .unwrap();

        let result = serde_json::to_string(&change_request).unwrap();
        Ok(PyUnicode::new(py, &result))
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
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "Path should be a list of strings and integers",
            ));
        }
    }
    Ok(result)
}

fn py_to_automerge_val(py_value: &PyAny) -> Value {
    let gil = Python::acquire_gil();
    let py = gil.python();
    let scalar_null = ScalarValue::Null;
    let mut converted_value: Value = Value::Primitive(scalar_null);

    if PyInt::type_object(py).is_instance(py_value).unwrap() {
        // First, extract the int value
        let int_value = py_value
            .downcast::<PyInt>()
            .unwrap()
            .extract::<i64>() // To build an Automerge Scalar Value, we need i64
            .unwrap();

        // Turn it into a scalar value for automerge - required for Primitive frontend values
        let scalar_value = ScalarValue::Int(int_value);

        // Now, we can build a frontend value from this scalar value
        converted_value = Value::Primitive(scalar_value);
    } else if PyFloat::type_object(py).is_instance(py_value).unwrap() {
        // First, extract the Float value
        let float_value = py_value
            .downcast::<PyFloat>()
            .unwrap()
            .extract::<f64>() // To build an Automerge Scalar Value, we need f32 or f64
            .unwrap();

        // Turn it into a scalar value for automerge - required for Primitive frontend values
        let scalar_value = ScalarValue::F64(float_value);

        // Now, we can build a frontend value from this scalar value
        converted_value = Value::Primitive(scalar_value);
    } else if PyBool::type_object(py).is_instance(py_value).unwrap() {
        // First, extract the Float value
        let bool_value = py_value
            .downcast::<PyBool>()
            .unwrap()
            .extract::<bool>() // To build an Automerge Scalar Value, we need a bool
            .unwrap();

        // Turn it into a scalar value for automerge - required for Primitive frontend values
        let scalar_value = ScalarValue::Boolean(bool_value);

        // Now, we can build a frontend value from this scalar value
        converted_value = Value::Primitive(scalar_value);
    }
    /*else if PyByteArray::type_object(py).is_instance(py_value).unwrap() {
        // TODO :  Build the frontend value
        let byte_array_value = py_value.downcast::<PyByteArray>().unwrap();
        println!(" RUST TODO HANDLE BYTE ARRAY {:?}", py_value);
        println!(" RUST UNICODE VALUE {:?}", byte_array_value);
    }*/
    else if PyUnicode::type_object(py).is_instance(py_value).unwrap() {
        let unicode_value = py_value.downcast::<PyUnicode>().unwrap();

        converted_value = Value::Text(unicode_value.to_str().unwrap().chars().collect());
    } else if PyList::type_object(py).is_instance(py_value).unwrap() {
        // Extract the list value
        let list_value = py_value.downcast::<PyList>().unwrap();

        let mut converted_list: std::vec::Vec<Value> = std::vec::Vec::new();
        for item in list_value.iter() {
            converted_list.push(py_to_automerge_val(item));
        }

        converted_value = Value::Sequence(converted_list);
    } else if PyDict::type_object(py).is_instance(py_value).unwrap() {
        // Extract the dict value
        let dict_value = py_value.downcast::<PyDict>().unwrap();

        // WARNING : Automerge only handles HashMap<String, Value>
        // So we can't handle python dicts with keys other than strings for the moment.
        let mut hashmap_converted: HashMap<String, Value> = HashMap::new();

        for key in dict_value.keys() {
            hashmap_converted
                .entry(key.to_string())
                .or_insert(py_to_automerge_val(dict_value.get_item(key).unwrap()));
        }

        converted_value = Value::Map(hashmap_converted, MapType::Map);
    } else if py_value.is_none() {
        let scalar_value = ScalarValue::Null;
        converted_value = Value::Primitive(scalar_value);
    } else {
        // TODO : handle this better
        println!(" RUST COULDNT CAST {:?}", py_value);
    }
    return converted_value;
}

#[pyfunction]
fn new_frontend() -> PyResult<PyFrontend> {
    Ok(PyFrontend {
        frontend: Frontend::new(),
    })
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
                let result = hashmap! {"value" => val};
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
fn automerge(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(new_backend, m)?)?;
    m.add_function(wrap_pyfunction!(new_frontend, m)?)?;
    m.add_class::<PyBackend>()?;
    m.add_class::<PyFrontend>()?;
    Ok(())
}
