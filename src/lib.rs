use std::{mem::transmute, sync::{Arc, RwLock}};

use pyo3::{exceptions::PyException, prelude::*, types::PyBytes};
use ::automerge::{self as am, transaction::Transactable, ChangeHash, ObjType, Prop, ReadDoc, ScalarValue};

struct Inner {
    doc: am::Automerge,
    tx: Option<am::transaction::Transaction<'static>>
}

fn get_heads(heads: Option<Vec<PyChangeHash>>) -> Option<Vec<ChangeHash>> {
    heads.map(|heads| heads.iter().map(|h| h.0).collect())
}

impl Inner {
    fn new(doc: am::Automerge) -> Self {
        Self {
            doc,
            tx: None
        }
    }

    // Read methods go on Inner as they're callable from either Transaction or Document.
    fn get(&self, py: Python, obj_id: PyObjId, prop: PyProp, heads: Option<Vec<PyChangeHash>>) -> PyResult<Option<(PyObject, PyObjId)>> {
        let res = if let Some(tx) = self.tx.as_ref() {
            match get_heads(heads) {
                Some(heads) => tx.get_at(obj_id.0, prop.0, &heads),
                None => tx.get(obj_id.0, prop.0),
            }
        } else {
            match get_heads(heads) {
                Some(heads) => self.doc.get_at(obj_id.0, prop.0, &heads),
                None => self.doc.get(obj_id.0, prop.0),
            }
        }.map_err(|e| PyException::new_err(e.to_string()))?;
        Ok(res.map(|(v, id)| (PyValue(v).into_py(py), PyObjId(id))))
    }

    fn keys(&self, obj_id: PyObjId, heads: Option<Vec<PyChangeHash>>) -> PyResult<Vec<String>> {
        let res = if let Some(tx) = self.tx.as_ref() {
            match get_heads(heads) {
                Some(heads) => tx.keys_at(obj_id.0, &heads),
                None => tx.keys(obj_id.0),
            }
        } else {
            match get_heads(heads) {
                Some(heads) => self.doc.keys_at(obj_id.0, &heads),
                None => self.doc.keys(obj_id.0),
            }
        };
        Ok(res.collect())
    }
    
    fn get_heads(&self) -> Vec<PyChangeHash> {
        if let Some(tx) = self.tx.as_ref() {
            tx.get_heads()
        } else {
            self.doc.get_heads()
        }.iter().map(|c| PyChangeHash(*c)).collect()
    }

    fn length(&self, obj_id: PyObjId, heads: Option<Vec<PyChangeHash>>) -> usize {
        if let Some(tx) = self.tx.as_ref() {
            match get_heads(heads) {
                Some(heads) => tx.length_at(obj_id.0, &heads),
                None => tx.length(obj_id.0)
            }
        } else {
            match get_heads(heads) {
                Some(heads) => self.doc.length_at(obj_id.0, &heads),
                None => self.doc.length(obj_id.0)
            }
        }
    }
}

#[pyclass]
struct Document {
    inner: Arc<RwLock<Inner>>,
}

#[pymethods]
impl Document {
    #[new]
    fn new() -> Self {
        Document {
            inner: Arc::new(RwLock::new(Inner::new(am::Automerge::new())))
        }
    }

    fn transaction(&self) -> PyResult<Transaction> {
        let mut inner = self.inner.write().map_err(|e| PyException::new_err(format!("error getting write lock: {}", e)))?;
        if let Some(tx) = inner.tx.as_ref() {
            return Err(PyException::new_err("transaction already active"));
        }

        // Here we're transmuting the lifetime of the transaction to `static`, which is okay
        // because we are then storing the transaction in `Inner` which means the document will
        // live as long as the transaction.
        let tx = unsafe { transmute(inner.doc.transaction()) };
        inner.tx = Some(tx);
        Ok(Transaction {
            inner: Arc::clone(&self.inner)
        })
    }
    
    fn save<'py>(&self, py: Python<'py>) -> PyResult<&'py PyBytes> {
        let inner = self.inner.read().map_err(|e| PyException::new_err(e.to_string()))?;
        if inner.tx.as_ref().is_some() {
            return Err(PyException::new_err("cannot save with an active transaction"))
        }

        Ok(PyBytes::new(py, &inner.doc.save()))
    }
    
    #[staticmethod]
    fn load(bytes: &[u8]) -> PyResult<Self> {
        let doc = am::Automerge::load(bytes).map_err(|e| PyException::new_err(e.to_string()))?;
        Ok(Self {
            inner: Arc::new(RwLock::new(Inner::new(doc)))
        })
    }
    
    fn get_heads(&self) -> PyResult<Vec<PyChangeHash>> {
        let inner = self.inner.read().map_err(|e| PyException::new_err(e.to_string()))?;
        Ok(inner.get_heads())
    }

    fn get(&self, py: Python, obj_id: PyObjId, prop: PyProp, heads: Option<Vec<PyChangeHash>>) -> PyResult<Option<(PyObject, PyObjId)>> {
        let inner = self.inner.read().map_err(|e| PyException::new_err(e.to_string()))?;
        inner.get(py, obj_id, prop, heads)
    }

    fn keys(&self, obj_id: PyObjId, heads: Option<Vec<PyChangeHash>>) -> PyResult<Vec<String>> {
        let inner = self.inner.read().map_err(|e| PyException::new_err(e.to_string()))?;
        inner.keys(obj_id, heads)
    }
    
    fn length(&self, obj_id: PyObjId, heads: Option<Vec<PyChangeHash>>) -> PyResult<usize> {
        let inner = self.inner.read().map_err(|e| PyException::new_err(e.to_string()))?;
        Ok(inner.length(obj_id, heads))
    }
}

#[derive(Clone)]
#[pyclass]
struct Transaction {
    inner: Arc<RwLock<Inner>>,
}

#[pymethods]
impl Transaction {
    #[pyo3(name = "__enter__")]
    fn enter(&self) -> PyResult<Transaction> {
        Ok(self.clone())
    }

    #[pyo3(name = "__exit__")]
    fn exit(&self, exc_type: Option<&PyAny>, exc_value: Option<&PyAny>, traceback: Option<&PyAny>) -> PyResult<()> {
        let mut inner = self.inner.write().map_err(|e| PyException::new_err(format!("error getting write lock: {}", e)))?;
        if let Some(tx) = inner.tx.take() {
            if let Some(exc_type) = exc_type {
                tx.rollback();
            } else {
                tx.commit();
            }
        }
        Ok(())
    }

    fn get_heads(&self) -> PyResult<Vec<PyChangeHash>> {
        let inner = self.inner.read().map_err(|e| PyException::new_err(e.to_string()))?;
        Ok(inner.get_heads())
    }

    fn get(&self, py: Python, obj_id: PyObjId, prop: PyProp, heads: Option<Vec<PyChangeHash>>) -> PyResult<Option<(PyObject, PyObjId)>> {
        let inner = self.inner.read().map_err(|e| PyException::new_err(e.to_string()))?;
        inner.get(py, obj_id, prop, heads)
    }

    fn keys(&self, obj_id: PyObjId, heads: Option<Vec<PyChangeHash>>) -> PyResult<Vec<String>> {
        let inner = self.inner.read().map_err(|e| PyException::new_err(e.to_string()))?;
        inner.keys(obj_id, heads)
    }
    
    fn put(&mut self, obj_id: PyObjId, prop: PyProp, value: &PyAny) -> PyResult<()> {
        let mut inner = self.inner.write().map_err(|e| PyException::new_err(format!("error getting write lock: {}", e)))?;
        let Some(tx) = inner.tx.as_mut() else {
            return Err(PyException::new_err("transaction no longer active"));
        };
        tx.put(obj_id.0, prop.0, import_scalar(value)?).map_err(|e| {
            PyException::new_err(format!("error putting: {}", e))
        })
    }

    fn put_object(&mut self, obj_id: PyObjId, prop: PyProp, objtype: PyObjType) -> PyResult<PyObjId> {
        let mut inner = self.inner.write().map_err(|e| PyException::new_err(format!("error getting write lock: {}", e)))?;
        let Some(tx) = inner.tx.as_mut() else {
            return Err(PyException::new_err("transaction no longer active"));
        };
        tx.put_object(obj_id.0, prop.0, objtype.0).map_err(|e| {
            PyException::new_err(format!("error putting: {}", e))
        }).map(PyObjId)
    }
    
    fn insert(&mut self, obj_id: PyObjId, index: usize, value: &PyAny) -> PyResult<()> {
        let mut inner = self.inner.write().map_err(|e| PyException::new_err(format!("error getting write lock: {}", e)))?;
        let Some(tx) = inner.tx.as_mut() else {
            return Err(PyException::new_err("transaction no longer active"));
        };
        tx.insert(obj_id.0, index, import_scalar(value)?).map_err(|e| {
            PyException::new_err(format!("error putting: {}", e))
        })
    }

    fn insert_object(&mut self, obj_id: PyObjId, index: usize, objtype: PyObjType) -> PyResult<PyObjId> {
        let mut inner = self.inner.write().map_err(|e| PyException::new_err(format!("error getting write lock: {}", e)))?;
        let Some(tx) = inner.tx.as_mut() else {
            return Err(PyException::new_err("transaction no longer active"));
        };
        tx.insert_object(obj_id.0, index, objtype.0).map_err(|e| {
            PyException::new_err(format!("error putting: {}", e))
        }).map(PyObjId)
    }
    
    fn increment(&mut self, obj_id: PyObjId, prop: PyProp, value: i64) -> PyResult<()> {
        let mut inner = self.inner.write().map_err(|e| PyException::new_err(format!("error getting write lock: {}", e)))?;
        let Some(tx) = inner.tx.as_mut() else {
            return Err(PyException::new_err("transaction no longer active"));
        };
        tx.increment(obj_id.0, prop.0, value).map_err(|e| {
            PyException::new_err(format!("error putting: {}", e))
        })
    }

    fn delete(&mut self, obj_id: PyObjId, prop: PyProp) -> PyResult<()> {
        let mut inner = self.inner.write().map_err(|e| PyException::new_err(format!("error getting write lock: {}", e)))?;
        let Some(tx) = inner.tx.as_mut() else {
            return Err(PyException::new_err("transaction no longer active"));
        };
        tx.delete(obj_id.0, prop.0).map_err(|e| {
            PyException::new_err(format!("error putting: {}", e))
        })
    }
}

fn import_scalar(value: &PyAny) -> Result<ScalarValue, PyErr> {
    if value.is_none() {
        Ok(ScalarValue::Null)
    } else if let Ok(b) = value.extract::<bool>() {
        Ok(ScalarValue::Boolean(b))
    } else if let Ok(s) = value.extract::<String>() {
        Ok(ScalarValue::Str(s.into()))
    } else if let Ok(n) = value.extract::<f64>() {
        if (n.round() - n).abs() < f64::EPSILON {
            Ok(ScalarValue::Int(n as i64))
        } else {
            Ok(ScalarValue::F64(n))
        }
    } else if let Ok(o) = &value.extract::<&[u8]>() {
        Ok(ScalarValue::Bytes(o.to_vec()))
    } else {
        Err(PyException::new_err("unknown value type"))
    }
}

/// A Python module implemented in Rust.
#[pymodule]
fn automerge(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Document>()?;
    m.add("ROOT", PyObjId(am::ROOT))?;
    Ok(())
}

#[derive(Debug)]
pub struct PyProp(Prop);

impl<'a> FromPyObject<'a> for PyProp {
    fn extract(prop: &'a PyAny) -> PyResult<Self> {
        Ok(PyProp(match prop.extract::<String>() {
            Ok(s) => Prop::Map(s),
            Err(_) => match prop.extract::<usize>() {
                Ok(i) => Prop::Seq(i),
                Err(_) => todo!(),
            }
        }))
    }
}

#[derive(Debug)]
pub struct PyObjId(am::ObjId);

impl<'a> FromPyObject<'a> for PyObjId {
    fn extract(prop: &'a PyAny) -> PyResult<Self> {
        prop
            .extract::<&[u8]>()
            .and_then(|b| am::ObjId::try_from(b).map_err(|e| PyException::new_err(e.to_string())))
            .map(PyObjId)
    }
}

impl IntoPy<PyObject> for PyObjId {
    fn into_py(self, py: Python<'_>) -> PyObject {
        let bytes: &[u8] = &self.0.to_bytes();
        bytes.into_py(py)
    }
}

#[derive(Debug)]
pub struct PyChangeHash(am::ChangeHash);

impl<'a> FromPyObject<'a> for PyChangeHash {
    fn extract(v: &'a PyAny) -> PyResult<Self> {
        v
            .extract::<&[u8]>()
            .and_then(|b| am::ChangeHash::try_from(b).map_err(|e| PyException::new_err(e.to_string())))
            .map(PyChangeHash)
    }
}

impl IntoPy<PyObject> for PyChangeHash {
    fn into_py(self, py: Python<'_>) -> PyObject {
        self.0.as_ref().into_py(py)
    }
}

#[derive(Debug)]
pub struct PyObjType(ObjType);

impl<'a> FromPyObject<'a> for PyObjType {
    fn extract(prop: &'a PyAny) -> PyResult<Self> {
        Ok(PyObjType(match prop.extract::<&str>() {
            Ok("map") => ObjType::Map,
            Ok("list") => ObjType::List,
            Ok("text") => ObjType::Text,
            Ok(_) => todo!(),
            Err(_) => todo!(),
        }))
    }
}

impl IntoPy<PyObject> for PyObjType {
    fn into_py(self, py: Python<'_>) -> PyObject {
        match self.0 {
            ObjType::Map => "map",
            ObjType::Table => "table",
            ObjType::List => "list",
            ObjType::Text => "text",
        }.into_py(py)
    }
}

#[derive(Debug)]
pub struct PyValue<'a>(am::Value<'a>);

impl<'a> IntoPy<PyObject> for PyValue<'a> {
    fn into_py(self, py: Python<'_>) -> PyObject {
        match self.0 {
            am::Value::Object(objtype) => PyObjType(objtype).into_py(py),
            am::Value::Scalar(s) => match s.as_ref() {
                ScalarValue::Bytes(v) => v.clone().into_py(py),
                ScalarValue::Str(v) => v.to_owned().into_py(py),
                ScalarValue::Int(v) => v.into_py(py),
                ScalarValue::Uint(v) => v.into_py(py),
                ScalarValue::F64(v) => v.into_py(py),
                ScalarValue::Counter(v) => todo!(),
                ScalarValue::Timestamp(v) => v.into_py(py),
                ScalarValue::Boolean(v) => v.into_py(py),
                ScalarValue::Unknown { type_code, bytes } => todo!(),
                ScalarValue::Null => Python::None(py),
            },
        }
    }
}
