use std::{
    mem::transmute,
    sync::{Arc, RwLock},
};

use ::automerge::{
    self as am, transaction::Transactable, ChangeHash, ObjType, Prop, ReadDoc, ScalarValue,
};
use am::sync::SyncDoc;
use pyo3::{
    exceptions::PyException,
    prelude::*,
    types::{PyBytes, PyDateTime},
};

struct Inner {
    doc: am::Automerge,
    tx: Option<am::transaction::Transaction<'static>>,
}

fn get_heads(heads: Option<Vec<PyChangeHash>>) -> Option<Vec<ChangeHash>> {
    heads.map(|heads| heads.iter().map(|h| h.0).collect())
}

impl Inner {
    fn new(doc: am::Automerge) -> Self {
        Self { doc, tx: None }
    }

    // Read methods go on Inner as they're callable from either Transaction or Document.
    fn object_type(&self, obj_id: PyObjId) -> PyResult<PyObjType> {
        if let Some(tx) = self.tx.as_ref() {
            tx.object_type(obj_id.0)
        } else {
            self.doc.object_type(obj_id.0)
        }
        .map_err(|e| PyException::new_err(e.to_string()))
        .map(PyObjType::from_objtype)
    }

    fn get(
        &self,
        py: Python,
        obj_id: PyObjId,
        prop: PyProp,
        heads: Option<Vec<PyChangeHash>>,
    ) -> PyResult<Option<(PyObject, PyObjId)>> {
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
        }
        .map_err(|e| PyException::new_err(e.to_string()))?;
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
        }
        .iter()
        .map(|c| PyChangeHash(*c))
        .collect()
    }

    fn length(&self, obj_id: PyObjId, heads: Option<Vec<PyChangeHash>>) -> usize {
        if let Some(tx) = self.tx.as_ref() {
            match get_heads(heads) {
                Some(heads) => tx.length_at(obj_id.0, &heads),
                None => tx.length(obj_id.0),
            }
        } else {
            match get_heads(heads) {
                Some(heads) => self.doc.length_at(obj_id.0, &heads),
                None => self.doc.length(obj_id.0),
            }
        }
    }

    fn text(&self, obj_id: PyObjId, heads: Option<Vec<PyChangeHash>>) -> PyResult<String> {
        if let Some(tx) = self.tx.as_ref() {
            match get_heads(heads) {
                Some(heads) => tx.text_at(obj_id.0, &heads),
                None => tx.text(obj_id.0),
            }
        } else {
            match get_heads(heads) {
                Some(heads) => self.doc.text_at(obj_id.0, &heads),
                None => self.doc.text(obj_id.0),
            }
        }
        .map_err(|e| PyException::new_err(e.to_string()))
    }

    fn marks(
        &self,
        py: Python,
        obj_id: PyObjId,
        heads: Option<Vec<PyChangeHash>>,
    ) -> PyResult<Vec<PyMark>> {
        let res = if let Some(tx) = self.tx.as_ref() {
            match get_heads(heads) {
                Some(heads) => tx.marks_at(obj_id.0, &heads),
                None => tx.marks(obj_id.0),
            }
        } else {
            match get_heads(heads) {
                Some(heads) => self.doc.marks_at(obj_id.0, &heads),
                None => self.doc.marks(obj_id.0),
            }
        }
        .map_err(|e| PyException::new_err(e.to_string()))?;
        Ok(res
            .iter()
            .map(|m| PyMark {
                start: m.start,
                end: m.end,
                name: m.name().to_owned(),
                value: PyScalarValue(&m.value()).into_py(py),
            })
            .collect())
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
            inner: Arc::new(RwLock::new(Inner::new(am::Automerge::new()))),
        }
    }

    fn transaction(&self) -> PyResult<Transaction> {
        let mut inner = self
            .inner
            .write()
            .map_err(|e| PyException::new_err(format!("error getting write lock: {}", e)))?;
        if let Some(tx) = inner.tx.as_ref() {
            return Err(PyException::new_err("transaction already active"));
        }

        // Here we're transmuting the lifetime of the transaction to `static`, which is okay
        // because we are then storing the transaction in `Inner` which means the document will
        // live as long as the transaction.
        let tx = unsafe { transmute(inner.doc.transaction()) };
        inner.tx = Some(tx);
        Ok(Transaction {
            inner: Arc::clone(&self.inner),
        })
    }

    fn save<'py>(&self, py: Python<'py>) -> PyResult<&'py PyBytes> {
        let inner = self
            .inner
            .read()
            .map_err(|e| PyException::new_err(e.to_string()))?;
        if inner.tx.as_ref().is_some() {
            return Err(PyException::new_err(
                "cannot save with an active transaction",
            ));
        }

        Ok(PyBytes::new(py, &inner.doc.save()))
    }

    #[staticmethod]
    fn load(bytes: &[u8]) -> PyResult<Self> {
        let doc = am::Automerge::load(bytes).map_err(|e| PyException::new_err(e.to_string()))?;
        Ok(Self {
            inner: Arc::new(RwLock::new(Inner::new(doc))),
        })
    }

    fn generate_sync_message(&self, state: &mut PySyncState) -> PyResult<Option<PyMessage>> {
        let inner = self
            .inner
            .read()
            .map_err(|e| PyException::new_err(e.to_string()))?;
        if inner.tx.as_ref().is_some() {
            return Err(PyException::new_err(
                "cannot sync with an active transaction",
            ));
        }
        Ok(inner.doc.generate_sync_message(&mut state.0).map(PyMessage))
    }

    fn receive_sync_message(
        &mut self,
        state: &mut PySyncState,
        message: &mut PyMessage,
    ) -> PyResult<()> {
        let mut inner = self
            .inner
            .write()
            .map_err(|e| PyException::new_err(e.to_string()))?;
        if inner.tx.as_ref().is_some() {
            return Err(PyException::new_err(
                "cannot sync with an active transaction",
            ));
        }
        inner
            .doc
            .receive_sync_message(&mut state.0, message.0.clone())
            .map_err(|e| PyException::new_err(e.to_string()))
    }

    fn get_heads(&self) -> PyResult<Vec<PyChangeHash>> {
        let inner = self
            .inner
            .read()
            .map_err(|e| PyException::new_err(e.to_string()))?;
        Ok(inner.get_heads())
    }

    fn object_type(&self, obj_id: PyObjId) -> PyResult<PyObjType> {
        let inner = self
            .inner
            .read()
            .map_err(|e| PyException::new_err(e.to_string()))?;
        inner.object_type(obj_id)
    }

    fn fork(&self, heads: Option<Vec<PyChangeHash>>) -> PyResult<Document> {
        let inner = self
            .inner
            .read()
            .map_err(|e| PyException::new_err(e.to_string()))?;
        if inner.tx.as_ref().is_some() {
            return Err(PyException::new_err(
                "cannot fork with an active transaction",
            ));
        }
        let new_doc = match get_heads(heads) {
            Some(heads) => inner.doc.fork_at(&heads),
            None => Ok(inner.doc.fork()),
        }
        .map_err(|e| PyException::new_err(e.to_string()))?;
        Ok(Document {
            inner: Arc::new(RwLock::new(Inner::new(new_doc))),
        })
    }

    fn merge(&mut self, other: &Document) -> PyResult<Vec<PyChangeHash>> {
        let mut inner = self
            .inner
            .write()
            .map_err(|e| PyException::new_err(e.to_string()))?;
        if inner.tx.as_ref().is_some() {
            return Err(PyException::new_err(
                "cannot merge with an active transaction",
            ));
        }
        let mut other_inner = other
            .inner
            .write()
            .map_err(|e| PyException::new_err(e.to_string()))?;
        if other_inner.tx.as_ref().is_some() {
            return Err(PyException::new_err(
                "cannot merge with an active transaction",
            ));
        }
        inner
            .doc
            .merge(&mut other_inner.doc)
            .map(|change_hashes| change_hashes.iter().map(|h| PyChangeHash(*h)).collect())
            .map_err(|e| PyException::new_err(e.to_string()))
    }

    fn get(
        &self,
        py: Python,
        obj_id: PyObjId,
        prop: PyProp,
        heads: Option<Vec<PyChangeHash>>,
    ) -> PyResult<Option<(PyObject, PyObjId)>> {
        let inner = self
            .inner
            .read()
            .map_err(|e| PyException::new_err(e.to_string()))?;
        inner.get(py, obj_id, prop, heads)
    }

    fn keys(&self, obj_id: PyObjId, heads: Option<Vec<PyChangeHash>>) -> PyResult<Vec<String>> {
        let inner = self
            .inner
            .read()
            .map_err(|e| PyException::new_err(e.to_string()))?;
        inner.keys(obj_id, heads)
    }

    fn length(&self, obj_id: PyObjId, heads: Option<Vec<PyChangeHash>>) -> PyResult<usize> {
        let inner = self
            .inner
            .read()
            .map_err(|e| PyException::new_err(e.to_string()))?;
        Ok(inner.length(obj_id, heads))
    }

    fn text(&self, obj_id: PyObjId, heads: Option<Vec<PyChangeHash>>) -> PyResult<String> {
        let inner = self
            .inner
            .read()
            .map_err(|e| PyException::new_err(e.to_string()))?;
        inner.text(obj_id, heads)
    }

    fn marks(
        &self,
        py: Python,
        obj_id: PyObjId,
        heads: Option<Vec<PyChangeHash>>,
    ) -> PyResult<Vec<PyMark>> {
        let inner = self
            .inner
            .read()
            .map_err(|e| PyException::new_err(e.to_string()))?;
        inner.marks(py, obj_id, heads)
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
    fn exit(
        &self,
        exc_type: Option<&PyAny>,
        exc_value: Option<&PyAny>,
        traceback: Option<&PyAny>,
    ) -> PyResult<()> {
        let mut inner = self
            .inner
            .write()
            .map_err(|e| PyException::new_err(format!("error getting write lock: {}", e)))?;
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
        let inner = self
            .inner
            .read()
            .map_err(|e| PyException::new_err(e.to_string()))?;
        Ok(inner.get_heads())
    }

    fn object_type(&self, obj_id: PyObjId) -> PyResult<PyObjType> {
        let inner = self
            .inner
            .read()
            .map_err(|e| PyException::new_err(e.to_string()))?;
        inner.object_type(obj_id)
    }

    fn get(
        &self,
        py: Python,
        obj_id: PyObjId,
        prop: PyProp,
        heads: Option<Vec<PyChangeHash>>,
    ) -> PyResult<Option<(PyObject, PyObjId)>> {
        let inner = self
            .inner
            .read()
            .map_err(|e| PyException::new_err(e.to_string()))?;
        inner.get(py, obj_id, prop, heads)
    }

    fn keys(&self, obj_id: PyObjId, heads: Option<Vec<PyChangeHash>>) -> PyResult<Vec<String>> {
        let inner = self
            .inner
            .read()
            .map_err(|e| PyException::new_err(e.to_string()))?;
        inner.keys(obj_id, heads)
    }

    fn length(&self, obj_id: PyObjId, heads: Option<Vec<PyChangeHash>>) -> PyResult<usize> {
        let inner = self
            .inner
            .read()
            .map_err(|e| PyException::new_err(e.to_string()))?;
        Ok(inner.length(obj_id, heads))
    }

    fn text(&self, obj_id: PyObjId, heads: Option<Vec<PyChangeHash>>) -> PyResult<String> {
        let inner = self
            .inner
            .read()
            .map_err(|e| PyException::new_err(e.to_string()))?;
        inner.text(obj_id, heads)
    }

    fn marks(
        &self,
        py: Python,
        obj_id: PyObjId,
        heads: Option<Vec<PyChangeHash>>,
    ) -> PyResult<Vec<PyMark>> {
        let inner = self
            .inner
            .read()
            .map_err(|e| PyException::new_err(e.to_string()))?;
        inner.marks(py, obj_id, heads)
    }

    fn put(
        &mut self,
        obj_id: PyObjId,
        prop: PyProp,
        value_type: &PyScalarType,
        value: &PyAny,
    ) -> PyResult<()> {
        let mut inner = self
            .inner
            .write()
            .map_err(|e| PyException::new_err(format!("error getting write lock: {}", e)))?;
        let Some(tx) = inner.tx.as_mut() else {
            return Err(PyException::new_err("transaction no longer active"));
        };
        tx.put(obj_id.0, prop.0, import_scalar(value, value_type)?)
            .map_err(|e| PyException::new_err(format!("error putting: {}", e)))
    }

    fn put_object(
        &mut self,
        obj_id: PyObjId,
        prop: PyProp,
        objtype: &PyObjType,
    ) -> PyResult<PyObjId> {
        let mut inner = self
            .inner
            .write()
            .map_err(|e| PyException::new_err(format!("error getting write lock: {}", e)))?;
        let Some(tx) = inner.tx.as_mut() else {
            return Err(PyException::new_err("transaction no longer active"));
        };
        tx.put_object(obj_id.0, prop.0, objtype.into())
            .map_err(|e| PyException::new_err(format!("error putting: {}", e)))
            .map(PyObjId)
    }

    fn insert(
        &mut self,
        obj_id: PyObjId,
        index: usize,
        value_type: &PyScalarType,
        value: &PyAny,
    ) -> PyResult<()> {
        let mut inner = self
            .inner
            .write()
            .map_err(|e| PyException::new_err(format!("error getting write lock: {}", e)))?;
        let Some(tx) = inner.tx.as_mut() else {
            return Err(PyException::new_err("transaction no longer active"));
        };
        tx.insert(obj_id.0, index, import_scalar(value, value_type)?)
            .map_err(|e| PyException::new_err(format!("error putting: {}", e)))
    }

    fn insert_object(
        &mut self,
        obj_id: PyObjId,
        index: usize,
        objtype: &PyObjType,
    ) -> PyResult<PyObjId> {
        let mut inner = self
            .inner
            .write()
            .map_err(|e| PyException::new_err(format!("error getting write lock: {}", e)))?;
        let Some(tx) = inner.tx.as_mut() else {
            return Err(PyException::new_err("transaction no longer active"));
        };
        tx.insert_object(obj_id.0, index, objtype.into())
            .map_err(|e| PyException::new_err(format!("error putting: {}", e)))
            .map(PyObjId)
    }

    fn increment(&mut self, obj_id: PyObjId, prop: PyProp, value: i64) -> PyResult<()> {
        let mut inner = self
            .inner
            .write()
            .map_err(|e| PyException::new_err(format!("error getting write lock: {}", e)))?;
        let Some(tx) = inner.tx.as_mut() else {
            return Err(PyException::new_err("transaction no longer active"));
        };
        tx.increment(obj_id.0, prop.0, value)
            .map_err(|e| PyException::new_err(format!("error putting: {}", e)))
    }

    fn delete(&mut self, obj_id: PyObjId, prop: PyProp) -> PyResult<()> {
        let mut inner = self
            .inner
            .write()
            .map_err(|e| PyException::new_err(format!("error getting write lock: {}", e)))?;
        let Some(tx) = inner.tx.as_mut() else {
            return Err(PyException::new_err("transaction no longer active"));
        };
        tx.delete(obj_id.0, prop.0)
            .map_err(|e| PyException::new_err(format!("error putting: {}", e)))
    }
}

fn datetime_to_timestamp(datetime: &PyDateTime) -> PyResult<i64> {
    Ok((datetime.call_method0("timestamp")?.extract::<f64>()? * 1000.0).round() as i64)
}

fn import_scalar(value: &PyAny, scalar_type: &PyScalarType) -> Result<ScalarValue, PyErr> {
    Ok(match scalar_type {
        PyScalarType::Bytes => ScalarValue::Bytes(value.extract::<&[u8]>()?.to_owned()),
        PyScalarType::Str => ScalarValue::Str(value.extract::<String>()?.into()),
        PyScalarType::Int => ScalarValue::Int(value.extract::<i64>()?),
        PyScalarType::Uint => ScalarValue::Uint(value.extract::<u64>()?),
        PyScalarType::F64 => ScalarValue::F64(value.extract::<f64>()?),
        PyScalarType::Counter => todo!(),
        PyScalarType::Timestamp => {
            ScalarValue::Timestamp(datetime_to_timestamp(value.downcast::<PyDateTime>()?)?)
        }
        PyScalarType::Boolean => ScalarValue::Boolean(value.extract::<bool>()?),
        PyScalarType::Unknown => todo!(),
        PyScalarType::Null => ScalarValue::Null,
    })
}

#[pyclass(name = "SyncState")]
struct PySyncState(am::sync::State);

#[pymethods]
impl PySyncState {
    #[new]
    pub fn new() -> PySyncState {
        PySyncState(am::sync::State::new())
    }
}

#[pyclass(name = "Message")]
struct PyMessage(am::sync::Message);

#[pymethods]
impl PyMessage {
    pub fn encode<'py>(&self, py: Python<'py>) -> &'py PyBytes {
        PyBytes::new(py, &self.0.clone().encode())
    }

    #[staticmethod]
    pub fn decode(bytes: &[u8]) -> PyResult<PyMessage> {
        Ok(PyMessage(
            am::sync::Message::decode(bytes).map_err(|e| PyException::new_err(e.to_string()))?,
        ))
    }
}

/// A Python module implemented in Rust.
#[pymodule]
fn _automerge(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Document>()?;
    m.add_class::<PyObjType>()?;
    m.add_class::<PySyncState>()?;
    m.add_class::<PyMessage>()?;
    m.add_class::<PyScalarType>()?;
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
            },
        }))
    }
}

#[derive(Debug)]
pub struct PyObjId(am::ObjId);

impl<'a> FromPyObject<'a> for PyObjId {
    fn extract(prop: &'a PyAny) -> PyResult<Self> {
        prop.extract::<&[u8]>()
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
        v.extract::<&[u8]>()
            .and_then(|b| {
                am::ChangeHash::try_from(b).map_err(|e| PyException::new_err(e.to_string()))
            })
            .map(PyChangeHash)
    }
}

impl IntoPy<PyObject> for PyChangeHash {
    fn into_py(self, py: Python<'_>) -> PyObject {
        self.0.as_ref().into_py(py)
    }
}

#[derive(Debug)]
#[pyclass(name = "ObjType")]
pub enum PyObjType {
    Map,
    List,
    Text,
}

impl PyObjType {
    fn from_objtype(objtype: ObjType) -> PyObjType {
        match objtype {
            ObjType::Map => PyObjType::Map,
            ObjType::Table => todo!(),
            ObjType::List => PyObjType::List,
            ObjType::Text => PyObjType::Text,
        }
    }
}

impl Into<ObjType> for &PyObjType {
    fn into(self) -> ObjType {
        match self {
            PyObjType::Map => ObjType::Map,
            PyObjType::List => ObjType::List,
            PyObjType::Text => ObjType::Text,
        }
    }
}

#[derive(Debug)]
#[pyclass(name = "ScalarType")]
pub enum PyScalarType {
    Bytes,
    Str,
    Int,
    Uint,
    F64,
    Counter,
    Timestamp,
    Boolean,
    Unknown,
    Null,
}

#[derive(Debug)]
pub struct PyScalarValue<'a>(&'a am::ScalarValue);
impl<'a> IntoPy<PyObject> for PyScalarValue<'a> {
    fn into_py(self, py: Python<'_>) -> PyObject {
        match self.0 {
            ScalarValue::Bytes(v) => (PyScalarType::Bytes, v.clone().into_py(py)),
            ScalarValue::Str(v) => (PyScalarType::Str, v.to_owned().into_py(py)),
            ScalarValue::Int(v) => (PyScalarType::Int, v.into_py(py)),
            ScalarValue::Uint(v) => (PyScalarType::Uint, v.into_py(py)),
            ScalarValue::F64(v) => (PyScalarType::F64, v.into_py(py)),
            ScalarValue::Counter(v) => todo!(),
            ScalarValue::Timestamp(v) => (
                PyScalarType::Timestamp,
                PyDateTime::from_timestamp(py, (*v as f64) / 1000.0, None)
                    .unwrap()
                    .into_py(py),
            ),
            ScalarValue::Boolean(v) => (PyScalarType::Boolean, v.into_py(py)),
            ScalarValue::Unknown { type_code, bytes } => todo!(),
            ScalarValue::Null => (PyScalarType::Null, Python::None(py)),
        }
        .into_py(py)
    }
}

#[derive(Debug)]
pub struct PyValue<'a>(am::Value<'a>);

impl<'a> IntoPy<PyObject> for PyValue<'a> {
    fn into_py(self, py: Python<'_>) -> PyObject {
        match self.0 {
            am::Value::Object(objtype) => PyObjType::from_objtype(objtype).into_py(py),
            am::Value::Scalar(s) => PyScalarValue(&s).into_py(py),
        }
    }
}

#[pyclass]
struct PyMark {
    start: usize,
    end: usize,
    name: String,
    value: PyObject,
}
