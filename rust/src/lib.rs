use std::{
    collections::BTreeMap,
    mem::transmute,
    sync::{Arc, RwLock},
};

use ::automerge::{
    self as am, transaction::Transactable, ChangeHash, ObjType, Prop, ReadDoc, ScalarValue,
};
use am::{
    marks::{ExpandMark, Mark},
    sync::SyncDoc,
    ActorId,
};
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

    fn get<'py>(
        &self,
        obj_id: PyObjId,
        prop: PyProp,
        heads: Option<Vec<PyChangeHash>>,
    ) -> PyResult<Option<(PyValue<'py>, PyObjId)>> {
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
        Ok(res.map(|(v, id)| (PyValue(v.into_owned()), PyObjId(id))))
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

    fn values<'py>(
        &self,
        obj_id: PyObjId,
        heads: Option<Vec<PyChangeHash>>,
    ) -> PyResult<Vec<(PyValue<'py>, PyObjId)>> {
        let res = if let Some(tx) = self.tx.as_ref() {
            match get_heads(heads) {
                Some(heads) => tx.values_at(obj_id.0, &heads),
                None => tx.values(obj_id.0),
            }
        } else {
            match get_heads(heads) {
                Some(heads) => self.doc.values_at(obj_id.0, &heads),
                None => self.doc.values(obj_id.0),
            }
        }
        .map(|(v, id)| (PyValue(v.into_owned()), PyObjId(id)));
        Ok(res.collect())
    }

    fn get_heads(&self) -> Vec<PyChangeHash> {
        if let Some(tx) = self.tx.as_ref() {
            tx.get_heads()
        } else {
            self.doc.get_heads()
        }
        .into_iter()
        .map(|c| PyChangeHash(c))
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

    fn marks(&self, obj_id: PyObjId, heads: Option<Vec<PyChangeHash>>) -> PyResult<Vec<PyMark>> {
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
            .into_iter()
            .map(|m| PyMark {
                start: m.start,
                end: m.end,
                name: m.name().to_owned(),
                value: PyScalarValue(m.value().clone()),
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
    fn new(actor_id: Option<&[u8]>) -> Self {
        let mut doc = am::Automerge::new();
        if let Some(id) = actor_id {
            doc.set_actor(ActorId::from(id));
        }
        Document {
            inner: Arc::new(RwLock::new(Inner::new(doc))),
        }
    }

    fn get_actor<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        let inner = self
            .inner
            .read()
            .map_err(|e| PyException::new_err(e.to_string()))?;
        if inner.tx.is_some() {
            return Err(PyException::new_err(
                "cannot get actor id with an active transaction",
            ));
        }

        Ok(PyBytes::new_bound(py, inner.doc.get_actor().to_bytes()))
    }

    fn set_actor(&mut self, actor_id: &[u8]) -> PyResult<()> {
        let mut inner = self
            .inner
            .write()
            .map_err(|e| PyException::new_err(format!("error getting write lock: {}", e)))?;
        if inner.tx.is_some() {
            return Err(PyException::new_err(
                "cannot set actor with an active transaction",
            ));
        }

        inner.doc.set_actor(ActorId::from(actor_id));
        Ok(())
    }

    fn transaction(&self) -> PyResult<Transaction> {
        let mut inner = self
            .inner
            .write()
            .map_err(|e| PyException::new_err(format!("error getting write lock: {}", e)))?;
        if inner.tx.is_some() {
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

    fn save<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        let inner = self
            .inner
            .read()
            .map_err(|e| PyException::new_err(e.to_string()))?;
        if inner.tx.is_some() {
            return Err(PyException::new_err(
                "cannot save with an active transaction",
            ));
        }

        Ok(PyBytes::new_bound(py, &inner.doc.save()))
    }

    #[staticmethod]
    fn load(bytes: &[u8]) -> PyResult<Self> {
        let doc = am::Automerge::load(bytes).map_err(|e| PyException::new_err(e.to_string()))?;
        Ok(Self {
            inner: Arc::new(RwLock::new(Inner::new(doc))),
        })
    }

    fn fork(&self, heads: Option<Vec<PyChangeHash>>) -> PyResult<Document> {
        let inner = self
            .inner
            .read()
            .map_err(|e| PyException::new_err(e.to_string()))?;
        if inner.tx.is_some() {
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
        if inner.tx.is_some() {
            return Err(PyException::new_err(
                "cannot merge with an active transaction",
            ));
        }
        let mut other_inner = other
            .inner
            .write()
            .map_err(|e| PyException::new_err(e.to_string()))?;
        if other_inner.tx.is_some() {
            return Err(PyException::new_err(
                "cannot merge with an active transaction",
            ));
        }
        inner
            .doc
            .merge(&mut other_inner.doc)
            .map(|change_hashes| change_hashes.into_iter().map(|h| PyChangeHash(h)).collect())
            .map_err(|e| PyException::new_err(e.to_string()))
    }

    fn diff(
        &self,
        before_heads: Vec<PyChangeHash>,
        after_heads: Vec<PyChangeHash>,
    ) -> PyResult<Vec<PyPatch>> {
        let inner = self
            .inner
            .read()
            .map_err(|e| PyException::new_err(e.to_string()))?;
        if inner.tx.is_some() {
            return Err(PyException::new_err(
                "cannot diff with an active transaction",
            ));
        }
        let before_heads: Vec<ChangeHash> = before_heads.iter().map(|h| h.0).collect();
        let after_heads: Vec<ChangeHash> = after_heads.iter().map(|h| h.0).collect();
        Ok(inner
            .doc
            .diff(
                &before_heads,
                &after_heads,
                am::patches::TextRepresentation::Array,
            )
            .into_iter()
            .map(|p| PyPatch(p))
            .collect())
    }

    fn generate_sync_message(&self, state: &mut PySyncState) -> PyResult<Option<PyMessage>> {
        let inner = self
            .inner
            .read()
            .map_err(|e| PyException::new_err(e.to_string()))?;
        if inner.tx.is_some() {
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
        if inner.tx.is_some() {
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

    fn get_last_local_change(&self) -> PyResult<Option<PyChange>> {
        let inner = self
            .inner
            .read()
            .map_err(|e| PyException::new_err(e.to_string()))?;
        Ok(inner
            .doc
            .get_last_local_change()
            .map(|c| PyChange(c.to_owned())))
    }

    fn object_type(&self, obj_id: PyObjId) -> PyResult<PyObjType> {
        let inner = self
            .inner
            .read()
            .map_err(|e| PyException::new_err(e.to_string()))?;
        inner.object_type(obj_id)
    }

    fn get_changes(&self, have_deps: Vec<PyChangeHash>) -> PyResult<Vec<PyChange>> {
        let inner = self
            .inner
            .read()
            .map_err(|e| PyException::new_err(e.to_string()))?;
        if inner.tx.is_some() {
            return Err(PyException::new_err(
                "cannot get changes with an active transaction",
            ));
        }

        let changes: Vec<ChangeHash> = have_deps.iter().map(|h| h.0).collect();
        Ok(inner
            .doc
            .get_changes(&changes)
            .into_iter()
            .map(|c| PyChange(c.to_owned()))
            .collect())
    }

    fn get(
        &self,
        obj_id: PyObjId,
        prop: PyProp,
        heads: Option<Vec<PyChangeHash>>,
    ) -> PyResult<Option<(PyValue, PyObjId)>> {
        let inner = self
            .inner
            .read()
            .map_err(|e| PyException::new_err(e.to_string()))?;
        inner.get(obj_id, prop, heads)
    }

    fn keys(&self, obj_id: PyObjId, heads: Option<Vec<PyChangeHash>>) -> PyResult<Vec<String>> {
        let inner = self
            .inner
            .read()
            .map_err(|e| PyException::new_err(e.to_string()))?;
        inner.keys(obj_id, heads)
    }

    fn values(
        &self,
        obj_id: PyObjId,
        heads: Option<Vec<PyChangeHash>>,
    ) -> PyResult<Vec<(PyValue, PyObjId)>> {
        let inner = self
            .inner
            .read()
            .map_err(|e| PyException::new_err(e.to_string()))?;
        inner.values(obj_id, heads)
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

    fn marks(&self, obj_id: PyObjId, heads: Option<Vec<PyChangeHash>>) -> PyResult<Vec<PyMark>> {
        let inner = self
            .inner
            .read()
            .map_err(|e| PyException::new_err(e.to_string()))?;
        inner.marks(obj_id, heads)
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

    #[pyo3(name = "__exit__", signature = (exc_type=None, exc_value=None, traceback=None))]
    fn exit(
        &self,
        exc_type: Option<Bound<'_, PyAny>>,
        exc_value: Option<Bound<'_, PyAny>>,
        traceback: Option<Bound<'_, PyAny>>,
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
        obj_id: PyObjId,
        prop: PyProp,
        heads: Option<Vec<PyChangeHash>>,
    ) -> PyResult<Option<(PyValue, PyObjId)>> {
        let inner = self
            .inner
            .read()
            .map_err(|e| PyException::new_err(e.to_string()))?;
        inner.get(obj_id, prop, heads)
    }

    fn keys(&self, obj_id: PyObjId, heads: Option<Vec<PyChangeHash>>) -> PyResult<Vec<String>> {
        let inner = self
            .inner
            .read()
            .map_err(|e| PyException::new_err(e.to_string()))?;
        inner.keys(obj_id, heads)
    }

    fn values(
        &self,
        obj_id: PyObjId,
        heads: Option<Vec<PyChangeHash>>,
    ) -> PyResult<Vec<(PyValue, PyObjId)>> {
        let inner = self
            .inner
            .read()
            .map_err(|e| PyException::new_err(e.to_string()))?;
        inner.values(obj_id, heads)
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

    fn marks(&self, obj_id: PyObjId, heads: Option<Vec<PyChangeHash>>) -> PyResult<Vec<PyMark>> {
        let inner = self
            .inner
            .read()
            .map_err(|e| PyException::new_err(e.to_string()))?;
        inner.marks(obj_id, heads)
    }

    fn put(
        &mut self,
        obj_id: PyObjId,
        prop: PyProp,
        value_type: &PyScalarType,
        value: Bound<'_, PyAny>,
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
        value: Bound<'_, PyAny>,
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

    fn mark(
        &mut self,
        obj_id: PyObjId,
        start: usize,
        end: usize,
        name: &str,
        value_type: &PyScalarType,
        value: Bound<'_, PyAny>,
        expand: &PyExpandMark,
    ) -> PyResult<()> {
        let mut inner = self
            .inner
            .write()
            .map_err(|e| PyException::new_err(format!("error getting write lock: {}", e)))?;
        let Some(tx) = inner.tx.as_mut() else {
            return Err(PyException::new_err("transaction no longer active"));
        };
        let value = import_scalar(value, value_type)?;
        tx.mark(
            obj_id.0,
            Mark::new(name.to_owned(), value, start, end),
            expand.into(),
        )
        .map_err(|e| PyException::new_err(e.to_string()))
    }

    fn unmark(
        &mut self,
        obj_id: PyObjId,
        start: usize,
        end: usize,
        name: &str,
        expand: &PyExpandMark,
    ) -> PyResult<()> {
        let mut inner = self
            .inner
            .write()
            .map_err(|e| PyException::new_err(format!("error getting write lock: {}", e)))?;
        let Some(tx) = inner.tx.as_mut() else {
            return Err(PyException::new_err("transaction no longer active"));
        };
        tx.unmark(obj_id.0, name, start, end, expand.into())
            .map_err(|e| PyException::new_err(e.to_string()))
    }
}

fn datetime_to_timestamp(datetime: Bound<'_, PyDateTime>) -> PyResult<i64> {
    Ok((datetime.call_method0("timestamp")?.extract::<f64>()? * 1000.0).round() as i64)
}

fn import_scalar(
    value: Bound<'_, PyAny>,
    scalar_type: &PyScalarType,
) -> Result<ScalarValue, PyErr> {
    Ok(match scalar_type {
        PyScalarType::Bytes => ScalarValue::Bytes(value.extract::<&[u8]>()?.to_owned()),
        PyScalarType::Str => ScalarValue::Str(value.extract::<String>()?.into()),
        PyScalarType::Int => ScalarValue::Int(value.extract::<i64>()?),
        PyScalarType::Uint => ScalarValue::Uint(value.extract::<u64>()?),
        PyScalarType::F64 => ScalarValue::F64(value.extract::<f64>()?),
        PyScalarType::Counter => todo!(),
        PyScalarType::Timestamp => ScalarValue::Timestamp(datetime_to_timestamp(
            value.downcast::<PyDateTime>()?.clone(),
        )?),
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
    pub fn encode<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new_bound(py, &self.0.clone().encode())
    }

    #[staticmethod]
    pub fn decode(bytes: &[u8]) -> PyResult<PyMessage> {
        Ok(PyMessage(
            am::sync::Message::decode(bytes).map_err(|e| PyException::new_err(e.to_string()))?,
        ))
    }
}

#[pyfunction]
fn random_actor_id<'py>(py: Python<'py>) -> Bound<'py, PyBytes> {
    PyBytes::new_bound(py, ActorId::random().to_bytes())
}

/// A Python module implemented in Rust.
#[pymodule]
fn _automerge(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Classes
    m.add_class::<Document>()?;
    m.add_class::<Transaction>()?;
    m.add_class::<PySyncState>()?;
    m.add_class::<PyMessage>()?;

    // Enums
    m.add_class::<PyObjType>()?;
    m.add_class::<PyScalarType>()?;
    m.add_class::<PyExpandMark>()?;
    // m.add_class::<PyActionType>()?;

    // Constants
    m.add("ROOT", PyObjId(am::ROOT))?;

    // Functions
    m.add_function(wrap_pyfunction!(random_actor_id, m)?)?;

    // Submodule
    let submodule = PyModule::new_bound(py, "patch_action")?;
    submodule.add_class::<PutMap>()?;
    submodule.add_class::<PutSeq>()?;
    submodule.add_class::<Increment>()?;
    submodule.add_class::<Insert>()?;
    submodule.add_class::<Conflict>()?;
    submodule.add_class::<DeleteMap>()?;
    submodule.add_class::<DeleteSeq>()?;
    submodule.add_class::<MarkAction>()?;
    m.add_submodule(&submodule)?;
    Ok(())
}

#[derive(Debug, Clone)]
pub struct PyProp(Prop);

impl<'a> FromPyObject<'a> for PyProp {
    fn extract_bound(prop: &Bound<'a, PyAny>) -> PyResult<Self> {
        Ok(PyProp(match prop.extract::<String>() {
            Ok(s) => Prop::Map(s),
            Err(_) => match prop.extract::<usize>() {
                Ok(i) => Prop::Seq(i),
                Err(_) => todo!(),
            },
        }))
    }
}

impl IntoPy<PyObject> for PyProp {
    fn into_py(self, py: Python<'_>) -> PyObject {
        match self.0 {
            am::Prop::Map(s) => s.into_py(py),
            am::Prop::Seq(i) => i.into_py(py),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PyObjId(am::ObjId);

impl<'a> FromPyObject<'a> for PyObjId {
    fn extract_bound(prop: &Bound<'a, PyAny>) -> PyResult<Self> {
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
    fn extract_bound(v: &Bound<'a, PyAny>) -> PyResult<Self> {
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

#[derive(Debug, Clone, PartialEq)]
#[pyclass(name = "ObjType", eq, eq_int)]
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

#[derive(Debug, Clone, PartialEq)]
#[pyclass(name = "ScalarType", eq, eq_int)]
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

#[derive(Debug, Clone)]
pub struct PyScalarValue(am::ScalarValue);
impl IntoPy<PyObject> for PyScalarValue {
    fn into_py(self, py: Python<'_>) -> PyObject {
        match self.0 {
            ScalarValue::Bytes(v) => (PyScalarType::Bytes, v.into_py(py)),
            ScalarValue::Str(v) => (PyScalarType::Str, v.into_py(py)),
            ScalarValue::Int(v) => (PyScalarType::Int, v.into_py(py)),
            ScalarValue::Uint(v) => (PyScalarType::Uint, v.into_py(py)),
            ScalarValue::F64(v) => (PyScalarType::F64, v.into_py(py)),
            ScalarValue::Counter(v) => todo!(),
            ScalarValue::Timestamp(v) => (
                PyScalarType::Timestamp,
                PyDateTime::from_timestamp_bound(py, (v as f64) / 1000.0, None)
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

impl<'a> FromPyObject<'a> for PyScalarValue {
    fn extract_bound(v: &Bound<'a, PyAny>) -> PyResult<Self> {
        v.extract::<(PyScalarType, Bound<'a, PyAny>)>()
            .and_then(|(t, v)| import_scalar(v, &t).map(|v| PyScalarValue(v)))
    }
}

#[derive(Debug, Clone)]
pub struct PyValue<'a>(am::Value<'a>);

impl<'a> IntoPy<PyObject> for PyValue<'a> {
    fn into_py(self, py: Python<'_>) -> PyObject {
        match self.0 {
            am::Value::Object(objtype) => PyObjType::from_objtype(objtype).into_py(py),
            am::Value::Scalar(s) => PyScalarValue(s.as_ref().clone()).into_py(py),
        }
    }
}

#[pyclass(name = "Mark", get_all, set_all)]
#[derive(Debug, Clone)]
struct PyMark {
    start: usize,
    end: usize,
    name: String,
    value: PyScalarValue,
}

#[pymethods]
impl PyMark {
    fn __repr__(&self) -> String {
        format!("{:?}", self)
    }
}

#[pyclass(name = "ExpandMark")]
enum PyExpandMark {
    Before,
    After,
    Both,
    // "None" is a reserved word in Python.
    Neither,
}

impl Into<ExpandMark> for &PyExpandMark {
    fn into(self) -> ExpandMark {
        match self {
            PyExpandMark::Before => ExpandMark::Before,
            PyExpandMark::After => ExpandMark::After,
            PyExpandMark::Both => ExpandMark::Both,
            PyExpandMark::Neither => ExpandMark::None,
        }
    }
}

#[pyclass(name = "Change")]
#[derive(Debug)]
struct PyChange(am::Change);

#[pymethods]
impl PyChange {
    fn __repr__(&self) -> String {
        format!("{:?}", self.0)
    }

    #[getter]
    fn actor_id(&self) -> &[u8] {
        self.0.actor_id().to_bytes()
    }

    #[getter]
    fn other_actor_ids(&self) -> Vec<&[u8]> {
        self.0
            .other_actor_ids()
            .iter()
            .map(|id| id.to_bytes())
            .collect()
    }

    fn __len__(&self) -> usize {
        self.0.len()
    }

    #[getter]
    fn max_op(&self) -> u64 {
        self.0.max_op()
    }

    #[getter]
    fn start_op(&self) -> u64 {
        self.0.start_op().into()
    }

    #[getter]
    fn message(&self) -> Option<String> {
        self.0.message().cloned()
    }

    #[getter]
    fn deps(&self) -> Vec<PyChangeHash> {
        self.0.deps().iter().map(|h| PyChangeHash(*h)).collect()
    }

    #[getter]
    fn hash(&self) -> PyChangeHash {
        PyChangeHash(self.0.hash())
    }

    #[getter]
    fn seq(&self) -> u64 {
        self.0.seq()
    }

    #[getter]
    fn timestamp<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDateTime>> {
        PyDateTime::from_timestamp_bound(py, (self.0.timestamp() as f64) / 1000.0, None)
    }

    #[getter]
    fn bytes<'py>(&mut self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new_bound(py, self.0.bytes().as_ref())
    }

    #[getter]
    fn raw_bytes(&self) -> &[u8] {
        self.0.raw_bytes()
    }

    #[getter]
    fn extra_bytes(&self) -> &[u8] {
        self.0.extra_bytes()
    }
}

#[pyclass(name = "Patch")]
#[derive(Debug)]
struct PyPatch(am::Patch);

#[pymethods]
impl PyPatch {
    fn __repr__(&self) -> String {
        format!("{:?}", self.0)
    }

    #[getter]
    fn obj(&self) -> PyObjId {
        PyObjId(self.0.obj.clone())
    }

    #[getter]
    fn path(&self) -> Vec<(PyObjId, PyProp)> {
        self.0
            .path
            .iter()
            .map(|(obj, prop)| (PyObjId(obj.clone()), PyProp(prop.clone())))
            .collect()
    }
    #[getter]
    fn action(&self, py: Python<'_>) -> PyObject {
        let action: PyActionType = self.0.action.clone().into();
        match action {
            PyActionType::Conflict(v) => v.into_py(py),
            PyActionType::DeleteMap(v) => v.into_py(py),
            PyActionType::DeleteSeq(v) => v.into_py(py),
            PyActionType::Increment(v) => v.into_py(py),
            PyActionType::Insert(v) => v.into_py(py),
            PyActionType::Mark(v) => v.into_py(py),
            PyActionType::PutMap(v) => v.into_py(py),
            PyActionType::PutSeq(v) => v.into_py(py),
            PyActionType::SpliceText(v) => v.into_py(py),
        }
    }
}

impl From<am::PatchAction> for PyActionType {
    fn from(value: am::PatchAction) -> PyActionType {
        match value {
            am::PatchAction::PutMap {
                key,
                value,
                conflict,
            } => PyActionType::PutMap(PutMap {
                key: key.clone(),
                value: (PyValue(value.0.clone()), PyObjId(value.1.clone())),
                conflict: conflict,
            }),

            am::PatchAction::PutSeq {
                index,
                value,
                conflict,
            } => PyActionType::PutSeq(PutSeq {
                index: index,
                value: (PyValue(value.0.clone()), PyObjId(value.1.clone())),
                conflict: conflict,
            }),
            am::PatchAction::Insert { index, values } => {
                let values: Vec<_> = values
                    .iter()
                    .map(|(v, oid, b)| (PyValue(v.clone()), PyObjId(oid.clone()), *b))
                    .collect();

                PyActionType::Insert(Insert {
                    index: index,
                    values: values,
                })
            }
            am::PatchAction::SpliceText {
                index,
                value,
                marks,
            } => {
                let marks = match marks {
                    Some(marks) => {
                        let marks: BTreeMap<String, PyScalarValue> = marks
                            .iter()
                            .map(|(key, value)| (key.to_string(), PyScalarValue(value.clone())))
                            .collect();
                        Some(marks)
                    }
                    None => None,
                };

                PyActionType::SpliceText(SpliceText {
                    index: index,
                    value: value.make_string(),
                    marks: marks,
                })
            }
            am::PatchAction::Increment { prop, value } => PyActionType::Increment(Increment {
                prop: PyProp(prop.clone()),
                value: value,
            }),
            am::PatchAction::Conflict { prop } => PyActionType::Conflict(Conflict {
                prop: PyProp(prop.clone()),
            }),
            am::PatchAction::DeleteMap { key } => {
                PyActionType::DeleteMap(DeleteMap { key: key.clone() })
            }
            am::PatchAction::DeleteSeq { index, length } => PyActionType::DeleteSeq(DeleteSeq {
                index: index,
                length: length,
            }),
            am::PatchAction::Mark { marks } => PyActionType::Mark(MarkAction {
                marks: marks
                    .iter()
                    .map(|m| PyMark {
                        start: m.start,
                        end: m.end,
                        name: m.name().to_owned(),
                        value: PyScalarValue(m.value().clone()),
                    })
                    .collect(),
            }),
        }
    }
}

/// Enum to represent the possible actions in `PyPatchAction`.

pub enum PyActionType {
    Conflict(Conflict),
    DeleteMap(DeleteMap),
    DeleteSeq(DeleteSeq),
    Increment(Increment),
    Insert(Insert),
    Mark(MarkAction),
    PutMap(PutMap),
    PutSeq(PutSeq),
    SpliceText(SpliceText),
}

/// Individual struct for `PutMap`.
#[pyclass]
#[derive(Debug, Clone)]
pub struct PutMap {
    #[pyo3(get)]
    pub key: String,
    #[pyo3(get)]
    pub value: (PyValue<'static>, PyObjId),
    #[pyo3(get)]
    pub conflict: bool,
}

/// Individual struct for `PutSeq`.
#[pyclass]
#[derive(Debug, Clone)]
pub struct PutSeq {
    #[pyo3(get)]
    pub index: usize,
    #[pyo3(get)]
    pub value: (PyValue<'static>, PyObjId),
    #[pyo3(get)]
    pub conflict: bool,
}

/// Struct for `Increment`.
#[pyclass]
#[derive(Debug, Clone)]
pub struct Increment {
    #[pyo3(get)]
    pub prop: PyProp,
    #[pyo3(get)]
    pub value: i64,
}

#[pyclass]
#[derive(Debug, Clone)]
pub struct Insert {
    #[pyo3(get)]
    index: usize,
    #[pyo3(get)]
    values: Vec<(PyValue<'static>, PyObjId, bool)>,
}

/// Struct for `Conflict`.
#[pyclass]
#[derive(Debug, Clone)]
pub struct Conflict {
    #[pyo3(get)]
    pub prop: PyProp,
}

/// Struct for `DeleteMap`.
#[pyclass]
#[derive(Debug, Clone)]
pub struct DeleteMap {
    #[pyo3(get)]
    pub key: String,
}

/// Struct for `DeleteSeq`.
#[pyclass]
#[derive(Debug, Clone)]
pub struct DeleteSeq {
    #[pyo3(get)]
    pub index: usize,
    #[pyo3(get)]
    pub length: usize,
}

/// Struct for `Mark`.
#[pyclass(name = "Mark")]
#[derive(Debug, Clone)]
pub struct MarkAction {
    #[pyo3(get)]
    pub marks: Vec<PyMark>,
}

#[pyclass]
#[derive(Debug, Clone)]
pub struct SpliceText {
    #[pyo3(get)]
    index: usize,
    #[pyo3(get)]
    value: String,
    #[pyo3(get)]
    marks: Option<BTreeMap<String, PyScalarValue>>,
}

macro_rules! impl_repr {
    ( $( $struct_name:ident { $( $field:ident ),* } ),* ) => {
        $(
            #[pymethods]
            impl $struct_name {
                pub fn __repr__(slf: &Bound<'_, Self>) -> PyResult<String> {
                    let class_name: Bound<'_, pyo3::types::PyString> = slf.get_type().qualname()?;
                    let fields: Vec<String> = vec![
                        $(
                            format!("{}: {:?}", stringify!($field), (&slf.borrow()).$field)
                        ),*
                    ];
                    let values = fields.join(", ");

                    Ok(format!("{}({})", class_name, values))
                }
            }
        )*
    };
}

impl_repr!(
    Conflict { prop },
    DeleteMap { key },
    DeleteSeq { index, length },
    Increment { prop, value },
    Insert { index, values },
    MarkAction { marks },
    PutMap {
        key,
        value,
        conflict
    },
    PutSeq {
        index,
        value,
        conflict
    },
    SpliceText { index, value }
);
