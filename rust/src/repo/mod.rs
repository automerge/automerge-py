//! Python bindings for samod-core
//!
//! This module provides PyO3 wrappers for samod-core types, exposing them to Python
//! without the "Py" prefix (e.g., `PeerId` in Python, `PyPeerId` in Rust).

use pyo3::prelude::*;

// Module declarations
pub mod types;
pub mod storage;
pub mod io;
pub mod loader;
pub mod commands;
pub mod hub_events;
pub mod hub_results;
pub mod document;
pub mod hub;
pub mod connection;

// Re-export all public types
pub use types::*;
pub use storage::*;
pub use io::*;
pub use loader::*;
pub use commands::*;
pub use hub_events::*;
pub use hub_results::*;
pub use document::*;
pub use hub::*;
pub use connection::*;

/// Register all repo types with the Python module
pub fn register_types(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Identity types
    m.add_class::<PyPeerId>()?;
    m.add_class::<PyStorageId>()?;
    m.add_class::<PyConnectionId>()?;
    m.add_class::<PyDocumentActorId>()?;
    m.add_class::<PyDocumentId>()?;
    m.add_class::<PyAutomergeUrl>()?;

    // Storage types
    m.add_class::<PyStorageKey>()?;
    m.add_class::<PyStorageTaskLoad>()?;
    m.add_class::<PyStorageTaskLoadRange>()?;
    m.add_class::<PyStorageTaskPut>()?;
    m.add_class::<PyStorageTaskDelete>()?;
    m.add_class::<PyStorageResult>()?;

    // IO types
    m.add_class::<PyIoTaskId>()?;

    // Action classes
    m.add_class::<PyStorageTaskAction>()?;
    m.add_class::<PySendAction>()?;
    m.add_class::<PyDisconnectAction>()?;
    m.add_class::<PyCheckAnnouncePolicyAction>()?;

    // Payload classes
    m.add_class::<PyStorageResultPayload>()?;
    m.add_class::<PyCheckAnnouncePolicyResultPayload>()?;
    m.add_class::<PySendResultPayload>()?;
    m.add_class::<PyDisconnectResultPayload>()?;

    // IO Result types
    m.add_class::<PyHubIoResultSend>()?;
    m.add_class::<PyHubIoResultDisconnect>()?;
    m.add_class::<PyDocumentIoResultStorage>()?;
    m.add_class::<PyDocumentIoResultCheckAnnouncePolicy>()?;

    // IoTask and IoResult
    m.add_class::<PyIoTask>()?;
    m.add_class::<PyIoResult>()?;

    // Loader
    m.add_class::<PyLoaderStateNeedIo>()?;
    m.add_class::<PyLoaderStateLoaded>()?;
    m.add_class::<PySamodLoader>()?;

    // Commands
    m.add_class::<PyCommandId>()?;
    m.add_class::<PyDispatchedCommand>()?;
    m.add_class::<PyCommandResultCreateConnection>()?;
    m.add_class::<PyCommandResultDisconnectConnection>()?;
    m.add_class::<PyCommandResultReceive>()?;
    m.add_class::<PyCommandResultActorReady>()?;
    m.add_class::<PyCommandResultCreateDocument>()?;
    m.add_class::<PyCommandResultFindDocument>()?;

    // Hub events
    m.add_class::<PyPeerInfo>()?;
    m.add_class::<PyConnDirection>()?;
    m.add_class::<PyHubEvent>()?;

    // Hub results
    m.add_class::<PyConnectionEventHandshakeCompleted>()?;
    m.add_class::<PyConnectionEventConnectionFailed>()?;
    m.add_class::<PyConnectionEventStateChanged>()?;
    m.add_class::<PyHubResults>()?;

    // Document types
    m.add_class::<PySpawnArgs>()?;
    m.add_class::<PyHubToDocMsg>()?;
    m.add_class::<PyDocToHubMsg>()?;
    m.add_class::<PyDocActorResult>()?;
    m.add_class::<PyDocumentActor>()?;

    // Hub
    m.add_class::<PyHub>()?;

    // Connection info types
    m.add_class::<PyConnectionState>()?;
    m.add_class::<PyConnectionStateHandshaking>()?;
    m.add_class::<PyConnectionStateConnected>()?;
    m.add_class::<PyPeerDocState>()?;
    m.add_class::<PyConnectionInfo>()?;

    Ok(())
}
