//! This module defines the single, unified error type for the entire Phoenix
//! library.
//! For v4.0, we add new variants to cover failures in the new,
//! more complex pipeline stages.

use pyo3::PyErr;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum PhoenixError {
    #[error("Unsupported data type for this operation: {0}")]
    UnsupportedType(String),

    #[error("Buffer length mismatch: expected a multiple of {0}, got {1}")]
    BufferMismatch(usize, usize),

    #[error("Zstd operation failed: {0}")]
    ZstdError(String),

    #[error("RLE decoding error: {0}")]
    RleDecodeError(String),

    #[error("LEB128 decoding error: {0}")]
    Leb128DecodeError(String),

    #[error("Bitpack decoding failed due to truncated buffer or data corruption")]
    BitpackDecodeError,

    #[error("Bitpack encoding error: value {0} exceeds bit width {1}")]
    BitpackEncodeError(u64, u8),

    #[error("Pipeline execution failed at stage '{stage}': {source}")]
    PipelineError {
        stage: String,
        #[source]
        source: Box<PhoenixError>,
    },

    #[error("FFI operation failed: {0}")]
    FfiError(String),

    #[error("Internal logic error (this is a bug): {0}")]
    InternalError(String),

    // --- NEW V4.0 ERRORS ---
    #[error("Structure discovery failed: {0}")]
    StructureDiscoveryError(String),

    #[error("Time-series relinearization failed: {0}")]
    RelinearizationError(String),

    #[error("Dictionary encoding/decoding failed: {0}")]
    DictionaryError(String),

    #[error("ANS encoding/decoding failed: {0}")]
    AnsError(String),

    #[error("Frame serialization/deserialization failed: {0}")]
    FrameFormatError(String),

    #[error("Sparsity transform failed: {0}")]
    SparsityError(String),
}

// --- FFI Conversion ---

impl From<PyErr> for PhoenixError {
    fn from(err: PyErr) -> Self {
        PhoenixError::FfiError(err.to_string())
    }
}

impl From<PhoenixError> for PyErr {
    fn from(err: PhoenixError) -> PyErr {
        pyo3::exceptions::PyValueError::new_err(err.to_string())
    }
}

// --- THIS IS THE FIX for the `?` operator ---

impl From<serde_json::Error> for PhoenixError {
    fn from(err: serde_json::Error) -> Self {
        PhoenixError::InternalError(format!("JSON serialization/deserialization error: {}", err))
    }
}

impl From<bytemuck::PodCastError> for PhoenixError {
    fn from(err: bytemuck::PodCastError) -> Self {
        PhoenixError::InternalError(format!("Byte slice casting error: {}", err))
    }
}
