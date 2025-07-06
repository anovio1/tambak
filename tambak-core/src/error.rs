// In: src/error.rs

//! This module defines the single, unified error type for the entire tambak library.
//! It uses the `thiserror` crate to provide ergonomic, context-aware error handling.

use pyo3::PyErr;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum tambakError {
    // =========================================================================
    // === High-Level, Semantic Errors (Specific to our library's logic)
    // =========================================================================
    #[error("Unsupported data type for this operation: {0}")]
    UnsupportedType(String),

    #[error("Frame serialization/deserialization failed: {0}")]
    FrameFormatError(String),

    #[error("Time-series relinearization failed: {0}")]
    RelinearizationError(String),
    // --- END FIX ---
    #[error("Internal logic error (this is a bug): {0}")]
    InternalError(String),

    // =========================================================================
    // === External Error Wrappers (Using #[from] for automatic conversion)
    // =========================================================================
    /// An error originating from the Arrow library.
    #[error("Arrow operation failed: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    /// An error originating from the underlying I/O subsystem (e.g., file not found, network error).
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// An error from the Serde JSON library, typically during plan/footer serialization.
    #[error("Serde JSON error: {0}")]
    SerdeJson(#[from] serde_json::Error),

    /// An error from a safe byte-casting operation failing.
    #[error("Byte slice casting error: {0}")]
    PodCast(String), // Manual `From` impl is needed as bytemuck::PodCastError doesn't impl Error

    /// An error for Python FFI (Foreign Function Interface) operations.
    #[error("FFI operation failed: {0}")]
    FfiError(String), // PyErr doesn't impl Error, so we can't use #[from] here.

    // =========================================================================
    // === Low-Level Pipeline/Kernel Errors (Could be consolidated later)
    // =========================================================================
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
        source: Box<tambakError>,
    },

    // --- NEW V4.0 ERRORS ---
    #[error("Structure discovery failed: {0}")]
    StructureDiscoveryError(String),

    #[error("Dictionary encoding/decoding failed: {0}")]
    DictionaryError(String),

    #[error("ANS encoding/decoding failed: {0}")]
    AnsError(String),

    #[error("Sparsity transform failed: {0}")]
    SparsityError(String),
}

// =============================================================================
// === Manual `From` Implementations ===
// =============================================================================

impl From<bytemuck::PodCastError> for tambakError {
    fn from(err: bytemuck::PodCastError) -> Self {
        tambakError::PodCast(err.to_string())
    }
}

impl From<PyErr> for tambakError {
    fn from(err: PyErr) -> Self {
        tambakError::FfiError(err.to_string())
    }
}

impl From<tambakError> for PyErr {
    fn from(err: tambakError) -> PyErr {
        pyo3::exceptions::PyValueError::new_err(err.to_string())
    }
}
