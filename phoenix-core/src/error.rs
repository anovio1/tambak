//! This module defines the single, unified error type for the entire Phoenix
//! library.
//!
//! By using a single error enum, we can easily propagate errors from any kernel
//! or pipeline stage all the way up to the FFI boundary, where it can be
//! cleanly translated into a Python exception. The `thiserror` crate is used
//! to reduce boilerplate.

use thiserror::Error;

#[derive(Error, Debug)]
pub enum PhoenixError {
    #[error("Unsupported data type for this operation: {0}")]
    UnsupportedType(String),

    #[error("Buffer length mismatch: expected {0}, got {1}")]
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

    #[error("Polars operation failed: {0}")]
    PolarsError(String),

    #[error("Internal logic error (this is a bug): {0}")]
    InternalError(String),
}

// This allows us to convert a PolarsError into our PhoenixError easily.
impl From<polars::prelude::PolarsError> for PhoenixError {
    fn from(err: polars::prelude::PolarsError) -> Self {
        PhoenixError::PolarsError(err.to_string())
    }
}

// This allows us to convert our PhoenixError into a PyErr for the FFI layer.
impl From<PhoenixError> for pyo3::prelude::PyErr {
    fn from(err: PhoenixError) -> pyo3::prelude::PyErr {
        pyo3::exceptions::PyValueError::new_err(err.to_string())
    }
}