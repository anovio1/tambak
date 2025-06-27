use pyo3::exceptions::PyValueError;
use pyo3::PyErr;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum PhoenixError {
    #[error("Buffer length ({0}) is not divisible by element size ({1})")]
    BufferMismatch(usize, usize),
    #[error("Unsupported or invalid type string for this operation: {0}")]
    UnsupportedType(String),
    #[error("Zstd compression/decompression error: {0}")]
    ZstdError(String),
    #[error("RLE decoding error: {0}")]
    RleDecodeError(String),
    #[error("LEB128 decoding error: {0}")]
    Leb128DecodeError(String),
    #[error("Bitpack decoding error: value count mismatch or truncated buffer")]
    BitpackDecodeError,
    #[error("Bitpack encoding error: value {0} exceeds bit width {1}")]
    BitpackEncodeError(u64, u8),
    #[error("Pipeline execution failed at stage '{stage}': {source}")]
    PipelineError {
        stage: String,
        #[source]
        source: Box<PhoenixError>,
    },
    #[error("Invalid pipeline configuration: {0}")]
    InvalidPipeline(String),
}

impl From<PhoenixError> for PyErr {
    fn from(err: PhoenixError) -> PyErr {
        PyValueError::new_err(err.to_string())
    }
}