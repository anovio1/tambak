// In: src/ffi/io_adapters.rs

use arrow::datatypes::Schema;
use arrow::error::{ArrowError, Result as ArrowResult};
use arrow::pyarrow::FromPyArrow;
use arrow::record_batch::{RecordBatch, RecordBatchReader};
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use std::error::Error as StdError;
use std::fmt;
use std::io::{self, Read, Seek, SeekFrom};
use std::sync::Arc;

// --- A simple error wrapper for PyErr ---
#[derive(Debug)]
pub struct PyErrWrapper(String);

impl fmt::Display for PyErrWrapper {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}
impl StdError for PyErrWrapper {}

// --- Adapter for READING from a Python file-like object ---
pub struct PythonFileReader {
    pub obj: PyObject,
}

impl Read for PythonFileReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        Python::with_gil(|py| {
            let result = self.obj.call_method1(py, "read", (buf.len(),));
            let py_bytes: &PyBytes = match result {
                Ok(obj) => match obj.extract(py) {
                    Ok(bytes) => bytes,
                    Err(e) => {
                        return Err(io::Error::new(
                            io::ErrorKind::Other,
                            format!("Failed to extract PyBytes: {}", e),
                        ));
                    }
                },
                Err(e) => {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        format!("Python exception in read: {}", e),
                    ));
                }
            };
            let bytes = py_bytes.as_bytes();
            let len = bytes.len().min(buf.len());
            buf[..len].copy_from_slice(&bytes[..len]);
            Ok(len)
        })
    }
}

impl Seek for PythonFileReader {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        Python::with_gil(|py| {
            let (whence, offset) = match pos {
                SeekFrom::Start(off) => (0, off as i64),
                SeekFrom::End(off) => (2, off),
                SeekFrom::Current(off) => (1, off),
            };
            let result = self.obj.call_method1(py, "seek", (offset, whence))?;
            result
                .extract(py)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
        })
    }
}

// --- Adapter for WRITING to a Python file-like object ---
pub struct PythonFileWriter {
    pub obj: PyObject,
}

impl io::Write for PythonFileWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        Python::with_gil(|py| {
            let bytes = PyBytes::new(py, buf);
            self.obj.call_method1(py, "write", (bytes,))?;
            Ok(buf.len())
        })
    }
    fn flush(&mut self) -> io::Result<()> {
        Python::with_gil(|py| {
            self.obj.call_method0(py, "flush")?;
            Ok(())
        })
    }
}

// --- Adapter for BRIDGING a Python reader to a Rust reader trait ---
pub struct PyRecordBatchReader {
    pub inner: PyObject,
}

impl Iterator for PyRecordBatchReader {
    type Item = ArrowResult<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        Python::with_gil(|py| {
            let result = self.inner.call_method0(py, "read_next_batch");
            match result {
                Ok(py_batch) => {
                    if py_batch.is_none(py) {
                        return None;
                    }
                    match RecordBatch::from_pyarrow(py_batch.as_ref(py)) {
                        Ok(batch) => Some(Ok(batch)),
                        Err(e) => Some(Err(ArrowError::ExternalError(Box::new(PyErrWrapper(
                            e.to_string(),
                        ))))),
                    }
                }
                Err(e) => {
                    if e.is_instance_of::<pyo3::exceptions::PyStopIteration>(py) {
                        None
                    } else {
                        Some(Err(ArrowError::ExternalError(Box::new(PyErrWrapper(
                            e.to_string(),
                        )))))
                    }
                }
            }
        })
    }
}

impl RecordBatchReader for PyRecordBatchReader {
    fn schema(&self) -> arrow::datatypes::SchemaRef {
        Python::with_gil(|py| {
            let py_schema = self.inner.getattr(py, "schema").unwrap();
            let rust_schema = Schema::from_pyarrow(py_schema.as_ref(py)).unwrap();
            Arc::new(rust_schema)
        })
    }
}
