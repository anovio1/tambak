// In: src/ffi/python.rs

use arrow::array::{make_array, Array, ArrayData, RecordBatch, RecordBatchReader};
use arrow::pyarrow::{FromPyArrow, PyArrowType, ToPyArrow};
use log::LevelFilter;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyTuple};
use std::fs::OpenOptions;
use std::io;
use std::sync::Once;

use crate::bridge::{
    self, compressor::Compressor, config::CompressorConfig, decompressor::Decompressor,
    TimeSeriesStrategy,
};
use crate::error::PhoenixError;
use crate::ffi::ioadapters::{PyRecordBatchReader, PythonFileReader, PythonFileWriter};
use crate::utils;

//==================================================================================
// I. Stateful File-Level API (The recommended approach)
//==================================================================================

#[pyclass(name = "CompressorConfig", module = "phoenix_cache")]
#[derive(Default, Clone)]
pub struct PyCompressorConfig {
    pub time_series_strategy: TimeSeriesStrategy,
    pub stream_id_column: Option<String>,
    pub timestamp_column: Option<String>,
}

#[pymethods]
impl PyCompressorConfig {
    #[new]
    #[pyo3(signature = (*, time_series_strategy="none", stream_id_column=None, timestamp_column=None, partition_key_column=None, partition_flush_rows=100_000))]
    fn new(
        time_series_strategy: &str,
        stream_id_column: Option<String>,
        timestamp_column: Option<String>,
        partition_key_column: Option<String>,
        partition_flush_rows: usize,
    ) -> PyResult<Self> {
        let strategy = match time_series_strategy.to_lowercase().as_str() {
            "none" => TimeSeriesStrategy::None,
            "per_batch_relinearize" => TimeSeriesStrategy::PerBatchRelinearization,
            "partitioned" => {
                // For 'partitioned', we MUST have the key column.
                if let Some(key_col) = partition_key_column {
                    TimeSeriesStrategy::Partitioned {
                        partition_key_column: key_col,
                        partition_flush_rows,
                    }
                } else {
                    return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                        "time_series_strategy 'partitioned' requires 'partition_key_column' to be set.",
                    ));
                }
            }
            _ => {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    "Invalid time_series_strategy. Must be one of 'none', 'per_batch_relinearize', or 'partitioned'.",
                ))
            }
        };
        Ok(Self {
            time_series_strategy: strategy,
            stream_id_column,
            timestamp_column,
        })
    }
}

#[pyclass(name = "Compressor", module = "phoenix_cache")]
pub struct PyCompressor {
    inner: Compressor<PythonFileWriter>,
}

#[pymethods]
impl PyCompressor {
    #[new]
    fn new(py_writer: PyObject, config: &PyCompressorConfig) -> PyResult<Self> {
        let writer = PythonFileWriter { obj: py_writer };
        let rust_config = CompressorConfig {
            time_series_strategy: config.time_series_strategy.clone(), // FIX: Clone the strategy
            stream_id_column_name: config.stream_id_column.clone(),
            timestamp_column_name: config.timestamp_column.clone(),
            ..Default::default()
        };

        let compressor = Compressor::new(writer, rust_config)?;
        Ok(Self { inner: compressor })
    }

    /// Compresses a PyArrow RecordBatchReader into the Phoenix file format.
    #[pyo3(name = "compress")]
    pub fn compress_py(&mut self, py: Python, reader: &PyAny) -> PyResult<()> {
        let mut rust_reader = PyRecordBatchReader {
            inner: reader.to_object(py),
        };
        py.allow_threads(|| self.inner.compress(&mut rust_reader))?;
        Ok(())
    }
}

#[pyclass(name = "Decompressor", module = "phoenix_cache")]
pub struct PyDecompressor {
    inner: Option<Decompressor<PythonFileReader>>,
}

#[pymethods]
impl PyDecompressor {
    #[new]
    fn new(py_reader: PyObject) -> PyResult<Self> {
        let reader = PythonFileReader { obj: py_reader };
        let decompressor = Decompressor::new(reader)?;
        Ok(Self {
            inner: Some(decompressor),
        })
    }

    /// Returns a streaming RecordBatchReader over the decompressed data for a non-partitioned file.
    /// This method can only be called once and consumes the Decompressor.
    pub fn batched(&mut self) -> PyResult<PyArrowType<Box<dyn RecordBatchReader + Send>>> {
        if let Some(decompressor) = self.inner.take() {
            // FIX: The inner `batched()` now returns a Result.
            let rust_reader = decompressor.batched()?;
            Ok(PyArrowType(Box::new(rust_reader)))
        } else {
            Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "Decompressor has already been consumed by .batched() or .partitions()",
            ))
        }
    }

    /// Returns an iterator over the partitions in the file.
    /// Each item yielded by the iterator is a tuple of (partition_key, RecordBatchReader).
    /// This method can only be called once and consumes the Decompressor.
    #[pyo3(name = "partitions")]
    pub fn partitions(&mut self) -> PyResult<PyPartitionIterator> {
        if let Some(decompressor) = self.inner.take() {
            let rust_partition_iter = decompressor.partitions()?;
            Ok(PyPartitionIterator {
                inner: rust_partition_iter,
            })
        } else {
            Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "Decompressor has already been consumed by .batched() or .partitions()",
            ))
        }
    }
}

/// A Python-facing iterator that yields (key, PartitionReader) tuples for each partition.
#[pyclass(name = "PartitionIterator", module = "phoenix_cache")]
pub struct PyPartitionIterator {
    inner: crate::bridge::decompressor::PartitionIterator<PythonFileReader>,
}

#[pymethods]
impl PyPartitionIterator {
    fn __iter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn __next__(&mut self, py: Python) -> PyResult<Option<PyObject>> {
        if let Some((key, rust_partition_reader)) = self.inner.next() {
            // Your new design: create a dedicated wrapper for the reader. This is excellent.
            let py_partition_reader = PyPartitionReader {
                inner: Box::new(rust_partition_reader),
            };
            // Correctly build the Python tuple.
            let py_tuple = PyTuple::new(py, &[key.into_py(py), py_partition_reader.into_py(py)]);
            Ok(Some(py_tuple.into()))
        } else {
            Ok(None)
        }
    }
}

/// A Python-facing RecordBatchReader for a single partition.
#[pyclass(name = "PartitionReader", module = "phoenix_cache")]
pub struct PyPartitionReader {
    inner: Box<crate::bridge::decompressor::PartitionReader<PythonFileReader>>,
}

#[pymethods]
impl PyPartitionReader {
    fn __iter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    /// Returns the next RecordBatch from this partition, or raises StopIteration.
    fn __next__(&mut self, py: Python) -> PyResult<Option<PyObject>> {
        // This logic is perfectly correct. It iterates the inner Rust reader...
        if let Some(batch_result) = self.inner.next() {
            // ...handles any potential ArrowError from Rust...
            let batch = batch_result.map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                    "Error reading next batch from partition: {}",
                    e
                ))
            })?;
            // ...and converts the successful RecordBatch to a PyArrow object.
            // Assuming `utils::record_batch_to_pyarrow` exists and is correct.
            let py_batch = batch.to_pyarrow(py)?;
            Ok(Some(py_batch))
        } else {
            // Signals the end of the iteration.
            Ok(None)
        }
    }
}

//==================================================================================
// II. Stateless Chunk-Level API (for advanced/FFI use cases)
//==================================================================================
#[pyfunction]
#[pyo3(name = "compress")]
pub fn compress_py<'py>(py: Python<'py>, array_py: &PyAny) -> PyResult<&'py PyBytes> {
    let array_data = ArrayData::from_pyarrow(array_py)?;
    let rust_array = make_array(array_data.into());
    // FIX: Use `?` for automatic error conversion.
    let compressed_vec =
        py.allow_threads(move || bridge::compress_arrow_chunk(rust_array.as_ref()))?;
    Ok(PyBytes::new(py, &compressed_vec))
}

#[pyfunction]
#[pyo3(name = "compress_analyze")]
pub fn compress_analyze_py(py: Python, array_py: &PyAny) -> PyResult<PyObject> {
    let array_data = ArrayData::from_pyarrow(array_py)?;
    let rust_array = make_array(array_data.into());

    // FIX: Use `?` for automatic error conversion.
    // First, compress the chunk using the stateless API
    let artifact_bytes =
        py.allow_threads(move || bridge::compress_arrow_chunk(rust_array.as_ref()))?;

    // Now, analyze the resulting bytes
    // FIX: Use `?` for automatic error conversion.
    let stats = bridge::analyze_chunk(&artifact_bytes)?;

    let result_dict = PyDict::new(py);
    result_dict.set_item("artifact", PyBytes::new(py, &artifact_bytes))?;
    result_dict.set_item("header_size", stats.header_size)?;
    result_dict.set_item("data_size", stats.data_size)?;
    result_dict.set_item("total_size", stats.total_size)?;
    result_dict.set_item("plan", stats.plan_json)?;
    result_dict.set_item("original_type", stats.original_type)?;

    Ok(result_dict.into())
}

/// Compresses a single PyArrow Array into a raw Phoenix chunk.
#[pyfunction]
#[pyo3(name = "compress_chunk")]
pub fn compress_chunk_py<'py>(py: Python<'py>, array_py: &PyAny) -> PyResult<&'py PyBytes> {
    let array_data = ArrayData::from_pyarrow(array_py)?;
    let rust_array = make_array(array_data.into());
    let compressed_vec =
        py.allow_threads(move || bridge::compress_arrow_chunk(rust_array.as_ref()))?;
    Ok(PyBytes::new(py, &compressed_vec))
}

/// Decompresses a raw Phoenix chunk into a PyArrow Array.
#[pyfunction]
#[pyo3(name = "decompress_chunk")]
pub fn decompress_chunk_py(py: Python, bytes: &[u8]) -> PyResult<PyObject> {
    let reconstructed_array = py.allow_threads(move || bridge::decompress_arrow_chunk(bytes))?;
    utils::arrow_array_to_py(py, reconstructed_array)
}

/// Analyzes a compressed Phoenix chunk without fully decompressing it.
#[pyfunction]
#[pyo3(name = "analyze_chunk")]
pub fn analyze_chunk_py(py: Python, chunk_bytes: &[u8]) -> PyResult<PyObject> {
    let stats = py.allow_threads(move || bridge::analyze_chunk(chunk_bytes))?;

    let result_dict = PyDict::new(py);
    result_dict.set_item("header_size", stats.header_size)?;
    result_dict.set_item("data_size", stats.data_size)?;
    result_dict.set_item("total_size", stats.total_size)?;
    result_dict.set_item("plan", stats.plan_json)?;
    result_dict.set_item("original_type", stats.original_type)?;

    Ok(result_dict.into())
}

//==================================================================================
// III. Module Definition
//==================================================================================

static INIT_LOGGER: Once = Once::new();

#[pyfunction]
#[pyo3(name = "enable_verbose_logging")]
pub fn enable_verbose_logging_py(log_file: Option<String>) {
    INIT_LOGGER.call_once(|| {
        let mut builder = env_logger::Builder::new();

        builder.is_test(false);
        builder.filter_level(LevelFilter::Info);

        // Custom formatter: just print the level and message
        builder.format(|buf, record| {
            use std::io::Write;
            writeln!(buf, "[{}] {}", record.level(), record.args())?;
            buf.flush()?;
            Ok(())
        });

        if let Some(filename) = log_file {
            let file = OpenOptions::new()
                .append(true) // open for append
                .create(true) // create if it doesn't exist
                .open(filename)
                .expect("Could not open log file in append mode");
            builder.target(env_logger::Target::Pipe(Box::new(file)));
        }

        let _ = builder.try_init();
    });
}
