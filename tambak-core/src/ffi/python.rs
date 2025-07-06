// In: src/ffi/python.rs

use arrow::array::{make_array, ArrayData, RecordBatchReader};
use arrow::pyarrow::{FromPyArrow, PyArrowType, ToPyArrow};
use log::LevelFilter;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyTuple};
use std::fs::OpenOptions;
use std::sync::Once;

use crate::bridge::{compressor::Compressor, decompressor::Decompressor};
use crate::config::{
    self, CompressionFormat, CompressionProfile, TambakConfig, TimeSeriesStrategy,
};
use crate::ffi::ioadapters::{PyRecordBatchReader, PythonFileReader, PythonFileWriter};
use crate::utils;

//==================================================================================
// I. Stateful File-Level API (The recommended approach)
//==================================================================================

#[pyclass(name = "Compressor", module = "tambak_cache")]
pub struct PyCompressor {
    inner: Compressor<PythonFileWriter>,
}

#[pymethods]
impl PyCompressor {
    /// Creates a new Compressor instance.
    ///
    /// This constructor is the main entry point from Python. It takes various
    /// configuration options as keyword arguments, constructs the unified
    /// `TambakConfig` struct, and initializes the Rust compression engine.
    #[new]
    #[pyo3(signature = (
        py_writer,
        format = "columnar_file",
        profile = "balanced",
        include_footer = true,
        enable_stats_collection = false,
        time_series_strategy = "none",
        chunk_size_rows = 100_000,
        stream_id_column = None,
        timestamp_column = None,
        partition_key_column = None,
        partition_flush_rows = 100_000
    ))]
    fn new(
        py_writer: PyObject,
        format: &str,
        profile: &str,
        include_footer: bool,
        enable_stats_collection: bool,
        time_series_strategy: &str,
        chunk_size_rows: usize,
        stream_id_column: Option<String>,
        timestamp_column: Option<String>,
        partition_key_column: Option<String>,
        partition_flush_rows: usize,
    ) -> PyResult<Self> {
        let writer = PythonFileWriter { obj: py_writer };

        // 1. Parse string arguments into their corresponding Rust enums.
        let parsed_format = match format.to_lowercase().as_str() {
            "columnar_file" => CompressionFormat::ColumnarFile,
            "interleaved_stream" => CompressionFormat::InterleavedStream,
            _ => {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    "Invalid format. Must be 'columnar_file' or 'interleaved_stream'.",
                ))
            }
        };

        let parsed_profile = match profile.to_lowercase().as_str() {
            "fast" => CompressionProfile::Fast,
            "balanced" => CompressionProfile::Balanced,
            "high_compression" => CompressionProfile::HighCompression,
            _ => {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    "Invalid profile. Must be 'fast', 'balanced', or 'high_compression'.",
                ))
            }
        };

        // 2. Build the TimeSeriesStrategy enum variant based on the user's choice.
        // This makes the invalid state of providing partitioning args with a non-partitioned
        // strategy difficult to express, which is good design.
        let parsed_ts_strategy = match time_series_strategy.to_lowercase().as_str() {
            "none" => TimeSeriesStrategy::None,
            "per_batch_relinearize" => TimeSeriesStrategy::PerBatchRelinearization,
            "partitioned" => {
                let key_column = partition_key_column.ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyValueError, _>(
                        "The 'partitioned' strategy requires the 'partition_key_column' argument.",
                    )
                })?;
                TimeSeriesStrategy::Partitioned {
                    key_column,
                    partition_flush_rows,
                }
            }
            _ => {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    "Invalid time_series_strategy. Must be 'none', 'per_batch_relinearize', or 'partitioned'.",
                ))
            }
        };

        // 3. Assemble the final, unified TambakConfig struct.
        let rust_config = TambakConfig {
            format: parsed_format,
            profile: parsed_profile,
            include_footer,
            enable_stats_collection,
            time_series_strategy: parsed_ts_strategy,
            chunk_size_rows,
            stream_id_column_name: stream_id_column,
            timestamp_column_name: timestamp_column,
            ..Default::default()
        };

        // 4. Pass the config to the bridge's Compressor, which will wrap it in an Arc.
        let compressor = Compressor::new(writer, rust_config)?;
        Ok(Self { inner: compressor })
    }

    /// Compresses a PyArrow RecordBatchReader into the tambak file format.
    #[pyo3(name = "compress")]
    pub fn compress_py(&mut self, py: Python, reader: &PyAny) -> PyResult<()> {
        let mut rust_reader = PyRecordBatchReader {
            inner: reader.to_object(py),
        };
        py.allow_threads(|| self.inner.compress(&mut rust_reader))?;
        Ok(())
    }
}

#[pyclass(name = "Decompressor", module = "tambak_cache")]
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
#[pyclass(name = "PartitionIterator", module = "tambak_cache")]
pub struct PyPartitionIterator {
    // This now correctly references the decompressor module directly
    inner: crate::bridge::decompressor::PartitionIterator<PythonFileReader>,
}

#[pymethods]
impl PyPartitionIterator {
    fn __iter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn __next__(&mut self, py: Python) -> PyResult<Option<PyObject>> {
        if let Some((key, rust_partition_reader)) = self.inner.next() {
            let py_partition_reader = PyPartitionReader {
                inner: Box::new(rust_partition_reader),
            };
            let py_tuple = PyTuple::new(py, &[key.into_py(py), py_partition_reader.into_py(py)]);
            Ok(Some(py_tuple.into()))
        } else {
            Ok(None)
        }
    }
}

/// A Python-facing RecordBatchReader for a single partition.
#[pyclass(name = "PartitionReader", module = "tambak_cache")]
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
        if let Some(batch_result) = self.inner.next() {
            let batch = batch_result.map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                    "Error reading next batch from partition: {}",
                    e
                ))
            })?;
            let py_batch = batch.to_pyarrow(py)?;
            Ok(Some(py_batch))
        } else {
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
    // NOTE: This stateless API does not take a config. It uses default planning.
    // In the future, we could add an optional config dict here.
    let compressed_vec =
        py.allow_threads(move || crate::bridge::compress_arrow_chunk(rust_array.as_ref()))?;
    Ok(PyBytes::new(py, &compressed_vec))
}

#[pyfunction]
#[pyo3(name = "compress_analyze")]
pub fn compress_analyze_py(py: Python, array_py: &PyAny) -> PyResult<PyObject> {
    let array_data = ArrayData::from_pyarrow(array_py)?;
    let rust_array = make_array(array_data.into());

    let artifact_bytes =
        py.allow_threads(move || crate::bridge::compress_arrow_chunk(rust_array.as_ref()))?;
    let stats = crate::bridge::analyze_chunk(&artifact_bytes)?;

    let result_dict = PyDict::new(py);
    result_dict.set_item("artifact", PyBytes::new(py, &artifact_bytes))?;
    result_dict.set_item("header_size", stats.header_size)?;
    result_dict.set_item("data_size", stats.data_size)?;
    result_dict.set_item("total_size", stats.total_size)?;
    result_dict.set_item("plan", stats.plan_json)?;
    result_dict.set_item("original_type", stats.original_type)?;

    Ok(result_dict.into())
}

/// Compresses a single PyArrow Array into a raw tambak chunk.
#[pyfunction]
#[pyo3(name = "compress_chunk")]
pub fn compress_chunk_py<'py>(py: Python<'py>, array_py: &PyAny) -> PyResult<&'py PyBytes> {
    let array_data = ArrayData::from_pyarrow(array_py)?;
    let rust_array = make_array(array_data.into());
    let compressed_vec =
        py.allow_threads(move || crate::bridge::compress_arrow_chunk(rust_array.as_ref()))?;
    Ok(PyBytes::new(py, &compressed_vec))
}

/// Decompresses a raw tambak chunk into a PyArrow Array.
#[pyfunction]
#[pyo3(name = "decompress_chunk")]
pub fn decompress_chunk_py(py: Python, bytes: &[u8]) -> PyResult<PyObject> {
    let reconstructed_array =
        py.allow_threads(move || crate::bridge::decompress_arrow_chunk(bytes))?;
    utils::arrow_array_to_py(py, reconstructed_array)
}

/// Analyzes a compressed tambak chunk without fully decompressing it.
#[pyfunction]
#[pyo3(name = "analyze_chunk")]
pub fn analyze_chunk_py(py: Python, chunk_bytes: &[u8]) -> PyResult<PyObject> {
    let stats = py.allow_threads(move || crate::bridge::analyze_chunk(chunk_bytes))?;

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
                .append(true)
                .create(true)
                .open(filename)
                .expect("Could not open log file in append mode");
            builder.target(env_logger::Target::Pipe(Box::new(file)));
        }

        let _ = builder.try_init();
    });
}
