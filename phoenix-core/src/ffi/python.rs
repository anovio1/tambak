// In: src/ffi/python.rs

use arrow::array::{make_array, Array, ArrayData, RecordBatch, RecordBatchReader};
use arrow::pyarrow::{FromPyArrow, PyArrowType, ToPyArrow};
use log::LevelFilter;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};
use std::io;
use std::sync::Once;

use crate::bridge::{self, Compressor, CompressorConfig, Decompressor, TimeSeriesStrategy};
use crate::chunk_pipeline;
use crate::error::PhoenixError;
use crate::ffi::ioadapters::{PyRecordBatchReader, PythonFileReader, PythonFileWriter};
use crate::utils;

//==================================================================================
// I. Stateful File-Level API (The recommended approach)
//==================================================================================

#[pyclass(name = "CompressorConfig", module = "phoenix_cache")]
#[derive(Default, Clone)]
pub struct PyCompressorConfig {
    // This is where user-configurable options will live.
    pub time_series_strategy: TimeSeriesStrategy,
    pub stream_id_column: Option<String>,
    pub timestamp_column: Option<String>,
}

#[pymethods]
impl PyCompressorConfig {
    #[new]
    #[pyo3(signature = (*, time_series_strategy="none", stream_id_column=None, timestamp_column=None))]
    fn new(
        time_series_strategy: &str,
        stream_id_column: Option<String>,
        timestamp_column: Option<String>,
    ) -> PyResult<Self> {
        let strategy = match time_series_strategy.to_lowercase().as_str() {
            "none" => TimeSeriesStrategy::None,
            "per_batch_relinearize" => TimeSeriesStrategy::PerBatchRelinearization,
            "global_sort" => TimeSeriesStrategy::GlobalSorting,
            _ => {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    "Invalid time_series_strategy. Must be one of 'none', 'per_batch_relinearize', 'global_sort'.",
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
    // The inner Compressor now holds a writer that can write to a Python file-like object.
    inner: Compressor<PythonFileWriter>,
}

#[pymethods]
impl PyCompressor {
    #[new]
    fn new(py_writer: PyObject, config: &PyCompressorConfig) -> PyResult<Self> {
        let writer = PythonFileWriter { obj: py_writer };
        // Convert the Python config to the Rust config.
        let rust_config = CompressorConfig {
            time_series_strategy: config.time_series_strategy,
            stream_id_column_name: config.stream_id_column.clone(),
            timestamp_column_name: config.timestamp_column.clone(),
            ..Default::default()
        };
        let compressor = Compressor::new(writer, rust_config).map_err(PhoenixError::into_pyerr)?;
        Ok(Self { inner: compressor })
    }

    /// Compresses a PyArrow RecordBatchReader into the Phoenix file format.
    #[pyo3(name = "compress")]
    pub fn compress_py(&mut self, py: Python, reader: &PyAny) -> PyResult<()> {
        // Adapt the Python RecordBatchReader to a Rust one.
        let mut rust_reader = PyRecordBatchReader {
            inner: reader.to_object(py),
        };
        // Delegate the entire compression process to the bridge.
        py.allow_threads(|| self.inner.compress(&mut rust_reader))
            .map_err(PhoenixError::into_pyerr)?;
        Ok(())
    }
}

#[pyclass(name = "Decompressor", module = "phoenix_cache")]
pub struct PyDecompressor {
    // Wrap in an option to allow `.batched()` to take ownership once.
    inner: Option<Decompressor<PythonFileReader>>,
}

#[pymethods]
impl PyDecompressor {
    #[new]
    fn new(py_reader: PyObject) -> PyResult<Self> {
        let reader = PythonFileReader { obj: py_reader };
        let decompressor = Decompressor::new(reader).map_err(PhoenixError::into_pyerr)?;
        Ok(Self {
            inner: Some(decompressor),
        })
    }

    /// Returns a streaming RecordBatchReader over the decompressed data.
    /// This method can only be called once.
    pub fn batched(&mut self) -> PyResult<PyArrowType<Box<dyn RecordBatchReader + Send>>> {
        if let Some(decompressor) = self.inner.take() {
            Ok(PyArrowType(Box::new(decompressor.batched())))
        } else {
            Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "batched() can only be called once.",
            ))
        }
    }

    /// If the file was globally sorted, this returns the permutation array needed to
    /// restore the original row order. Returns `None` otherwise.
    pub fn get_global_unsort_indices(&mut self, py: Python) -> PyResult<Option<PyObject>> {
        if let Some(decompressor) = self.inner.as_mut() {
            let maybe_array = py
                .allow_threads(move || decompressor.get_global_unsort_indices())
                .map_err(PhoenixError::into_pyerr)?;

            if let Some(array) = maybe_array {
                let py_array = array.to_data().to_pyarrow(py)?;
                Ok(Some(py_array))
            } else {
                Ok(None)
            }
        } else {
            Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "Decompressor has already been consumed by .batched()",
            ))
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
    // --- THE ONLY CHANGE IS HERE ---
    let compressed_vec =
        py.allow_threads(move || bridge::compress_arrow_chunk(rust_array.as_ref()))?;
    Ok(PyBytes::new(py, &compressed_vec))
}

#[pyfunction]
#[pyo3(name = "compress_analyze")]
pub fn compress_analyze_py(py: Python, array_py: &PyAny) -> PyResult<PyObject> {
    let array_data = ArrayData::from_pyarrow(array_py)?;
    let rust_array = make_array(array_data.into());

    // First, compress the chunk using the stateless API
    let artifact_bytes =
        py.allow_threads(move || bridge::compress_arrow_chunk(rust_array.as_ref()))?;

    // Now, analyze the resulting bytes
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

/// Compresses a single PyArrow Array and returns detailed statistics about the result.
#[pyfunction]
#[pyo3(name = "analyze_chunk")]
pub fn analyze_chunk_py(py: Python, array_py: &PyAny) -> PyResult<PyObject> {
    let array_data = ArrayData::from_pyarrow(array_py)?;
    let rust_array = make_array(array_data.into());

    let artifact_bytes =
        py.allow_threads(move || bridge::compress_arrow_chunk(rust_array.as_ref()))?;
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

//==================================================================================
// III. Misc Functions
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
            let file = std::fs::File::create(filename).expect("Could not create log file");
            builder.target(env_logger::Target::Pipe(Box::new(file)));
        }

        let _ = builder.try_init();
    });
}
