//! This module is the FFI "Anti-Corruption Layer" for the Phoenix library.
// ... (module description is unchanged) ...

use arrow::array::{make_array, ArrayData, RecordBatch};
use pyo3::prelude::*;
// use polars::prelude::{Series, PolarsDataType};
use arrow::pyarrow::FromPyArrow;
// use pyo3_polars::PyDataFrame
use crate::error::PhoenixError;
use crate::pipeline::{get_compressed_chunk_info, orchestrator, planner, PlannerHints};
use crate::utils;
use pyo3::types::{PyBytes, PyDict, PyString};

//==================================================================================
// 1. Public Python Functions (Final Version)
//==================================================================================

#[pyfunction]
#[pyo3(name = "compress")]
pub fn compress_py<'py>(py: Python<'py>, array_py: &PyAny) -> PyResult<&'py PyBytes> {
    // --- THIS IS THE CORRECT IMPLEMENTATION ---

    // 1. WITH GIL: Convert the Python Arrow object into a Rust `ArrayData`.
    //    `ArrayData` implements `FromPyArrow`, and this is the primary FFI
    //    data structure transfer.
    let array_data = ArrayData::from_pyarrow(array_py)?;

    // 2. Convert the `ArrayData` into a dynamic `Box<dyn Array>` using the
    //    standard `arrow-rs` factory function.
    let rust_array = make_array(array_data.into());

    // 3. RELEASE GIL: The rest of the function, which operates on the pure
    //    `rust_array` object, is now correct.
    let compressed_vec = py.allow_threads(move || {
        orchestrator::compress_chunk(rust_array.as_ref())
    })?;

    Ok(PyBytes::new(py, &compressed_vec))
}

#[pyfunction]
#[pyo3(name = "compress_analyze")]
pub fn compress_analyze_py(py: Python, array_py: &PyAny) -> PyResult<PyObject> {
    let array_data = ArrayData::from_pyarrow(array_py)?;
    let rust_array = make_array(array_data.into());

    let artifact_bytes =
        py.allow_threads(move || orchestrator::compress_chunk(rust_array.as_ref()))?;

    let (header_size, data_size, pipeline_json, original_type) =
        get_compressed_chunk_info(&artifact_bytes)?;

    let result_dict = PyDict::new(py);
    result_dict.set_item("artifact", PyBytes::new(py, &artifact_bytes))?;
    result_dict.set_item("header_size", header_size)?;
    result_dict.set_item("data_size", data_size)?;
    result_dict.set_item("total_size", artifact_bytes.len())?;
    result_dict.set_item("plan", pipeline_json)?;
    // --- ADD THIS LINE ---
    // Add the newly available original_type to the dictionary.
    result_dict.set_item("original_type", original_type)?;

    Ok(result_dict.into())
}

#[pyfunction]
#[pyo3(name = "decompress")]
pub fn decompress_py(py: Python, bytes: &[u8]) -> PyResult<PyObject> {
    // The call to `decompress_chunk` now only takes the bytes, as the
    // artifact is self-describing.
    let reconstructed_array =
        py.allow_threads(move || orchestrator::decompress_chunk(bytes))?;

    utils::arrow_array_to_py(py, reconstructed_array)
}

#[pyfunction]
#[pyo3(name = "plan")]
/// This function is correct and unchanged. The planner still needs the original type
/// to know whether to use integer or float-as-integer analysis.
pub fn plan_py(py: Python, bytes: &[u8], original_type: &str) -> PyResult<String> {
    py.allow_threads(move || planner::plan_pipeline(bytes, original_type).map_err(PhoenixError::into))
}

#[pyfunction]
#[pyo3(name = "compress_frame", signature = (batch_py, hints = None))]
pub fn compress_frame_py<'py>(
    py: Python<'py>,
    batch_py: &PyAny,
    hints: Option<&PyDict>,
) -> PyResult<&'py PyBytes> {
    let rust_batch = RecordBatch::from_pyarrow(batch_py)?;

    let rust_hints = if let Some(hints_dict) = hints {
        Some(PlannerHints {
            stream_id_column: hints_dict.get_item("stream_id_column")?.map(|v| v.extract()).transpose()?,
            timestamp_column: hints_dict.get_item("timestamp_column")?.map(|v| v.extract()).transpose()?,
        })
    } else {
        None
    };

    let compressed_vec = py.allow_threads(move || {
        orchestrator::compress_frame(&rust_batch, &rust_hints)
    })?;

    Ok(PyBytes::new(py, &compressed_vec))
}

#[pyfunction]
pub fn decompress_frame_py(py: Python, bytes: &[u8]) -> PyResult<PyObject> {
    let batch = py.allow_threads(move || orchestrator::decompress_frame(bytes))?;
    batch.to_pyarrow(py)
}

#[pyfunction]
pub fn get_frame_diagnostics_py(py: Python, bytes: &[u8]) -> PyResult<PyObject> {
    let diagnostics_json = orchestrator::get_frame_diagnostics(bytes)?;
    Ok(PyString::new(py, &diagnostics_json).into())
}