//! This module is the FFI "Anti-Corruption Layer" for the Phoenix library.
//! It handles the conversion between Python (PyArrow) and Rust (arrow-rs) types.

use arrow::array::{make_array, ArrayData, RecordBatch};
use arrow::pyarrow::{FromPyArrow, ToPyArrow};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyString};

use crate::error::PhoenixError;
// MODIFIED: Import the refactored modules and PlannerHints
use crate::pipeline::{executor, frame_orchestrator, get_compressed_chunk_info, orchestrator, planner, PlannerHints};
use crate::utils;

//==================================================================================
// 1. Public Python Functions (v4.1 Refactored)
//==================================================================================

#[pyfunction]
#[pyo3(name = "compress")]
pub fn compress_py<'py>(py: Python<'py>, array_py: &PyAny) -> PyResult<&'py PyBytes> {
    let array_data = ArrayData::from_pyarrow(array_py)?;
    let rust_array = make_array(array_data.into());

    // The top-level call now orchestrates the new two-step process.
    let compressed_vec = py.allow_threads(move || {
        // 1. Orchestrator creates the plan.
        let plan_json = orchestrator::create_plan(rust_array.as_ref())?;
        // 2. Executor executes the plan.
        executor::execute_plan(rust_array.as_ref(), &plan_json)
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
    // The internal `decompress_plan` is now the correct entry point.
    let reconstructed_array = py.allow_threads(move || executor::decompress_plan(bytes))?;

    utils::arrow_array_to_py(py, reconstructed_array)
}

#[pyfunction]
#[pyo3(name = "plan")]
pub fn plan_py(py: Python, bytes: &[u8], original_type: &str) -> PyResult<String> {
    py.allow_threads(move || {
        // The planner now returns a tuple, so we just take the plan string.
        planner::plan_pipeline(bytes, original_type)
            .map(|(plan, _)| plan)
            .map_err(PhoenixError::into)
    })
}

// --- RESTORED AND UPDATED FRAME-LEVEL FUNCTIONS ---

#[pyfunction]
#[pyo3(name = "compress_frame", signature = (batch_py, hints = None))]
pub fn compress_frame_py<'py>(
    py: Python<'py>,
    batch_py: &PyAny,
    hints: Option<&PyDict>,
) -> PyResult<&'py PyBytes> {
    let rust_batch = RecordBatch::from_pyarrow(batch_py)?;

    let rust_hints = if let Some(hints_dict) = hints {
        // This hint parsing logic is correct and remains.
        Some(PlannerHints {
            stream_id_column: hints_dict
                .get_item("stream_id_column")?
                .and_then(|v| v.extract().ok()),
            timestamp_column: hints_dict
                .get_item("timestamp_column")?
                .and_then(|v| v.extract().ok()),
        })
    } else {
        None
    };

    // The internal call is updated to use the refactored frame_orchestrator.
    let compressed_vec =
        py.allow_threads(move || frame_orchestrator::compress_frame(&rust_batch, &rust_hints))?;

    Ok(PyBytes::new(py, &compressed_vec))
}

#[pyfunction]
pub fn decompress_frame_py(py: Python, bytes: &[u8]) -> PyResult<PyObject> {
    // The internal call is updated to use the refactored frame_orchestrator.
    let batch = py.allow_threads(move || frame_orchestrator::decompress_frame(bytes))?;
    batch.to_pyarrow(py)
}

#[pyfunction]
pub fn get_frame_diagnostics_py(py: Python, bytes: &[u8]) -> PyResult<PyObject> {
    // The internal call is updated to use the refactored frame_orchestrator.
    let diagnostics_json = frame_orchestrator::get_frame_diagnostics(bytes)?;
    Ok(PyString::new(py, &diagnostics_json).into())
}
