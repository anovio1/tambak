// In: src/ffi/python.rs

use arrow::array::{make_array, ArrayData, RecordBatch};
use arrow::pyarrow::{FromPyArrow, ToPyArrow};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};

use crate::error::PhoenixError;
use crate::pipeline::{frame_orchestrator, orchestrator, planner};
// NEW: Import PlanningContext
use crate::pipeline::planner::PlanningContext;
use crate::types::PhoenixDataType;
use crate::utils;

//==================================================================================
// 1. Public Python Functions (v4.3 Corrected)
//==================================================================================

#[pyfunction]
#[pyo3(name = "compress")]
pub fn compress_py<'py>(py: Python<'py>, array_py: &PyAny) -> PyResult<&'py PyBytes> {
    let array_data = ArrayData::from_pyarrow(array_py)?;
    let rust_array = make_array(array_data.into());
    let compressed_vec =
        py.allow_threads(move || orchestrator::compress_chunk(rust_array.as_ref()))?;
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
        orchestrator::get_compressed_chunk_info(&artifact_bytes)?;

    let result_dict = PyDict::new(py);
    result_dict.set_item("artifact", PyBytes::new(py, &artifact_bytes))?;
    result_dict.set_item("header_size", header_size)?;
    result_dict.set_item("data_size", data_size)?;
    result_dict.set_item("total_size", artifact_bytes.len())?;
    result_dict.set_item("plan", pipeline_json)?;
    result_dict.set_item("original_type", original_type)?;

    Ok(result_dict.into())
}

#[pyfunction]
#[pyo3(name = "decompress")]
pub fn decompress_py(py: Python, bytes: &[u8]) -> PyResult<PyObject> {
    let reconstructed_array = py.allow_threads(move || orchestrator::decompress_chunk(bytes))?;
    utils::arrow_array_to_py(py, reconstructed_array)
}

#[pyfunction]
#[pyo3(name = "plan")]
pub fn plan_py(py: Python, bytes: &[u8], original_type: &str) -> PyResult<String> {
    py.allow_threads(move || {
        // 1. Convert the Python string to our internal, type-safe enum.
        let dtype = match original_type {
            "Int8" => PhoenixDataType::Int8,
            "Int16" => PhoenixDataType::Int16,
            "Int32" => PhoenixDataType::Int32,
            "Int64" => PhoenixDataType::Int64,
            "UInt8" => PhoenixDataType::UInt8,
            "UInt16" => PhoenixDataType::UInt16,
            "UInt32" => PhoenixDataType::UInt32,
            "UInt64" => PhoenixDataType::UInt64,
            "Float32" => PhoenixDataType::Float32,
            "Float64" => PhoenixDataType::Float64,
            "Boolean" => PhoenixDataType::Boolean,
            _ => return Err(PhoenixError::UnsupportedType(original_type.to_string()).into()),
        };

        // --- THIS IS THE CORE CHANGE ---
        // 2. Construct the PlanningContext. For this simple FFI helper,
        //    the initial and physical types are the same.
        let context = PlanningContext {
            initial_dtype: dtype,
            physical_dtype: dtype,
        };

        // 3. Call the refactored planner with the context.
        let plan_struct = planner::plan_pipeline(bytes, context)?;

        // 4. Serialize the `Plan` struct back to a JSON string for Python.
        serde_json::to_string_pretty(&plan_struct).map_err(|e| PhoenixError::from(e).into())
    })
}

// --- Frame-level functions are unchanged as they call the stable orchestrator facade ---

#[pyfunction]
#[pyo3(name = "compress_frame")]
pub fn compress_frame_py<'py>(py: Python<'py>, batch_py: &PyAny) -> PyResult<&'py PyBytes> {
    let rust_batch = RecordBatch::from_pyarrow(batch_py)?;
    let compressed_vec =
        py.allow_threads(move || frame_orchestrator::compress_frame(&rust_batch, &None))?;
    Ok(PyBytes::new(py, &compressed_vec))
}

#[pyfunction]
pub fn decompress_frame_py(py: Python, bytes: &[u8]) -> PyResult<PyObject> {
    let batch = py.allow_threads(move || frame_orchestrator::decompress_frame(bytes))?;
    batch.to_pyarrow(py)
}

#[pyfunction]
pub fn get_frame_diagnostics_py(py: Python, bytes: &[u8]) -> PyResult<PyObject> {
    use pyo3::types::PyString;
    let diagnostics_json = frame_orchestrator::get_frame_diagnostics(bytes)?;
    Ok(PyString::new(py, &diagnostics_json).into())
}
