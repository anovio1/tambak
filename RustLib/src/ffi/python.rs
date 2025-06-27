//! This module is the FFI "Anti-Corruption Layer" for the Phoenix library.
// ... (module description is unchanged) ...

use pyo3::prelude::*;
use polars::prelude::{Series, PolarsDataType};
use arrow::pyarrow::ToPyArrow;

use crate::pipeline::{orchestrator, planner};
use crate::utils;
use crate::error::PhoenixError;
use crate::null_handling;

//==================================================================================
// 1. Public Python Functions (Final Version)
//==================================================================================

#[pyfunction]
#[pyo3(name = "compress")]
pub fn compress_py(py: Python, series_py: &PyAny) -> PyResult<Vec<u8>> {
    let series = utils::py_to_series(series_py)?;
    py.allow_threads(move || {
        orchestrator::compress_chunk(&series).map_err(PhoenixError::into)
    })
}

#[pyfunction]
#[pyo3(name = "decompress")]
pub fn decompress_py(py: Python, bytes: &[u8], original_type: &str) -> PyResult<PyObject> {
    py.allow_threads(move || {
        let reconstructed_arrow_array = orchestrator::decompress_chunk(bytes, original_type)?;
        reconstructed_arrow_array.to_pyarrow(py)
    })
}

#[pyfunction]
#[pyo3(name = "plan")]
/// Analyzes a Polars Series and returns the optimal compression plan as a JSON string.
/// This is a utility function for inspection and debugging. It is not required for decompression.
pub fn plan_py(py: Python, series_py: &PyAny) -> PyResult<String> {
    let series = utils::py_to_series(series_py)?;

    py.allow_threads(move || {
        // The planner needs the raw data bytes, stripped of nulls.
        let (data_bytes, _) = null_handling::bitmap::strip_validity_to_bytes(&series)?;
        let original_type = series.dtype().to_string();

        // Call the pure Rust planner.
        planner::plan_pipeline(&data_bytes, &original_type)
            .map_err(PhoenixError::into)
    })
}