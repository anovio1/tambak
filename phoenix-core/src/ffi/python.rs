//! This module is the FFI "Anti-Corruption Layer" for the Phoenix library.
// ... (module description is unchanged) ...

use arrow::array::{make_array, ArrayData};
use pyo3::prelude::*;
// use polars::prelude::{Series, PolarsDataType};
use arrow::pyarrow::FromPyArrow;
// use pyo3_polars::PyDataFrame
use crate::error::PhoenixError;
use crate::pipeline::{orchestrator, planner};
use crate::utils;

//==================================================================================
// 1. Public Python Functions (Final Version)
//==================================================================================

#[pyfunction]
#[pyo3(name = "compress")]
pub fn compress_py(py: Python, array_py: &PyAny) -> PyResult<Vec<u8>> {
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
    py.allow_threads(move || {
        orchestrator::compress_chunk(rust_array.as_ref())
            .map_err(PhoenixError::into)
    })
}


#[pyfunction]
#[pyo3(name = "decompress")]
/// This function is correct.
pub fn decompress_py(
    py: Python,
    bytes: &[u8],
    original_type: &str,
) -> PyResult<PyObject> {
    let reconstructed_array = py.allow_threads(move || {
        orchestrator::decompress_chunk(bytes, original_type)
    })?;

    utils::arrow_array_to_py(py, reconstructed_array)
}

#[pyfunction]
#[pyo3(name = "plan")]
/// This function is correct.
pub fn plan_py(py: Python, bytes: &[u8], original_type: &str) -> PyResult<String> {
    py.allow_threads(move || {
        planner::plan_pipeline(bytes, original_type)
            .map_err(PhoenixError::into)
    })
}

// // --- DataFrame Functions ---

// #[pyfunction]
// #[pyo3(name = "compress_frame")]
// pub fn compress_frame_py(py: Python, dataframe_py: &PyAny) -> PyResult<Vec<u8>> {
//     let df = dataframe_py.extract::<PyDataFrame>()?.into();
//     py.allow_threads(move || {
//         frame_orchestrator::compress_frame(&df).map_err(PhoenixError::into)
//     })
// }

// #[pyfunction]
// #[pyo3(name = "decompress_frame")]
// pub fn decompress_frame_py(py: Python, bytes: &[u8]) -> PyResult<PyDataFrame> {
//     py.allow_threads(move || {
//         let df = frame_orchestrator::decompress_frame(bytes)?;
//         Ok(PyDataFrame(df))
//     })
// }
