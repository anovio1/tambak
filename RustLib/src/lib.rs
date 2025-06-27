// src/lib.rs
mod error;
mod utils;
mod ffi; // This now points to the ffi/ directory
mod kernels;
mod pipeline;
mod null_handling;

use pyo3::prelude::*;

#[pymodule]
fn phoenix_cache(_py: Python, m: &PyModule) -> PyResult<()> {
    // We call the functions that now live in ffi/python.rs
    m.add_function(wrap_pyfunction!(ffi::python::compress_series_ffi, m)?)?;
    m.add_function(wrap_pyfunction!(ffi::python::decompress_series_ffi, m)?)?;
    Ok(())
}