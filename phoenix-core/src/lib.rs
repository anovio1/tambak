//! This file is the root of the `phoenix_cache` Rust crate.
//!
//! Its responsibilities are strictly limited to:
//! 1.  Declaring all the top-level modules of our library (`pipeline`, `kernels`, etc.)
//!     so the Rust compiler knows they exist.
//! 2.  Defining the `#[pymodule]` which acts as the main entry point when the
//!     compiled library is imported into Python.

//==================================================================================
// 1. Module Declarations
//==================================================================================
// This section tells the compiler about the other files and directories in `src/`.
// The order does not matter, but alphabetical is conventional.

mod error;
mod ffi;
mod kernels;
mod null_handling;
mod pipeline;
mod utils;
pub mod traits;

//==================================================================================
// 2. Python Module Definition
//==================================================================================
// This is the core of the FFI integration. The `#[pymodule]` macro generates
// the necessary boilerplate to create a Python-importable module.

use pyo3::prelude::*;

/// Defines the `phoenix_cache` Python module.
///
/// This function is called by the Python interpreter when a user runs
/// `import phoenix_cache`. Its only job is to add our public-facing

/// functions (which live in the `ffi` module) to the module's namespace.
#[pymodule]
fn phoenix_cache(py: Python, m: &PyModule) -> PyResult<()> {
    // Add the core functions from our FFI layer.
    // We use the `_py` suffix in Rust to be clear these are the FFI entry points,
    // but the `#[pyo3(name = "...")]` attribute in `ffi/python.rs` gives them
    // a clean name in Python.
    m.add_function(wrap_pyfunction!(ffi::python::compress_py, m)?)?;
    m.add_function(wrap_pyfunction!(ffi::python::decompress_py, m)?)?;
    m.add_function(wrap_pyfunction!(ffi::python::plan_py, m)?)?;

    //  data frame
    m.add_function(wrap_pyfunction!(ffi::python::compress_frame_py, m)?)?;
    m.add_function(wrap_pyfunction!(ffi::python::decompress_frame_py, m)?)?;

    // As a best practice, we can also expose our custom Rust error type
    // to Python, which can be useful for more specific error handling.
    m.add("PhoenixError", py.get_type::<error::PhoenixError>())?;

    Ok(())
}