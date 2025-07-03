//! This file is the root of the `phoenix_cache` Rust crate.
//!
//! Its responsibilities are strictly limited to:
//! 1.  Declaring all the top-level modules of our library (`pipeline`, `kernels`, etc.)
//!     so the Rust compiler knows they exist.
//! 2.  Defining the `#[pymodule]` which acts as the main entry point when the
//!     compiled library is imported into Python.

//==================================================================================
// 0. Constants
//==================================================================================
/// The crate version, automatically set from Cargo.toml at compile time.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
//==================================================================================
// 1. Module Declarations
//==================================================================================
#[macro_use]
mod observability; // Make macros available throughout the crate

mod bridge;
mod error;
mod ffi;
mod kernels;
mod null_handling;
mod pipeline;
mod traits;
mod types;
mod utils;

//==================================================================================
// 2. Python Module Definition
//==================================================================================
use pyo3::prelude::*;
use ffi::python::{PyCompressor, PyCompressorConfig};

/// The `phoenix_cache` Python module, containing all exposed Rust functions.
#[pymodule]
fn phoenix_cache(py: Python, m: &PyModule) -> PyResult<()> {
    // --- V4.4 Refactor Orchestrator ---
    m.add_function(wrap_pyfunction!(ffi::compress_bridge_py, m)?)?;
    m.add_function(wrap_pyfunction!(ffi::decompress_bridge_py, m)?)?;
    // m.add_function(wrap_pyfunction!(ffi::plan_bridge_py, m)?)?;
    m.add_function(wrap_pyfunction!(ffi::compress_analyze_bridge_py, m)?)?;

    // --- V4.0 Frame-level API ---
    m.add_function(wrap_pyfunction!(ffi::compress_frame_py, m)?)?;
    m.add_function(wrap_pyfunction!(ffi::decompress_frame_py, m)?)?;
    m.add_function(wrap_pyfunction!(ffi::get_frame_diagnostics_py, m)?)?;

    // --- V3.9 Legacy Chunk-level API ---
    m.add_function(wrap_pyfunction!(ffi::compress_py, m)?)?;
    m.add_function(wrap_pyfunction!(ffi::decompress_py, m)?)?;
    m.add_function(wrap_pyfunction!(ffi::plan_py, m)?)?;
    m.add_function(wrap_pyfunction!(ffi::compress_analyze_py, m)?)?;


    // --- Add our new stateful writer classes to the Python module ---
    m.add_class::<PyCompressorConfig>()?;
    m.add_class::<PyCompressor>()?;

    // --- Expose the custom error type ---
    m.add(
        "PhoenixError",
        py.get_type::<pyo3::exceptions::PyValueError>(),
    )?;

    // --- Expose version string as a module attribute ---
    m.add("__version__", VERSION)?;

    // --- Turn on logging for planner col strategy ---
    m.add_function(wrap_pyfunction!(ffi::enable_verbose_logging_py, m)?)?;

    Ok(())
}
