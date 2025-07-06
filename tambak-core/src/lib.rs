//! This file is the root of the `tambak_cache` Rust crate.
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

pub mod bridge;
pub mod config;
pub mod kernels;

mod chunk_pipeline;
mod error;
mod ffi;
mod frame_pipeline;
mod null_handling;
mod traits;
mod types;
mod utils;

//==================================================================================
// 2. Python Module Definition
//==================================================================================
use ffi::python::{PyCompressor, PyDecompressor, PyPartitionIterator};
use pyo3::prelude::*;

/// The `tambak_cache` Python module, containing all exposed Rust functions.
#[pymodule]
fn tambak_cache(py: Python, m: &PyModule) -> PyResult<()> {
    // --- V4.5 Refactor Orchestrator ---
    m.add_function(wrap_pyfunction!(ffi::compress_py, m)?)?;
    m.add_function(wrap_pyfunction!(ffi::compress_analyze_py, m)?)?;
    m.add_function(wrap_pyfunction!(ffi::compress_chunk_py, m)?)?;
    m.add_function(wrap_pyfunction!(ffi::decompress_chunk_py, m)?)?;

    // --- Add our classes module ---
    m.add_class::<PyCompressor>()?;
    m.add_class::<PyDecompressor>()?;
    m.add_class::<PyPartitionIterator>()?;

    // --- Expose the custom error type ---
    m.add(
        "tambakError",
        py.get_type::<pyo3::exceptions::PyValueError>(),
    )?;

    // --- Expose version string as a module attribute ---
    m.add("__version__", VERSION)?;

    // --- Turn on logging for planner col strategy ---
    m.add_function(wrap_pyfunction!(ffi::enable_verbose_logging_py, m)?)?;

    Ok(())
}
