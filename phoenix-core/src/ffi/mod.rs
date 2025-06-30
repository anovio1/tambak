//! This module serves as the public API for the Foreign Function Interface (FFI) layer.
//!
//! Its primary responsibility is to declare the sub-modules that handle the
//! "impedance mismatch" between different language ecosystems. Currently, it only
//! contains the Python bridge, but could be extended in the future to support
//! other languages (e.g., a `c.rs` for a C-style API).

//==================================================================================
// 1. Module Declarations
//==================================================================================
/// Contains all logic for interfacing with the Python/CPython ecosystem,
pub mod python;

//==================================================================================
// 2. Public API Re-exports
//==================================================================================
// This section defines the public API of the FFI layer that the main `lib.rs`
// will use to construct the final Python module.
pub use self::python::{
    compress_py,
    compress_analyze_py,
    decompress_py,
    plan_py,
    compress_frame_py,
    decompress_frame_py,
    get_frame_diagnostics_py,
};