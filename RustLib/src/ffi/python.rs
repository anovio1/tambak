//! This module is the FFI "Anti-Corruption Layer" for the Phoenix library.
//!
//! Its ONLY responsibility is to translate Python objects (like Polars Series)
//! into pure Rust types (&[u8], &str) to pass to the core library, and to
//! translate the results back into Python objects. It contains ALL `polars`
//! and `pyo3` specific code. This layer is the only part of the library that
//! should be aware of the Python GIL.

use pyo3::prelude::*;
use polars::prelude::Series;

use crate::pipeline::{orchestrator, planner};
use crate::utils; // Will contain our safe casting and FFI helpers
use crate::error::PhoenixError;

//==================================================================================
// 1. Public Python Functions
//==================================================================================

#[pyfunction]
#[pyo3(name = "compress")]
/// The public FFI entry point for compressing a Polars Series.
///
/// This function handles the conversion from a Python object to Rust types,
/// releases the GIL for the heavy computation, and calls the pure Rust core.
pub fn compress_py(py: Python, series_py: &PyAny) -> PyResult<Vec<u8>> {
    // Convert the Python object to a Rust Polars Series.
    // This is a complex FFI operation that we abstract into a helper.
    let series = utils::py_to_series(series_py)?;

    // Release the GIL before calling our CPU-bound Rust code.
    py.allow_threads(move || {
        // Get ZERO-COPY slices from the Polars Series. This is a key performance win.
        let phys_series = series.to_physical_repr();
        let (data_slice, validity_slice_opt) = phys_series.to_byte_slices();
        let original_type = series.dtype().to_string();

        // Call the pure Rust orchestrator with simple slices.
        orchestrator::compress_chunk(
            data_slice,
            validity_slice_opt,
            &original_type
        )
        // Convert our library's error into a Python-compatible error.
        .map_err(PhoenixError::into)
    })
}

#[pyfunction]
#[pyo3(name = "decompress")]
/// The public FFI entry point for decompressing a byte artifact into a Polars Series.
///
/// This function calls the pure Rust core and then handles the conversion
/// of the resulting raw parts back into a Python object.
pub fn decompress_py(
    py: Python,
    bytes: &[u8],
    pipeline_json: &str,
    original_type: &str,
) -> PyResult<PyObject> {
    // Release the GIL before calling our CPU-bound Rust code.
    py.allow_threads(move || {
        // 1. Call the pure Rust orchestrator. It returns pure Rust types.
        let (reconstructed_data, reconstructed_validity) = orchestrator::decompress_chunk(
            bytes,
            pipeline_json,
            original_type,
        )?;

        // 2. Reconstruct a Polars Series from the pure Rust results.
        // This is a complex FFI operation that we abstract into a helper.
        let series = utils::reconstruct_series(
            &reconstructed_data,
            reconstructed_validity,
            original_type,
        )?;

        // 3. Convert the Rust Polars Series back to a Python object.
        utils::series_to_py(py, series)
    })
}

#[pyfunction]
#[pyo3(name = "plan")]
/// Analyzes a byte buffer and returns the optimal pipeline plan as a JSON string.
/// This is the FFI-facing dispatcher for the core planner logic.
pub fn plan_py(py: Python, bytes: &[u8], original_type: &str) -> PyResult<String> {
    // Release the GIL for the CPU-bound analysis.
    py.allow_threads(move || {
        // This macro safely casts the byte slice and calls the pure Rust planner.
        macro_rules! dispatch {
            ($T:ty) => {{
                // Use a SAFE cast from utils.rs
                let data = utils::safe_bytes_to_typed_slice::<$T>(bytes)?;
                planner::plan_pipeline_for_type(data)
            }};
        }

        // The match statement now lives here, in its correct home.
        let result = match original_type {
            "Int8" => dispatch!(i8),
            "Int16" => dispatch!(i16),
            "Int32" => dispatch!(i32),
            "Int64" => dispatch!(i64),
            // Add other supported types here...
            _ => {
                // Default plan for unsupported types.
                let pipeline = serde_json::json!([{"op": "zstd", "params": {"level": 3}}]);
                serde_json::to_string(&pipeline)
                    .map_err(|e| PhoenixError::InternalError(e.to_string()))
            }
        };

        // Convert our library's error into a Python-compatible error.
        result.map_err(PhoenixError::into)
    })
}