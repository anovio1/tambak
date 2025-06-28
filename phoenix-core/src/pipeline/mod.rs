//! This module serves as the public API for the entire data processing pipeline.
//!
//! It encapsulates and orchestrates the various stages of compression and
//! decompression by composing the lower-level kernels. It defines the highest-level
//! workflows within the pure Rust core.

//==================================================================================
// 1. Module Declarations
//==================================================================================

/// The "General Contractor": Manages the end-to-end workflow, including null handling.
pub mod orchestrator;
pub mod frame_orchestrator;

/// The "Strategist": Analyzes data to create an optimal compression plan.
pub mod planner;

/// The "Foreman": Executes a given compression/decompression plan.
pub mod executor;

//==================================================================================
// 2. Public API Re-exports
//==================================================================================
// This section defines the public, stable API of the `pipeline` module.
// The FFI layer should only need to interact with the `orchestrator`.

pub use self::orchestrator::{
    compress_chunk,
    decompress_chunk,
    get_compressed_chunk_info,
};