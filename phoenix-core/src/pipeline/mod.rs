//! This module serves as the public API for the entire data processing pipeline.
//!
//! It encapsulates and orchestrates the various stages of compression and
//! decompression by composing the lower-level kernels. It defines the highest-level
//! workflows within the pure Rust core.

//==================================================================================
// 1. Module Declarations
//==================================================================================

pub mod artifact;

pub mod frame_orchestrator;
/// The "General Contractor": Manages the end-to-end workflow, including null handling.
pub mod orchestrator;

/// The "Strategist": Analyzes data to create an optimal compression plan.
pub mod planner;
// pub mod pipeline_generator;

/// The "Foreman": Executes a given compression/decompression plan.
pub mod executor;

pub mod profiler;
pub mod relinearize;

pub mod context;
pub mod models;
pub mod traits;

//==================================================================================
// 2. Public API Re-exports
//==================================================================================
// This section defines the public, stable API of the `pipeline` module.
// The FFI layer should only need to interact with the `orchestrator`.

pub use self::orchestrator::{compress_chunk, decompress_chunk, get_compressed_chunk_info};

pub use self::frame_orchestrator::{compress_frame, decompress_frame, get_frame_diagnostics};

pub use self::planner::plan_pipeline;

pub use self::profiler::{find_stride_by_autocorrelation, PlannerHints};

pub use self::models::{Operation, Plan};

pub use self::traits::OperationBehavior;

#[cfg(test)]
mod frame_orchestrator_tests;
mod orchestrator_tests;
mod planner_tests;
