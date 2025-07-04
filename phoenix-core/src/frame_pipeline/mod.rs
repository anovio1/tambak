//! This module serves as the public API for the entire frame processing pipeline.
//!
//! It encapsulates and orchestrates the various stages of transformation and
//! compression

//==================================================================================
// 1. Module Declarations
//==================================================================================

pub mod profiler;
pub mod relinearize;
pub mod frame_orchestrator;

pub use self::profiler::{find_stride_by_autocorrelation, PlannerHints};
pub use self::frame_orchestrator::{compress_frame, decompress_frame, get_frame_diagnostics};

#[cfg(test)]
mod frame_orchestrator_tests;